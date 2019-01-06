package com.fys.spark.rpc.netty

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.common._
import com.fys.spark.rpc.exception.{RpcEndpointNotFoundException, RpcEnvStoppedException}
import com.fys.spark.rpc.serializer.{JavaSerializer, JavaSerializerInstance}
import com.fys.spark.rpc.util.{ThreadUtils, Utils}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.server.{RpcHandler, StreamManager, TransportServer, TransportServerBootstrap}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise, TimeoutException}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{DynamicVariable, Failure, Success}

class NettyRpcEnv( val conf: RpcConf,
                   javaSerializerInstance: JavaSerializerInstance,
                   host: String) extends RpcEnv(conf) {

  private val log = LoggerFactory.getLogger(classOf[NettyRpcEnv])

  private[netty] val transportConf = KrapsTransportConf.fromSparkConf(
    conf.set("spark.rpc.io.numConnectionsPerPeer","1"),
    "rpc",
    conf.getInt("spark.rpc.io.threads", 0)
  )

  private val dispatcher: Dispatcher = new Dispatcher(this)

  private val streamManager = new StreamManager {
    override def getChunk(streamId: Long, chunkIndex: Int) = null
  }

  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] =
    java.util.Collections.emptyList[TransportClientBootstrap]

  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.getInt("spark.rpc.connect.threads",64)
  )

  @volatile private var server: TransportServer = _
  private val stopped = new AtomicBoolean(false)
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  private[netty] def removeOutbox(address: RpcAddress): Unit ={
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  def startServer(bindAddress: String, port: Int): Unit ={
    val bootstraps: java.util.List[TransportServerBootstrap] = java.util.Collections.emptyList()
    server = transportContext.createServer(bindAddress, port, bootstraps)
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this,dispatcher)
    )
  }

  @Nullable
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort) else null
  }

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf,addr,this)
    val verifier = new NettyRpcEndpointRef(conf, RpcEndpointAddress(addr.rpcAddress,RpcEndpointVerifier.NAME),this)
    verifier.ask[Boolean](RpcEndpointVerifier.createCheckExistence(endpointRef.name))
      .flatMap {
        find =>
          if (find) {
            Future.successful(endpointRef)
          } else {
            Future.failed(new RpcEndpointNotFoundException(uri))
          }
      }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver != null,
      "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }

      if (stopped.get) {
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  private[netty] def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => log.warn(e.getMessage)
      }
    } else {
      postToOutbox(message.receiver, OneWayOutboxMessage(serialize(message)))
    }
  }

  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout ): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        log.warn(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          log.warn(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message,p)
      } else {
        val rpcMessage = RpcOutboxMessage(serialize(message),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client,response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.onFailure {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }
      val timeoutCancelable = timeoutScheduler.schedule( new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)

      promise.future.onComplete {
        v =>
          timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e )
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)

  }


  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize {
        () =>
          javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def endpointRef(endpoint: RpcEndpoint) : RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  override def shutdown(): Unit = {
    cleanup()
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) return
    val iter = outboxes.values().iterator()
    while(iter.hasNext) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if(timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
  }

  override def deserialize[T](deserializationAction:() => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

}

private[netty] object NettyRpcEnv {
  private[netty] val currentEnv = new DynamicVariable[NettyRpcEnv](null)
  private[netty] val currentClient = new DynamicVariable[TransportClient](null)
}

private[netty] class NettyRpcEndpointRef( @transient private val conf: RpcConf,
                                          endpointAddress: RpcEndpointAddress,
                                          @transient @volatile private var nettyEnv: NettyRpcEnv)
                 extends RpcEndpointRef(conf) with Serializable {
  @transient
  @volatile var client: TransportClient = _

  private val _address = if (endpointAddress.rpcAddress != null) endpointAddress else null
  private val _name = endpointAddress.name

  override def address: RpcAddress = if (_address != null) _address.rpcAddress else null

  private def readObject(in: ObjectInputStream): Unit ={
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def name: String = _name

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def send(message: Any): Unit ={
    require(message != null, "Message is null")
    nettyEnv.send(RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${_address})"

  def toURI: URI = new URI(_address.toString)

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => _address == other._address
    case _ => false
  }

  final override def hashCode(): Int = if (_address == null) 0 else _address.hashCode()

}

object NettyRpcEnvFactory extends RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv = {
    val conf = config.conf
    val javaSerializerInstance =
      new JavaSerializer(conf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(conf, javaSerializerInstance, config.bindAddress)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = {
        actualPort =>
          nettyEnv.startServer(config.bindAddress, actualPort)
          (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port,startNettyRpcEnv, conf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }

}

private[netty] case class RpcFailure(e: Throwable)

private[netty] case class RequestMessage(senderAddress: RpcAddress, receiver: NettyRpcEndpointRef, content: Any)

private[netty] class NettyRpcHandler( dispatcher: Dispatcher,
                                      nettyEnv: NettyRpcEnv,
                                      streamManager: StreamManager) extends RpcHandler {

  private val log = LoggerFactory.getLogger(classOf[NettyRpcHandler])

  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(client: TransportClient,
                       message: ByteBuffer,
                       callback: RpcResponseCallback): Unit ={
    val messageToDispatch = internalReceive(client,message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(client: TransportClient,
                       message: ByteBuffer): Unit ={
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient,
                              message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = nettyEnv.deserialize[RequestMessage](client,message)
    if (requestMessage.senderAddress == null) {
      RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
    }
    requestMessage
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val  clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
      val remoteEnvAddress = remoteAddresses.get(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      }
    } else {
      log.error("Exception before connecting to the client", cause)
    }
  }

  override def channelActive(client: TransportClient): Unit = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  override def channelInactive(client: TransportClient): Unit = {
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
      val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      nettyEnv.removeOutbox(clientAddr)
      dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
      val remoteEnvAddress = remoteAddresses.remove(clientAddr)
      if (remoteEnvAddress != null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
    } else {}
  }

}
