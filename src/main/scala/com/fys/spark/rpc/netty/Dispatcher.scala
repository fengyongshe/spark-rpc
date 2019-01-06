package com.fys.spark.rpc.netty

import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import com.fys.spark.rpc.common.{RpcEndpoint, RpcEndpointAddress, RpcEndpointRef}
import com.fys.spark.rpc.exception.{RpcEnvStoppedException, RpcException}
import com.fys.spark.rpc.util.ThreadUtils
import org.apache.spark.network.client.RpcResponseCallback
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) {

  private val log = LoggerFactory.getLogger(classOf[Dispatcher])

  private class EndpointData( val name: String,
                              val endpoint: RpcEndpoint,
                              val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String,EndpointData]
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  private val receivers = new LinkedBlockingQueue[EndpointData]

  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint ): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpoints.putIfAbsent(name, new EndpointData(name,endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      endpointRefs.put(data.endpoint, data.ref)
      receivers.offer(data)
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  private def unregisterRpcEndpoint(name: String): Unit = {
    val data = endpoints.remove(name)
    if (data != null) {
      data.inbox.stop()
      receivers.offer(data)
    }
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next()
      postMessage(name, message,(e) => log.warn(s"Message %message dropped. ${e.getMessage}"))
    }
  }

  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name,
      OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  private def postMessage(endpointName: String,
                          message: InboxMessage,
                          callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null){
        Some(new RpcException(s"Could not find $endpointName."))
      } else {
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) return
      stopped = true
    }
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    receivers.offer(PosionPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  private val threadpool: ThreadPoolExecutor = {
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, Runtime.getRuntime.availableProcessors()))
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatch-event-loop")
    for ( i <- 0 until numThreads) {
      pool.execute(new Messageloop)
    }
    pool
  }

  private class Messageloop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = receivers.take()
            if (data == PosionPill) {
              receivers.offer(PosionPill)
              return
            }
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => log.error(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException =>
      }
    }
  }

  private val PosionPill = new EndpointData(null,null,null)

}
