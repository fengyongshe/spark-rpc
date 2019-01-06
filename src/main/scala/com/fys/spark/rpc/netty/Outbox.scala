package com.fys.spark.rpc.netty

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import com.fys.spark.rpc.common.RpcAddress
import com.fys.spark.rpc.exception.{RpcEnvStoppedException, RpcException}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

private[netty] sealed trait OutboxMessage {
  def sendWith(client: TransportClient): Unit
  def onFailure(e: Throwable): Unit
}

private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage {

  private val log = LoggerFactory.getLogger(classOf[OneWayOutboxMessage])

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => log.warn(e1.getMessage)
      case e1: Throwable => log.warn(s"Failed to send one-way RPC.", e1)
    }
  }
}

private[netty] case class RpcOutboxMessage(content: ByteBuffer,
                                           _onFailure:(Throwable) => Unit,
                                          _onSuccess: (TransportClient, ByteBuffer) => Unit)
    extends OutboxMessage with RpcResponseCallback {

  private val log = LoggerFactory.getLogger(classOf[RpcOutboxMessage])
  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content,this)
  }

  def onTimeout(): Unit ={
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      log.error("Ask timeout before connecting successfully")
    }
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
  outbox =>

  @GuardedBy("this")
  private val messages = new util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  private var client: TransportClient = null

  @GuardedBy("this")
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  private var stopped = false

  @GuardedBy("this")
  private var draining = false

  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) true
      else {
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new RpcException(s"Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      if (stopped) return
      if (connectFuture != null) return
      if (client == null) {
        launchConnectTask()
        return
      }
      if (draining) return
      message = messages.poll()
      if (message == null) return
      draining = true
    }
    while(true) {
      try {
        val _client = synchronized {client}
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if(stopped) return
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  private def launchConnectTask(): Unit = {
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null}
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized {
          connectFuture = null
        }
        drainOutbox()
      }
    })
  }

  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
    }
    nettyEnv.removeOutbox(address)
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  private def closeClient(): Unit = synchronized {
    client = null
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) return
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }
  }

}
