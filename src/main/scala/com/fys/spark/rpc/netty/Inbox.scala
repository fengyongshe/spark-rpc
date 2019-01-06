package com.fys.spark.rpc.netty

import java.util
import javax.annotation.concurrent.GuardedBy

import com.fys.spark.rpc.common.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}
import com.fys.spark.rpc.exception.RpcException
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

private[netty] sealed trait InboxMessage

private[netty] case class OneWayMessage(senderAddress: RpcAddress, content: Any) extends InboxMessage
private[netty] case class RpcMessage(senderAddress: RpcAddress, content: Any,
                                     context: NettyRpcCallContext) extends InboxMessage
private[netty] case object OnStart extends InboxMessage
private[netty] case object OnStop extends InboxMessage

private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress) extends InboxMessage

private[netty] class Inbox(
                            val endpointRef: NettyRpcEndpointRef,
                            val endpoint: RpcEndpoint) {

  inbox =>

  private val log = LoggerFactory.getLogger(classOf[Inbox])

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  @GuardedBy("this")
  private var stopped = false

  @GuardedBy("this")
  private var enableConcurrent = false

  @GuardedBy("this")
  private var numActiveThreads = 0

  inbox.synchronized {
    messages.add(OnStart)
  }

  def process(dispatcher: Dispatcher): Unit ={
     var message : InboxMessage = null
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) return
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any,Unit](content, {
                msg => throw new RpcException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any,Unit](content, {
              msg => throw new RpcException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized {
              inbox.numActiveThreads
            }
            assert(activeThreads == 1, s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        if (!enableConcurrent && numActiveThreads != 1) {
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }

    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    if (!stopped) {
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
    }
  }

  def isEmpty: Boolean = inbox.synchronized {
    messages.isEmpty
  }

  protected def onDrop(message: InboxMessage): Unit = {
    log.warn(s"Drop $message because $endpointRef is stopped")
  }

  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => log.error(s"Ignoring error", ee)
        }
    }
  }

}
