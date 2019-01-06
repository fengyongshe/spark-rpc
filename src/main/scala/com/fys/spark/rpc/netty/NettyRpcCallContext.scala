package com.fys.spark.rpc.netty

import com.fys.spark.rpc.common.{RpcAddress, RpcCallContext}
import org.apache.spark.network.client.RpcResponseCallback

import scala.concurrent.Promise

private[netty] abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext {

  protected def send(message: Any): Unit
  override def reply(response: Any): Unit = {
    send(response)
  }
  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }
}

private[netty] class LocalNettyRpcCallContext( senderAddress: RpcAddress, p: Promise[Any])
  extends NettyRpcCallContext(senderAddress ) {

  override protected def send(message: Any): Unit ={
    p.success(message)
  }

}

private[netty] class RemoteNettyRpcCallContext( nettyEnv: NettyRpcEnv,
                                                callback: RpcResponseCallback,
                                                senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit ={
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}
