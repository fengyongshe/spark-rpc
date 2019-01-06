package com.fys.spark.rpc.common

import com.fys.spark.rpc.exception.RpcException

trait RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv
}

trait RpcEndpoint {

  val rpcEnv: RpcEnv

  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new RpcException(self + " does not implement 'receive'")
  }

  def receiveAndReply(context: RpcCallContext): PartialFunction[Any,Unit] = {
    case _ => context.sendFailure(new RpcException(self + " won't reply anything"))
  }

  def onError(cause: Throwable): Unit ={
    throw cause
  }

  def onConnected(remoteAddress: RpcAddress): Unit ={}

  def onDisconnected(remoteAddress: RpcAddress): Unit ={}

  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit ={}

  def onStart(): Unit ={}

  def onStop(): Unit = {}

  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }

}

trait ThreadSafeRpcEndpoint extends RpcEndpoint
