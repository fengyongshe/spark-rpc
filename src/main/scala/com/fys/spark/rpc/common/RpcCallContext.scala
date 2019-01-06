package com.fys.spark.rpc.common

trait RpcCallContext {

  def reply(response: Any): Unit

  def sendFailure(e: Throwable): Unit

  def senderAddress: RpcAddress

}
