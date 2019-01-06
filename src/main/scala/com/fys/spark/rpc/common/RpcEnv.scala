package com.fys.spark.rpc.common

import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.util.RpcUtils

import scala.concurrent.Future

abstract class RpcEnv(conf: RpcConf) {

  val defaultLoopupTimeout = RpcUtils.lookupRpcTimeout(conf)
  def address: RpcAddress

  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLoopupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  def stop(endpoint: RpcEndpointRef): Unit
  def shutdown(): Unit
  def awaitTermination(): Unit

  def deserialize[T](deserializationAction:() => T): T

}

abstract class RpcEnvConfig {
  def conf: RpcConf
  def name: String
  def bindAddress: String
  def port: Int
  def clientMode: Boolean
}

case class RpcEnvServerConfig(conf: RpcConf,
                              name: String ,
                              bindAddress: String,
                              port: Int ) extends RpcEnvConfig {
  override def clientMode: Boolean = false
}

case class RpcEnvClientConfig(conf: RpcConf, name: String) extends RpcEnvConfig {

  override def bindAddress: String = null
  override def port: Int = 0
  override def clientMode: Boolean = true

}
