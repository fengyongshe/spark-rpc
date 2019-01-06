package com.fys.spark.rpc.common

import java.net.URI

import com.fys.spark.rpc.util.Utils

case class RpcAddress(host: String, port: Int) {

  def hostPort: String = host + ":" + port

  def toSparkURL: String = "spark://" + hostPort

  override def toString: String = hostPort

}

object RpcAddress {

  def fromURIString(uri: String): RpcAddress = {
    val uriObj = new URI(uri)
    RpcAddress(uriObj.getHost, uriObj.getPort)
  }

  def fromSparkURL(sparkUrl: String): RpcAddress = {
    val (host,port) = Utils.extractHostPortFromKrapsUrl(sparkUrl)
    RpcAddress(host,port)
  }
}
