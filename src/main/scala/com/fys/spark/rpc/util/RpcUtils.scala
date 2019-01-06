package com.fys.spark.rpc.util

import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.common.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

object RpcUtils {

  def makeDriverRef(name: String, conf:RpcConf, rpcEnv: RpcEnv ): RpcEndpointRef = {
    val driverHost: String = conf.get("spark.driver.host","localhost")
    val driverPort: Int = conf.getInt("spark.driver.port", 7077)
    Utils.checkHost(driverHost, "Expected hostname")
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  def numRetries(conf: RpcConf): Int = {
    conf.getInt("spark.rpc.numRetries",3)
  }

  def retryWaitMs(conf: RpcConf): Long = {
    conf.getTimeAsMs("spark.rpc.retry.wait", "3s")
  }

  def askRpcTimeout(conf: RpcConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.askTimeout","spark.network.timeout"),"120s")
  }

  def lookupRpcTimeout(conf: RpcConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.lookupTimeout","spark.network.timeout"),"120s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  def maxMessageSizeBytes(conf: RpcConf): Int = {
    val maxSizeInMB = conf.getInt("spark.rpc.message.maxSize", 128)
    if(maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"spark.rpc.message.maxSize should not be greeter tham $MAX_MESSAGE_SIZE_IN_MB MB"
      )
    }
    maxSizeInMB * 1024 * 1024
  }

}

