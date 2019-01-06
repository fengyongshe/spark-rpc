package com.fys.spark.rpc.common

import java.util.concurrent.TimeoutException

import com.fys.spark.rpc.util.Utils
import com.fys.spark.rpc.RpcConf
import com.fys.spark.rpc.exception.RpcException

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

class RpcTimeoutException(message: String, cause: TimeoutException )
  extends TimeoutException(message) {
  initCause(cause)
}

class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String) extends Serializable {

  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage + ". This timeout is controlled by "+ timeoutProp, te)
  }

  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    case rte: RpcTimeoutException => throw rte
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  def awaitResult[T](future: Future[T]): T = {
    val wrapAndRethow: PartialFunction[Throwable, T] = {
      case NonFatal(t) =>
        throw new RpcException("Exception throw in awaitResult", t)
    }
    try {
      Await.result(future, duration)
    } catch addMessageIfTimeout.orElse(wrapAndRethow)
  }

}

object RpcTimeout {

  def apply(conf: RpcConf, timeoutProp: String): RpcTimeout = {
    val timeout = {
      conf.getTimeAsSeconds(timeoutProp).seconds
    }
    new RpcTimeout(timeout, timeoutProp)
  }

  def apply(conf: RpcConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
    val timeout = {
      conf.getTimeAsSeconds(timeoutProp, defaultValue).seconds
    }
    new RpcTimeout(timeout, timeoutProp)
  }

  def apply(conf: RpcConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)
    val itr = timeoutPropList.iterator
    var foundProp: Option[(String,String)] = None
    while (itr.hasNext && foundProp.isEmpty) {
      val propKey = itr.next()
      conf.getOption(propKey).foreach{ prop => foundProp = Some(propKey, prop)}
    }
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
    val timeout = {
      Utils.timeStringAsSeconds(finalProp._2).seconds
    }
    new RpcTimeout(timeout, finalProp._1)
  }

}