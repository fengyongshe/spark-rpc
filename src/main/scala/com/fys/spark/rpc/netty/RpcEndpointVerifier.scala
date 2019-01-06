package com.fys.spark.rpc.netty

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.fys.spark.rpc.common.{RpcCallContext, RpcEndpoint, RpcEnv}
import com.fys.spark.rpc.util.Utils

private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case c: RpcEndpointVerifier.CheckExistence => context.reply(dispatcher.verify(c.getName))
  }
}

private[netty] object RpcEndpointVerifier {

  val NAME = "endpoint-verifier"

  class CheckExistence(var name: String) extends Serializable with Externalizable {
    def getName: String = name

    def this() = this(null)

    override def writeExternal(out: ObjectOutput): Unit = {
      Utils.tryOrIOException {
        out.writeUTF(name)
      }
    }

    override def readExternal(in: ObjectInput): Unit = {
      Utils.tryOrIOException {
        name = in.readUTF()
      }
    }
  }

  def createCheckExistence(name: String) = {
    new CheckExistence(name)
  }

}

