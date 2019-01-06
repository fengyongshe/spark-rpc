package com.fys.spark.rpc.serializer

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import com.fys.spark.rpc.io.NextIterator
import org.apache.spark.annotation.{DeveloperApi, Private}

import scala.reflect.ClassTag

@DeveloperApi
abstract class Serializer {

  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  def newInstance(): SerializerInstance

  @Private
  def supportsRelocationOfSerializedObjects: Boolean = false

}


@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {

  def serialize[T: ClassTag](t: T): ByteBuffer
  def deserialize[T: ClassTag](bytes: ByteBuffer): T
  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream
  def deserializeStream(s: InputStream): DeserializationStream

}

@DeveloperApi
abstract class SerializationStream {

  def writeObject[T: ClassTag](t: T): SerializationStream
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)

  def flush(): Unit
  def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext){
      writeObject(iter.next())
    }
    this
  }

}

@DeveloperApi
abstract class DeserializationStream {

  def readObject[T: ClassTag](): T
  def readKey[T: ClassTag]: T = readObject[T]()
  def readValue[T: ClassTag](): T = readObject[T]()
  def close(): Unit

  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }

  }
}