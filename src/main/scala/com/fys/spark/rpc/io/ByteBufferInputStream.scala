package com.fys.spark.rpc.io

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(private var buffer: ByteBuffer) extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest,offset,amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amoutToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position + amoutToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amoutToSkip
    } else {
      0L
    }
  }

  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}
