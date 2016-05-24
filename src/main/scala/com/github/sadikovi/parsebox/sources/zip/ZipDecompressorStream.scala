/*
 * Copyright 2016 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sadikovi.parsebox.sources.zip

import java.io.InputStream
import java.util.zip.ZipInputStream

import org.apache.hadoop.io.compress.{CompressionInputStream, Decompressor}

/**
 * [[ZipDecompressorStream]] interface is a wrapper around `ZipInputStream` to provide decompression
 * for `zip` files. This supports multiple entries in zip archive. Buffer size cannot be increased
 * due to default constructor.
 */
private[parsebox] class ZipDecompressorStream(
    in: InputStream,
    decompressor: Decompressor) extends CompressionInputStream(in) {
  def this(in: InputStream) = this(in, null)

  private val zip = new ZipInputStream(in)
  private val oneByte: Array[Byte] = new Array[Byte](1)

  override def read(): Int = {
    if (read(oneByte, 0, oneByte.length) == -1) -1 else (oneByte(0) & 0xff)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    var numBytes = zip.read(b, off, len)
    if (numBytes == -1) {
      zip.getNextEntry()
      numBytes = zip.read(b, off, len)
    }

    numBytes
  }

  override def resetState(): Unit = {
    decompressor.reset()
  }

  override def close(): Unit = {
    if (zip != null) {
      zip.closeEntry()
      zip.close()
    }

    super.close()
  }
}
