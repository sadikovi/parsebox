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

import java.io.{BufferedOutputStream, OutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.hadoop.io.compress.{CompressionOutputStream, Compressor}

private[parsebox] class ZipCompressorStream(
    out: OutputStream,
    compressor: Compressor) extends CompressionOutputStream(out) {
  def this(out: OutputStream) = this(out, null)

  private val zip = new ZipOutputStream(out)
  zip.putNextEntry(new ZipEntry("content"))

  override def close(): Unit = {
    super.close()
    zip.close()
  }

  override def finish(): Unit = {
    zip.closeEntry()
    zip.finish()
  }

  override def resetState(): Unit = compressor.reset()

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    zip.write(b, off, len)
  }

  override def write(b: Int): Unit = {
    write(Array[Byte](b.toByte), 0, 1)
  }
}
