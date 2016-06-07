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

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.io.compress.{CompressionCodec, Compressor, CompressionInputStream,
  CompressionOutputStream, Decompressor}

/**
 * [[ZipCodec]] interface provides compression/decompression pair for Hadoop InputFormat. Note that
 * current implementation only supports decompression based on `ZipInputStream`, but compression
 * can easily be added using `ZipOutputFormat` and similar approach with [[ZipDecompressorStream]].
 */
private[parsebox] class ZipCodec extends CompressionCodec {
  override def createOutputStream(out: OutputStream): CompressionOutputStream = {
    new ZipCompressorStream(out, createCompressor())
  }

  override def createOutputStream(out: OutputStream, compr: Compressor): CompressionOutputStream = {
    new ZipCompressorStream(out, compr)
  }

  override def getCompressorType(): Class[_ <: Compressor] = classOf[ZipCompressor]

  override def createCompressor(): Compressor = new ZipCompressor()

  override def createInputStream(in: InputStream): CompressionInputStream = {
    new ZipDecompressorStream(in, createDecompressor())
  }

  override def createInputStream(in: InputStream, decompr: Decompressor): CompressionInputStream = {
    new ZipDecompressorStream(in, decompr)
  }

  override def getDecompressorType(): Class[_ <: Decompressor] = classOf[ZipDecompressor]

  override def createDecompressor(): Decompressor = new ZipDecompressor()

  override def getDefaultExtension(): String = ".zip"
}
