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

import com.github.sadikovi.testutil.UnitTestSpec
import com.github.sadikovi.testutil.implicits._

class ZipCompressionSuite extends UnitTestSpec {
  val zipSinglePath = testDirectory / "resources" / "zip" / "single.zip"
  val zipEmptyPath = testDirectory / "resources" / "zip" / "empty.zip"

  test("init ZipDecompressorStream with empty decompressor") {
    val in = open(zipSinglePath)
    try {
      val stream = new ZipDecompressorStream(in)
      stream.isDecompressorSet() should be (false)
      assert(stream.read() > 1, "Failed to read zip stream")
    } finally {
      in.close()
    }
  }

  test("read byte from empty ZipDecompressorStream") {
    val in = open(zipEmptyPath)
    try {
      val stream = new ZipDecompressorStream(in)
      assert(stream.read() == -1, "Failed to read zip stream")
    } finally {
      in.close()
    }
  }

  test("create zip output stream") {
    val zip = new ZipCodec()
    intercept[UnsupportedOperationException] {
      zip.createOutputStream(null)
    }

    intercept[UnsupportedOperationException] {
      zip.createOutputStream(null, zip.createCompressor())
    }
  }

  test("get zip compressor") {
    val zip = new ZipCodec()
    intercept[UnsupportedOperationException] {
      zip.getCompressorType()
    }

    intercept[UnsupportedOperationException] {
      zip.createCompressor()
    }
  }

  test("create zip input stream") {
    val zip = new ZipCodec()
    val in = open(zipEmptyPath)

    // Creating stream without decompressor
    try {
      val zipStream = zip.createInputStream(in)
      zipStream.getClass should be (classOf[ZipDecompressorStream])
    } finally {
      in.close()
    }

    // Creating stream with decompressor
    try {
      val zipStream = zip.createInputStream(in, zip.createDecompressor())
      zipStream.getClass should be (classOf[ZipDecompressorStream])
    } finally {
      in.close()
    }
  }

  test("get zip decompressor") {
    val zip = new ZipCodec()
    zip.getDecompressorType() should be (classOf[ZipDecompressor])
    zip.createDecompressor().getClass should be (classOf[ZipDecompressor])
  }

  test("get zip extension") {
    val zip = new ZipCodec()
    zip.getDefaultExtension() should be (".zip")
  }
}
