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

import java.util.zip.Deflater
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater

/** Simple subclass for decompressor */
private[parsebox] class ZipCompressor(
    level: Int, nowrap: Boolean)
  extends BuiltInZlibDeflater(level, nowrap) {

  def this(level: Int) = this(level, false)

  def this() = this(Deflater.DEFAULT_COMPRESSION)
}
