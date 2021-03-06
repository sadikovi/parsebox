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

package com.github.sadikovi.parsebox.examples.zip

import com.github.sadikovi.parsebox.api._

/** Default format for reading zip files */
class DefaultFormat extends HadoopFormat[Opt1RecordType] {
  override def recordClass(): Class[Opt1RecordType] = classOf[Opt1RecordType]

  override def filter(rawValue: String): Boolean = true

  override def process(rawValue: String, record: Opt1RecordType): Unit = {
    record.setKey(rawValue)
  }
}
