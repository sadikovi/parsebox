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

package com.github.sadikovi.parsebox.examples.csv

import com.github.sadikovi.parsebox.api._

/** [[DefaultFormat]] to process comma-separated values with default delimiter */
class DefaultFormat extends HadoopFormat[Opt2RecordType] {
  override def recordClass(): Class[Opt2RecordType] = classOf[Opt2RecordType]

  override def filter(rawValue: String): Boolean = rawValue.startsWith("option1-0")

  override def process(rawValue: String, record: Opt2RecordType): Unit = {
    val arr = rawValue.split(',')
    record.key1 = arr(0)
    record.key2 = arr(1)
  }
}
