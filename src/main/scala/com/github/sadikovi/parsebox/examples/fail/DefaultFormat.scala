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

package com.github.sadikovi.parsebox.examples.fail

import com.github.sadikovi.parsebox.api._

class DefaultFormat extends HadoopFormat[Opt2RecordType] {
  override def recordClass(): Class[Opt2RecordType] = classOf[Opt2RecordType]

  override def filter(rawValue: String): Boolean = true

  override def fail(rawValue: String, error: Throwable): Unit = error match {
    case iae: IllegalArgumentException => // do nothing
  }

  override def process(rawValue: String, record: Opt2RecordType): Unit = {
    throw new IllegalArgumentException("test")
  }
}
