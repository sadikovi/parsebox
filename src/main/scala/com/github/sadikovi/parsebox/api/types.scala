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

package com.github.sadikovi.parsebox.api

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Abstract [[RecordType]] type for supported records. Subclasses should overwrite methods below,
 * and provide empty constructor (to resolve data schema), and provide setters for values (or be
 * mutable), so they can reused by Hadoop InputFormat.
 */
abstract class RecordType {
  /**
   * Convert current type into SQL Row object. This method should be in sync with data schema,
   * since, rows and schema will be used to create DataFrame. Here is an example of overwriting it:
   * {{{
   *  class CustomType(var opt1: Int, var opt2: String) extends RecordType {
   *    override def toRow(): Row = Row(opt1, opt2)
   *  }
   * }}}
   */
  def toRow(): Row

  /**
   * Provide data schema for current type using Spark SQL `StructType`. This should be in sync with
   * row conversion. It is recommended to keep schema flat for simplicity, but schema can be
   * arbitrary including map types, if required. Here is an example of implementing method:
   * {{{
   *  override def dataSchema(): StructType = StructType(
   *    StructField("opt1", IntegerType, false) :: StructField("opt2", StringType, true) :: Nil)
   * }}}
   */
  def dataSchema(): StructType
}
