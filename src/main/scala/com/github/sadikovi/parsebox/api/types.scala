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
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

/**
 * Abstract [[RecordType]] type for supported records. Subclasses should overwrite methods below,
 * and provide empty constructor (to resolve data schema), and provide setters for values (or be
 * mutable), so they can reused by Hadoop InputFormat. It is also recommended to overwrite
 * `hashCode` function to efficiency.
 * Since no-args constructor is required, there is no need to provide constructor with parameters,
 * just different setters is already enough for mutable type.
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

/**
 * Custom [[Opt1RecordType]] to hold only one string field.
 * Used as an example of creating user-defined record type, note that default empty constructor and
 * mutable fields are provided.
 */
class Opt1RecordType(var key: String) extends RecordType {
  def this() = this(null)

  def setKey(value: String): Unit = {
    key = value
  }

  override def toRow(): Row = Row(key)

  override def hashCode(): Int = {
    var result: Int = 1
    result = 31 * result + (if (key == null) 0 else key.hashCode())
    result
  }

  override def dataSchema(): StructType = StructType(
    StructField("opt1", StringType, true) :: Nil)
}

/**
 * Custom [[Opt2RecordType]] to hold only two string fields.
 * Used as an example of creating user-defined record type, note that default empty constructor and
 * mutable fields are provided.
 */
class Opt2RecordType(var key1: String, var key2: String) extends RecordType {
  def this() = this(null, null)

  def setKey1(value: String): Unit = {
    key1 = value
  }

  def setKey2(value: String): Unit = {
    key2 = value
  }

  override def hashCode(): Int = {
    var result: Int = 1
    result = 31 * result + (if (key1 == null) 0 else key1.hashCode())
    result = 31 * result + (if (key2 == null) 0 else key2.hashCode())
    result
  }

  override def toRow(): Row = Row(key1, key2)

  override def dataSchema(): StructType = StructType(
    StructField("opt1", StringType, true) :: StructField("opt2", StringType, true) :: Nil)
}
