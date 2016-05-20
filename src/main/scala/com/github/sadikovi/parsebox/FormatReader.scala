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

package com.github.sadikovi.parsebox

import scala.collection.mutable.{HashMap => MutableMap}

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.parsebox.api.{BaseFormat, ResolvedParser}

/**
 * [[FormatReader]] interface transforms [[BaseFormat]] and [[ResolvedParser]] into DataFrame with
 * additional parsing options.
 */
case class FormatReader(
    private val format: BaseFormat,
    @transient private val sqlContext: SQLContext) {
  // Format reader settings that will be pushed down to parser
  private val extraOptions = new MutableMap[String, String]()

  /** Compare both schemas ignoring nullability */
  private[parsebox] def verifySchemaIgnoreNullability(
      left: StructType,
      right: StructType): Boolean = {
    left.size == right.size &&
      left.map { field => (field.name, field.dataType) } ==
        right.map { field => (field.name, field.dataType) }
  }

  /** Add option to `FormatReader` */
  def option(key: String, value: String): FormatReader = {
    extraOptions += (key -> value)
    this
  }

  /** Get specified options */
  private[parsebox] def getOptions(): Map[String, String] = extraOptions.toMap

  /** Post-process DataFrame and also apply validation rules */
  private[parsebox] def processDataFrame(df: DataFrame, expectedSchema: StructType): DataFrame = {
    require(verifySchemaIgnoreNullability(df.schema, expectedSchema),
      s"Provided schema $expectedSchema does not match actual schema ${df.schema}")
    // return DataFrame after validating schema
    df
  }

  /** Load DataFrame using provided format with paths and options */
  def load(paths: Array[String]): DataFrame = {
    val parser: ResolvedParser = format.createParser(sqlContext, paths, extraOptions.toMap)
    val df = parser.create()
    processDataFrame(df, parser.dataSchema())
  }

  /** Load DataFrame using provided format, without any file system path (for Cassandra) */
  def load(): DataFrame = {
    load(Array.empty[String])
  }

  /** Load DataFrame using provided format with file path (pattern) */
  def load(path: String): DataFrame = {
    load(Array(path))
  }
}
