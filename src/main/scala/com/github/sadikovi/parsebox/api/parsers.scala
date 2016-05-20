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

import scala.reflect.ClassTag

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.parsebox.sources.{Metrics, TypeRegistry}

/**
 * [[ResolvedParser]] interface to provide generic methods to process data and collect metrics.
 * Note that this is very low-level API and it is not recommended to subclass it directly unless
 * very custom logic is required. Instead, consider using nice wrapping subclasses defined below.
 */
abstract class ResolvedParser {
  /**
   * Return collected metrics. Note, since we return DataFrame, before evaluation of it, only
   * certain statistics are available, the rest will be collected after DataFrame materialization.
   */
  def getMetrics(): Metrics = {
    throw new UnsupportedOperationException("Not implemented")
  }

  /** Create DataFrame with data */
  def create(): DataFrame

  /** Provide data schema for DataFrame */
  def dataSchema(): StructType
}

/**
 * [[ExternalParser]] provides API for custom low-level processing using 3rd party package, e.g.
 * one of the Spark packages, such as JSON, CSV, JDBC, etc. Note that type is defined by provided
 * record type, and final DataFrame schema should match defined type schema, otherwise exception is
 * thrown.
 */
abstract class ExternalParser(recordType: Class[_<:RecordType]) extends ResolvedParser {
  def create(): DataFrame

  final override def dataSchema(): StructType = {
    TypeRegistry.lookupSchema(recordType).getOrElse(
      sys.error(s"Could not resolve schema for type $recordType"))
  }
}
