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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.parsebox.Metrics

/** [[ResolvedParser]] interface to provide generic methods to process data and collect metrics */
abstract class ResolvedParser {
  /**
   * Return collected metrics. Note, since we return DataFrame, before evaluation of it, only
   * certain statistics are available, the rest will be collected after DataFrame materialization.
   */
  def getMetrics(): Metrics

  /** Create DataFrame with data */
  def create(): DataFrame

  /** Provide data schema for DataFrame */
  def dataSchema(): StructType
}
