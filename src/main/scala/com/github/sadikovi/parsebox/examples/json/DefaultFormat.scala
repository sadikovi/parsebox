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

package com.github.sadikovi.parsebox.examples.json

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import com.github.sadikovi.parsebox.api._

/** Default format for JSON using low-level ExternalParser */
class DefaultFormat extends BaseFormat {
  override def createParser(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: Map[String, String]): ResolvedParser = {
    new ExternalParser[Opt2RecordType] {
      override def create(): DataFrame = {
        // We use hack of the first path, since multiple paths for DataFrameReader/Writer are
        // supported since 1.6.0
        val df = sqlContext.read.options(parameters).json(paths.headOption.getOrElse(""))
        df.select("opt1", "opt2")
      }
    }
  }
}
