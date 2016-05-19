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

package com.github.sadikovi.parsebox.impl.formats.netflow2

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import com.github.sadikovi.parsebox.api._

////////////////////////////////////////////////////////////////
// Default format for NetFlow using low-level ResolvedParser
////////////////////////////////////////////////////////////////

class DefaultFormat extends BaseFormat {
  override def createParser(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: Map[String, String]): ResolvedParser = {
    new NetFlowParser(sqlContext, paths, parameters)
  }
}

class NetFlowParser(
    @transient val sqlContext: SQLContext,
    val paths: Array[String],
    parameters: Map[String, String])
  extends ExternalParser[Opt2RecordType] {

  override def create(): DataFrame = {
    val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
      options(parameters).load(paths.headOption.getOrElse(""))
    df.select(df("srcip").as("opt1"), df("dstip").as("opt2"))
  }
}
