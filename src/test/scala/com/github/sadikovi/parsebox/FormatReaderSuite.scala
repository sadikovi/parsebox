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

import org.scalatest.ConfigMap
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType}

import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class FormatReaderSuite extends UnitTestSpec with SparkLocal {
  // define global Spark SQL context
  var sqlContext: SQLContext = null

  override def beforeAll(configMap: ConfigMap) {
    startSparkContext()
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(configMap: ConfigMap) {
    stopSparkContext()
    sqlContext = null
  }

  test("verify schema") {
    val fr = FormatReader(null, sqlContext)
    // different nullability flag
    fr.verifySchemaIgnoreNullability(
      StructType(StructField("a", StringType, false) :: Nil),
      StructType(StructField("a", StringType, true) :: Nil)) should be (true)

    // identical schema
    fr.verifySchemaIgnoreNullability(
      StructType(StructField("a", StringType, false) :: Nil),
      StructType(StructField("a", StringType, false) :: Nil)) should be (true)

    // different field type
    fr.verifySchemaIgnoreNullability(
      StructType(StructField("a", StringType, true) :: Nil),
      StructType(StructField("a", IntegerType, true) :: Nil)) should be (false)

    // different set of columns
    fr.verifySchemaIgnoreNullability(
      StructType(StructField("a", StringType, true) :: StructField("b", StringType, true) :: Nil),
      StructType(StructField("a", StringType, true) :: Nil)) should be (false)
  }

  test("add option") {
    val fr = FormatReader(null, sqlContext)
    fr.option("key1", "value1").option("key2", "value2").option("key3", "value3")
    fr.getOptions() should be (Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))
  }

  test("post-process DataFrame") {
    val fr = FormatReader(null, sqlContext)
    val df = sqlContext.range(0, 10)
    val res = fr.processDataFrame(df, StructType(StructField("id", LongType, false) :: Nil))
    (res eq df) should be (true)
  }

  test("fail to post-process DataFrame") {
    val fr = FormatReader(null, sqlContext)
    val df = sqlContext.range(0, 10)
    intercept[IllegalArgumentException] {
      fr.processDataFrame(df, StructType(StructField("abc", StringType, false) :: Nil))
    }
  }
}
