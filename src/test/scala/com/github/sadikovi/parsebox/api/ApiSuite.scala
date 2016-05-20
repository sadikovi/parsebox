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

import org.scalatest.ConfigMap
import org.apache.spark.sql.{SQLContext, DataFrame}

import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class ApiSuite extends UnitTestSpec with SparkLocal {
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

  test("check Opt1RecordType") {
    val opt1 = new Opt1RecordType("test")
    val rdd = sc.parallelize(Seq(opt1.toRow))
    val df = sqlContext.createDataFrame(rdd, opt1.dataSchema)
    df.count should be (1)
  }

  test("check Opt2RecordType") {
    val opt2 = new Opt2RecordType("test1", "test2")
    val rdd = sc.parallelize(Seq(opt2.toRow))
    val df = sqlContext.createDataFrame(rdd, opt2.dataSchema)
    df.count should be (1)
  }

  test("check ExternalParser schema") {
    val ep = new ExternalParser(classOf[Opt2RecordType]) {
      override def create(): DataFrame = null
    }
    ep.dataSchema should be (new Opt2RecordType().dataSchema)
  }

  test("check metrics implementation for parsers") {
    val ep = new ExternalParser(classOf[Opt2RecordType]) {
      override def create(): DataFrame = null
    }

    intercept[UnsupportedOperationException] {
      ep.getMetrics()
    }
  }
}
