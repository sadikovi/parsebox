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

import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class ParseboxContextSuite extends UnitTestSpec with SparkLocal {
  // define global Spark SQL context
  var sqlContext: SQLContext = null
  val jsonPath1 = testDirectory / "resources" / "json" / "options.json"

  override def beforeAll(configMap: ConfigMap) {
    startSparkContext()
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(configMap: ConfigMap) {
    stopSparkContext()
    sqlContext = null
  }

  test("set and get configuration") {
    val pc = new ParseboxContext(sqlContext)
    pc.getConf("key1") should be (None)
    pc.setConf("key2", "val2").getConf("key2") should be (Some("val2"))
    pc.setConf("key3", "val3").setConf("key3", "val4").getConf("key3") should be (Some("val4"))
  }

  test("read correct format") {
    val pc = new ParseboxContext(sqlContext)
    val reader = pc.format("com.github.sadikovi.parsebox.examples.json")
    reader.isInstanceOf[FormatReader] should be (true)
  }

  test("fail if format is unresolved") {
    val pc = new ParseboxContext(sqlContext)
    intercept[ClassNotFoundException] {
      pc.format("xyz.test")
    }
  }

  test("load correct format") {
    val pc = new ParseboxContext(sqlContext)
    val df = pc.format("com.github.sadikovi.parsebox.examples.json").load(jsonPath1)
    df.count() should be (10)
  }

  test("create context") {
    val pc1 = ParseboxContext.create(sqlContext)
    val pc2 = ParseboxContext.create(sqlContext)
    (pc1 eq pc2) should be (false)
  }

  test("get or create context") {
    val pc1 = ParseboxContext.getOrCreate(sqlContext)
    val pc2 = ParseboxContext.getOrCreate(sqlContext)
    (pc1 eq pc2) should be (true)
  }
}
