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
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import com.github.sadikovi.parsebox.examples.csv.{DefaultFormat => CsvFormat}
import com.github.sadikovi.parsebox.examples.fail.{DefaultFormat => FailFormat}
import com.github.sadikovi.parsebox.examples.json.{DefaultFormat => JsonFormat}
import com.github.sadikovi.parsebox.examples.splunk.{DefaultFormat => SplunkFormat}
import com.github.sadikovi.parsebox.examples.zip.{DefaultFormat => ZipFormat}
import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class ApiSuite extends UnitTestSpec with SparkLocal {
  // define global Spark SQL context
  var sqlContext: SQLContext = null
  // resources for testing API
  val jsonPath = testDirectory / "resources" / "json" / "options.json"
  val csvPath = testDirectory / "resources" / "csv" / "options.csv"
  val splunkPath = testDirectory / "resources" / "splunk" / "sample.txt"
  val zipSinglePath = testDirectory / "resources" / "zip" / "single.zip"
  val zipMultiPath = testDirectory / "resources" / "zip" / "multi.zip"
  val zipEmptyPath = testDirectory / "resources" / "zip" / "empty.zip"

  override def beforeAll(configMap: ConfigMap) {
    startSparkContext()
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(configMap: ConfigMap) {
    stopSparkContext()
    sqlContext = null
  }

  test("check Opt1RecordType") {
    val opt = new Opt1RecordType("test")
    val rdd = sc.parallelize(Seq(opt.toRow))
    val df = sqlContext.createDataFrame(rdd, opt.dataSchema)
    df.count should be (1)
  }

  test("check Opt1RecordType setters") {
    val opt = new Opt1RecordType()
    opt.setKey("test")
    opt.key should be ("test")
  }

  test("check Opt1RecordType hashCode") {
    val opt1 = new Opt1RecordType()
    val opt2 = new Opt1RecordType()
    opt1.hashCode() should be (opt2.hashCode())
    // set different key values and compare hash codes
    opt1.setKey("a")
    opt2.setKey("b")
    opt1.hashCode() shouldNot be (opt2.hashCode())
  }

  test("check Opt2RecordType") {
    val opt = new Opt2RecordType("test1", "test2")
    val rdd = sc.parallelize(Seq(opt.toRow))
    val df = sqlContext.createDataFrame(rdd, opt.dataSchema)
    df.count should be (1)
  }

  test("check Opt2RecordType setters") {
    val opt = new Opt2RecordType()
    opt.setKey1("a")
    opt.setKey2("b")
    opt.key1 should be ("a")
    opt.key2 should be ("b")
  }

  test("check Opt2RecordType hashCode") {
    val opt1 = new Opt2RecordType()
    val opt2 = new Opt2RecordType()
    opt1.hashCode() should be (opt2.hashCode())
    // set different key1
    opt1.setKey1("a")
    opt2.setKey1("b")
    opt1.hashCode() shouldNot be (opt2.hashCode())
    // set different key2, but same key1
    opt1.setKey1("1")
    opt1.setKey2("a")
    opt2.setKey1("1")
    opt2.setKey1("b")
    opt1.hashCode() shouldNot be (opt2.hashCode())
  }

  test("override create method for BaseFormat") {
    val bf = new BaseFormat {
      override def dataSchema(): StructType = null
    }

    val err = intercept[UnsupportedOperationException] {
      bf.create(null, Array.empty[String], Map.empty[String, String])
    }
    err.getMessage.contains("Must implement either Scala or Java methods") should be (true)
  }

  test("unresolved schema for unregistered type in TypedFormat") {
    class UnregisteredType extends RecordType {
      override def toRow() = Row("test")
      override def dataSchema(): StructType =
        StructType(StructField("test", StringType, false) :: Nil)
    }

    val tf = new TypedFormat[UnregisteredType] {
      override def recordClass(): Class[UnregisteredType] = classOf[UnregisteredType]
    }

    val err = intercept[RuntimeException] {
      tf.dataSchema()
    }
    err.getMessage().contains("Could not resolve schema for type") should be (true)
  }

  test("check ExternalFormat schema") {
    val ep = new ExternalFormat[Opt2RecordType] {
      override def recordClass(): Class[Opt2RecordType] = classOf[Opt2RecordType]
      override def create(
          sqlContext: SQLContext,
          paths: Array[String],
          parameters: Map[String, String]): DataFrame = null
    }
    ep.dataSchema should be (new Opt2RecordType().dataSchema)
  }

  test("check metrics implementation for parsers") {
    val ep = new ExternalFormat[Opt2RecordType] {
      override def recordClass(): Class[Opt2RecordType] = classOf[Opt2RecordType]
      override def create(
          sqlContext: SQLContext,
          paths: Array[String],
          parameters: Map[String, String]): DataFrame = null
    }

    intercept[UnsupportedOperationException] {
      ep.getMetrics()
    }
  }

  test("check default implementations of HadoopFormat") {
    val hf = new HadoopFormat[Opt2RecordType] {
      override def recordClass(): Class[Opt2RecordType] = classOf[Opt2RecordType]
      override def filter(rawValue: String) = true
      override def process(rawValue: String, record: Opt2RecordType): Unit = {}
    }

    hf.isSplittable(null, null) should be (false)
    hf.delimiter() should be (null)
    intercept[IllegalArgumentException] {
      hf.fail("test", new IllegalArgumentException("test"))
    }
  }

  //////////////////////////////////////////////////////////////
  // Test default formats
  //////////////////////////////////////////////////////////////

  test("read json file using example default format") {
    val json = new JsonFormat()
    val df = json.create(sqlContext, Array(jsonPath), Map.empty[String, String])
    checkAnswer(df, sqlContext.read.json(jsonPath))
  }

  test("read csv file using example default format") {
    val csv = new CsvFormat()
    val df = csv.create(sqlContext, Array(csvPath), Map.empty[String, String])
    checkAnswer(df, Row("option1-0", "option2-0") :: Nil)
  }

  test("correctly read failed records for csv file") {
    val csv = new FailFormat()
    val df = csv.create(sqlContext, Array(csvPath), Map.empty[String, String])
    df.count() should be (0)
  }

  test("read sample file using default format with custom delimiter") {
    val t = sqlContext
    val splunk = new SplunkFormat()
    val df = splunk.create(sqlContext, Array(splunkPath), Map.empty[String, String])
    checkAnswer(df,
      Row("999", "4624") ::
      Row("1001", "4625") ::
      Row("1003", "4624") ::
      Row("1004", "4624") ::
      Row("1006", "4625") ::
      Row("1007", "4625") ::
      Row("1010", "4624") :: Nil)
  }

  test("read zip file with single entry") {
    val zip = new ZipFormat()
    val df = zip.create(sqlContext, Array(zipSinglePath), Map.empty[String, String])
    checkAnswer(df,
      Row("option1-0,option2-0") ::
      Row("option1-1,option2-1") ::
      Row("option1-2,option2-2") ::
      Row("option1-3,option2-3") ::
      Row("option1-4,option2-4") :: Nil)
  }

  test("read zip file with multiple entries (2)") {
    val zip = new ZipFormat()
    val df = zip.create(sqlContext, Array(zipMultiPath), Map.empty[String, String])
    checkAnswer(df,
      Row("option1-0,option2-0") ::
      Row("option1-1,option2-1") ::
      Row("option1-2,option2-2") ::
      Row("option1-3,option2-3") ::
      Row("option1-4,option2-4") ::
      Row("""{"opt1": "option1-0", "opt2": "option2-0"}""") ::
      Row("""{"opt1": "option1-1", "opt2": "option2-1"}""") ::
      Row("""{"opt1": "option1-2", "opt2": "option2-2"}""") ::
      Row("""{"opt1": "option1-3", "opt2": "option2-3"}""") ::
      Row("""{"opt1": "option1-4", "opt2": "option2-4"}""") :: Nil)
  }

  test("read empty zip file") {
    val zip = new ZipFormat()
    val df = zip.create(sqlContext, Array(zipEmptyPath), Map.empty[String, String])
    df.count() should be (0)
  }
}
