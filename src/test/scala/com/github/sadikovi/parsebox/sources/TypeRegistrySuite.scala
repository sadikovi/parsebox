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

package com.github.sadikovi.parsebox.sources

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

import com.github.sadikovi.parsebox.api.{Opt1RecordType, Opt2RecordType, RecordType}
import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

// Dummy non-registered record type, without no-args constructor
sealed class DummyRecordType(var key: String) extends RecordType {
  override def toRow(): Row = Row(key)
  override def dataSchema(): StructType = StructType(
    StructField("opt1", StringType, true) :: Nil)
}

class TypeRegistrySuite extends UnitTestSpec {
  test("exists provided type") {
    TypeRegistry.exists(classOf[Opt1RecordType]) should be (true)
    TypeRegistry.exists(classOf[Opt2RecordType]) should be (true)
  }

  test("exists unknown type") {
    TypeRegistry.exists(classOf[DummyRecordType]) should be (false)
  }

  test("register unknown type") {
    TypeRegistry.exists(classOf[DummyRecordType]) should be (false)
    TypeRegistry.register(classOf[DummyRecordType]) should be (true)
    TypeRegistry.exists(classOf[DummyRecordType]) should be (true)
  }

  test("register known type") {
    TypeRegistry.register(classOf[Opt1RecordType]) should be (false)
  }

  test("lookup schema for valid type") {
    val schema = TypeRegistry.lookupSchema(classOf[Opt1RecordType])
    schema.isEmpty should be (false)
  }

  test("fail to lookup schema for type without non-args constructor") {
    TypeRegistry.register(classOf[DummyRecordType])
    val schema = TypeRegistry.lookupSchema(classOf[DummyRecordType])
    schema.isEmpty should be (true)
  }

  test("fail to lookup schema for invalid class") {
    val schema = TypeRegistry.lookupSchema(getClass)
    schema.isEmpty should be (true)
  }

  test("fail to lookup schema for unregistered class") {
    TypeRegistry.clearTempRegistry()
    val schema = TypeRegistry.lookupSchema(classOf[DummyRecordType])
    schema.isEmpty should be (true)
  }
}
