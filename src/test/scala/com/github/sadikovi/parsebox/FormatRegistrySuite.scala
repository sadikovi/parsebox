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

import com.github.sadikovi.parsebox.api.BaseFormat
import com.github.sadikovi.testutil.{UnitTestSpec, SparkLocal}
import com.github.sadikovi.testutil.implicits._

class FormatRegistrySuite extends UnitTestSpec {
  test("lookup format for short name") {
    val klass = FormatRegistry.lookupFormat("example-json")
    klass.newInstance().isInstanceOf[BaseFormat] should be (true)
  }

  test("lookup format for provider package") {
    val klass = FormatRegistry.lookupFormat("com.github.sadikovi.parsebox.examples.json")
    klass.newInstance().isInstanceOf[BaseFormat] should be (true)
  }

  test("lookup format for full provider name") {
    val klass = FormatRegistry.
      lookupFormat("com.github.sadikovi.parsebox.examples.json.DefaultFormat")
    klass.newInstance().isInstanceOf[BaseFormat] should be (true)
  }

  test("lookup format for unknown provider") {
    intercept[ClassNotFoundException] {
      FormatRegistry.lookupFormat("xyz.test")
    }
  }

  test("resolve format") {
    val format = FormatRegistry.resolveFormat("example-json")
    format.isInstanceOf[BaseFormat] should be (true)
  }

  test("fail to resolve format") {
    intercept[UnsupportedOperationException] {
      FormatRegistry.resolveFormat(getClass.getCanonicalName)
    }
  }
}
