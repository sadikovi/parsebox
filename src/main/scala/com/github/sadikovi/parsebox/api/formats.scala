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

import org.apache.spark.sql.SQLContext

/**
 * [[BaseFormat]] is a low-level generic interface to provide means to load custom parser based on
 * options below. Options for `createFormat()` are optional and usage of those depends on
 * user-defined format. User should need to subclass this format, and define their own parser class.
 * Note that it is recommended that class is called `DefaultFormat`, so user can provide package
 * name without specifying class.
 * {{{
 *  class DefaultFormat extends BaseFormat {
 *    override def createParser(
 *        sqlContext: SQLContext,
 *        paths: Array[String],
 *        parameters: Map[String, String]): ResolvedParser = {
 *      new CustomParser(sqlContext, paths, parameters)
 *    }
 *  }
 * }}}
 */
abstract class BaseFormat {
  def createParser(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: Map[String, String]): ResolvedParser
}
