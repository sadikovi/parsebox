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

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SQLContext

/**
 * [[ParseboxContext]] interface is a main entry point to work with Parsebox. Provides common
 * functionality over creating parsers, registering custom types, and API to convert raw data into
 * DataFrame.
 */
final class ParseboxContext(@transient private val sqlContext: SQLContext) {
  // Internal context settings
  @transient protected val settings = new ConcurrentHashMap[String, String]()
  private val logger = LoggerFactory.getLogger(getClass())

  /** Set configuration for key */
  def setConf(key: String, value: String): ParseboxContext = {
    settings.put(key, value)
    logger.info(s"Added key $key with value $value")
    this
  }

  /** Get configuration key */
  def getConf(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Resolver provided format and return associated `FormatReader` */
  def format(provider: String): FormatReader = {
    FormatRegistry.lookupFormat(provider)
  }
}

/** Companion object to provide singleton access to context */
object ParseboxContext {
  private var currentContext: ParseboxContext = null

  /** Get or create new `ParseboxContext`, recommended usage pattern */
  def getOrCreate(sqlContext: SQLContext): ParseboxContext = {
    if (currentContext == null) {
      currentContext = new ParseboxContext(sqlContext)
    }

    currentContext
  }

  /** Create new `ParseboxContext`, for testing purposes */
  private[parsebox] def create(sqlContext: SQLContext): ParseboxContext = {
    new ParseboxContext(sqlContext)
  }
}
