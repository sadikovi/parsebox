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

import java.util.{HashSet => JHashSet}

import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.parsebox.api.{RecordType, Opt1RecordType, Opt2RecordType}

/** Type registry to lookup type information and register external types */
object TypeRegistry {
  // Map of provided external types
  private val externalTypes: JHashSet[Class[_<:RecordType]] = new JHashSet()
  private val logger = LoggerFactory.getLogger(getClass())

  /** Provided internal types that we know from the beginning */
  private def providedTypes: Set[Class[_<:RecordType]] = Set(
    classOf[Opt1RecordType],
    classOf[Opt2RecordType])

  /** Check if class is registered already or provided external class */
  def exists(klass: Class[_<:RecordType]): Boolean = {
    providedTypes.contains(klass) || externalTypes.contains(klass)
  }

  /** Register external class */
  def register(klass: Class[_<:RecordType]): Boolean = {
    if (exists(klass)) {
      // Return false since class is already registered
      logger.info(s"Class $klass is already registered, no-op")
      false
    } else {
      // Register class as an external provider
      logger.info(s"Adding new class $klass to registry")
      externalTypes.add(klass)
    }
  }

  /** Clear temporary set of registered classes */
  def clearTempRegistry(): Unit = {
    externalTypes.clear()
  }

  /** Since we work with `RecordType` we need to expose API to get data schema */
  def lookupSchema(klass: Class[_]): Option[StructType] = {
    val params: Array[Class[_]] = Array.empty
    try {
      val castClass = klass.asInstanceOf[Class[RecordType]]
      require(exists(castClass), s"Class $castClass is not registered")
      val constructor = castClass.getDeclaredConstructor(params: _*)
      val recordType: RecordType = constructor.newInstance()
      Some(recordType.dataSchema)
    } catch {
      case ia: IllegalArgumentException =>
        logger.error(s"Failed to get constructor", ia)
        None
      case cc: ClassCastException =>
        logger.error(s"Failed to cast $klass to RecordType", cc)
        None
      case nsm: NoSuchMethodException =>
        logger.error(s"Failed to load type $klass, does not provide empty constructor", nsm)
        None
    }
  }
}
