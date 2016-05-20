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

import scala.util.{Failure, Success, Try}

import com.github.sadikovi.parsebox.api.BaseFormat

/**
 * [[FormatRegistry]] interface allows to resolve provider into [[FormatReader]], which defines
 * entire parsing logic. Note that provider should point to one of the instances of [[BaseFormat]].
 * Resolves fully qualified class name into provider. Also keeps map of short names for pre-defined
 * formats.
 */
object FormatRegistry {
  /** Cases for resolving different short names for formats */
  private def resolveWithShortName(shortName: String): String = shortName match {
    case "example-json" => "com.github.sadikovi.parsebox.examples.json"
    case otherProvider => otherProvider
  }

  /**
   * Look up provider based on class name as string, also look up map of short names for pre-defined
   * formats.
   */
  private[parsebox] def lookupFormat(provider: String): Class[_] = {
    val provider1 = resolveWithShortName(provider)
    val provider2 = s"$provider1.DefaultFormat"
    val loader = Utils.getContextClassLoader()

    Try(loader.loadClass(provider)).orElse(Try(loader.loadClass(provider2))) match {
      case Success(resolvedClass) =>
        resolvedClass
      case Failure(error) =>
        throw new ClassNotFoundException(s"Failed to locate class for $provider", error)
    }
  }

  /** Create format reader from specified provider */
  def resolveFormat(provider: String): BaseFormat = {
    lookupFormat(provider).newInstance() match {
      case resolvedFormat: BaseFormat => resolvedFormat
      case _ => throw new UnsupportedOperationException(
        s"Provider $provider does not support BaseFormat interface")
    }
  }
}
