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

import java.util.UUID

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{Path => HadoopPath}

import org.apache.spark.network.util.JavaUtils

private[parsebox] object Utils {
  /**
   * Convert string of bytes (1024, 10Mb, 5Kb, etc.) into number of bytes.
   * Copied from Apache Spark `Utils.scala`.
   * @param str string to parse
   * @return number of bytes for corresponding string
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Create UUID for a string as 128-bit value string.
   * @param str string to create uuid for
   * @return generated UUID as string
   */
  def uuidForString(str: String): String = {
    UUID.nameUUIDFromBytes(str.getBytes()).toString()
  }

  /**
   * Get context class laoder on this thread or, if not present, default class loader for this
   * class.
   */
  def getContextClassLoader(): ClassLoader = {
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
  }

  /** Return updated path with suffix appended */
  def withSuffix(path: HadoopPath, suffix: String*): HadoopPath = {
    path.suffix(s"${HadoopPath.SEPARATOR}${suffix.mkString(HadoopPath.SEPARATOR)}")
  }

  /** Create temporary directory on local file system */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "netflow"): HadoopPath = {
    val dir = Utils.withSuffix(new HadoopPath(root), namePrefix, UUID.randomUUID().toString)
    val fs = dir.getFileSystem(new HadoopConf(false))
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path */
  private def withTempHadoopPath(path: HadoopPath)(func: HadoopPath => Unit): Unit = {
    try {
      func(path)
    } finally {
      val fs = path.getFileSystem(new HadoopConf(false))
      fs.delete(path, true)
    }
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(Utils.createTempDir())(func)
  }

  /** Execute code block with created temporary file */
  def withTempFile(func: HadoopPath => Unit): Unit = {
    val file = Utils.withSuffix(Utils.createTempDir(), UUID.randomUUID().toString)
    withTempHadoopPath(file)(func)
  }
}
