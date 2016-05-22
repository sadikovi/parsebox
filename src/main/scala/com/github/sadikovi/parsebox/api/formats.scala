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

import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob, InputSplit => NewInputSplit,
  JobContext => NewJobContext, RecordReader => NewHadoopRecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

import com.github.sadikovi.parsebox.sources.{Metrics, TypeRegistry, FilteredLineReader}

/**
 * [[BaseFormat]] interface to provide generic methods to process data and collect metrics.
 * Note that this is very low-level API and it is not recommended to subclass it directly unless
 * very custom logic is required. Instead, consider using nice wrapping subclasses defined below.
 */
trait BaseFormat {
  /**
   * Return collected metrics. Note, since we return DataFrame, before evaluation of it, only
   * certain statistics are available, the rest will be collected after DataFrame materialization.
   */
  def getMetrics(): Metrics = {
    throw new UnsupportedOperationException("Not implemented")
  }

  /** Create DataFrame with data */
  def create(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: Map[String, String]): DataFrame = {
    throw new UnsupportedOperationException(
      "Must implement either Scala or Java methods 'create(sqlContext, paths, parameters)'")
  }

  def create(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: JavaMap[String, String]): DataFrame = {
    create(sqlContext, paths, parameters.asScala.toMap)
  }

  /** Provide data schema for DataFrame */
  def dataSchema(): StructType
}

/**
 * [[TypedFormat]] allows to specify [[RecordType]] instead of creating schema manually.
 * All methods except `dataSchema()` should be overwritten, similar to [[BaseFormat]].
 */
trait TypedFormat[T<:RecordType] extends BaseFormat {

  /** Get class for individual record. Usually it is just classOf[MyRecordType] */
  def recordClass(): Class[T]

  override final def dataSchema(): StructType = {
    val foundClass = recordClass()
    TypeRegistry.lookupSchema(foundClass).getOrElse(
      sys.error(s"Could not resolve schema for type $foundClass"))
  }
}

/**
 * [[ExternalFormat]] provides API for custom low-level processing using 3rd party package, e.g.
 * one of the Spark packages, such as JSON, CSV, JDBC, etc. Note that type is defined by provided
 * record type, and final DataFrame schema should match defined type schema, otherwise exception is
 * thrown.
 */
abstract class ExternalFormat[T<:RecordType] extends TypedFormat[T]

/**
 * [[HadoopFormat]] provides API for custom file-based parser. Hadoop `InputFormat` resolves file
 * types and compression codecs automatically, so one needs to just implement record processing.
 * Note that type is supposed to be mutable, so each processing is just filling up type properties.
 * By default files are non-splittable, this behaviour can be overwritten, but underlying
 * implementation will still check if it is possible to split file.
 * Delimiter, by default, is null, which forces usage of `\n` new line separator, though any UTF-8
 * string can be used as a delimiter (long delimiters might affect performance).
 * Filtering results in function call for every string regardless of actual implementation, it is
 * not recommended to use regular expressions, unless they are defined globally.
 */
abstract class HadoopFormat[T<:RecordType]
    extends NewFileInputFormat[T, NullWritable] with TypedFormat[T] {

  /** Get class for individual record. Usually it is just classOf[MyRecordType] */
  def recordClass(): Class[T]

  /** Whether or not file should be split for processing, by default is `false` */
  def isSplittable(context: NewJobContext, file: HadoopPath): Boolean = false

  /** Provide custom delimiter for a record, if delimiter is a new line (`\n`) do not overwrite */
  def delimiter(): String = null

  /**
   * Whether or not to continue processing if `rawValue` is encountered. If method returns `false`,
   * then record is skipped, otherwise `processRecord()` is invoked. Note that regular expressions
   * are not recommended, unless creating global varialbe of a regex pattern for a class. If all
   * records should not be skipped overwrite it with return value `true`.
   */
  def filter(rawValue: String): Boolean

  /**
   * Process raw value as String and fill the key, key is guaranteed to be non-null. Note that it
   * is not recommended to use `try-catch` blocks inside this method, instead implement `onFailure`.
   * This will allow to skip wrong records without post-filtering by user.
   */
  def process(rawValue: String, record: T): Unit

  /** Handle error that happened during processing in `processRecord`, throws error by default */
  def fail(rawValue: String, error: Throwable): Unit = throw error

  override private[parsebox] def createRecordReader(
      split: NewInputSplit,
      context: TaskAttemptContext): NewHadoopRecordReader[T, NullWritable] = {
    new FilteredLineReader[T](recordClass(), delimiter()) {
      override def keepRecord(rawValue: String): Boolean = filter(rawValue)
      override def processRecord(rawValue: String, record: T): Unit = process(rawValue, record)
      override def onFailure(rawValue: String, error: Throwable): Unit = fail(rawValue, error)
    }
  }

  override protected def isSplitable(context: NewJobContext, file: HadoopPath): Boolean = {
    !isSplittable(context, file) || {
      val codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file)
      if (codec == null) true else codec.isInstanceOf[SplittableCompressionCodec]
    }
  }

  /** Get internal class as InputFormat */
  private def inputFormatClass(): Class[HadoopFormat[T]] = {
    getClass().asInstanceOf[Class[HadoopFormat[T]]]
  }

  override final def create(
      sqlContext: SQLContext,
      paths: Array[String],
      parameters: Map[String, String]): DataFrame = {
    val sparkContext = sqlContext.sparkContext
    val job = NewHadoopJob.getInstance(sparkContext.hadoopConfiguration)
    // Set all input paths provided, and return updated job configuration
    val hadoopPaths = paths.map { each => new HadoopPath(each) }
    NewFileInputFormat.setInputPaths(job, hadoopPaths: _*)
    val updatedConf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    // Create RDD of row copies (potentially double copy) of internal input format
    val rdd = sparkContext.newAPIHadoopRDD(updatedConf,
      inputFormatClass(),
      recordClass(),
      classOf[NullWritable]).map { writable => writable._1.toRow() }
    sqlContext.createDataFrame(rdd, dataSchema())
  }
}
