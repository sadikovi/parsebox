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

import java.nio.charset.Charset

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit => NewInputSplit,
  RecordReader => NewHadoopRecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{LineRecordReader => NewLineRecordReader}

import com.github.sadikovi.parsebox.api.RecordType

/**
 * [[FilteredLineReader]] implements Hadoop `RecordReader` interface and provides methods to
 * process record, handle failures, and apply filtering. Note that it is recommended to use
 * `onFailure` to handled exceptions and not use regular expressions, unless patterns are created
 * globally, not per record (affects performance).
 */
private[parsebox] abstract class FilteredLineReader[T<:RecordType](
    private val recordClass: Class[T],
    private val delimiter: Array[Byte])
  extends NewHadoopRecordReader[T, NullWritable] {

  // Underlying Hadoop line reader, this class will change implementation depending on delimiter
  private val recordReader = new NewLineRecordReader(delimiter)
  // Current key writable
  private var key: T = _

  /** Constructor with delimiter as UTF-8 string */
  def this(recordClass: Class[T], delimiterString: String) = this(recordClass,
    if (delimiterString == null) null else delimiterString.getBytes(Charset.forName("UTF-8")))

  override def initialize(genericSplit: NewInputSplit, context: TaskAttemptContext): Unit = {
    recordReader.initialize(genericSplit, context)
  }

  override def getCurrentKey(): T = key

  override def getCurrentValue(): NullWritable = NullWritable.get()

  override def getProgress(): Float = {
    recordReader.getProgress()
  }

  override def nextKeyValue(): Boolean = {
    if (key == null) {
      key = recordClass.newInstance()
    }

    var continue = true
    var next = true
    while (continue) {
      next = recordReader.nextKeyValue()
      if (!next) {
        // underlying record reader does not have content to read, using weird hack to bypass
        // assignment to null directly
        key = null.asInstanceOf[T]
        next = false
        continue = false
      } else {
        // for text reader is byte position in a file
        val rawKey = recordReader.getCurrentKey().get()
        // content for that starting byte position
        val rawValue = recordReader.getCurrentValue().toString()
        // check if record passes predicate, if not, skip to the next underlying record
        if (keepRecord(rawValue)) {
          try {
            // invoke custom processing
            processRecord(rawValue, key)
            continue = false
          } catch {
            case NonFatal(err) =>
              // handling exceptions
              onFailure(rawValue, err)
              continue = true
          }
        } else {
          continue = true
        }
      }
    }

    next
  }

  override def close(): Unit = {
    recordReader.close()
  }

  /**
   * Whether or not to continue processing if `rawValue` is encountered. If method returns `false`,
   * then record is skipped, otherwise `processRecord()` is invoked. Note that regular expressions
   * are not recommended, unless creating global varialbe of a regex pattern for a class.
   */
  def keepRecord(rawValue: String): Boolean

  /**
   * Process raw value as String and fill the key, key is guaranteed to be non-null. Note that it
   * is not recommended to use `try-catch` blocks inside this method, instead implement `onFailure`.
   * This will allow to skip wrong records without post-filtering by user.
   */
  def processRecord(rawValue: String, record: T): Unit

  /** Handle error that happened during processing in `processRecord` */
  def onFailure(rawValue: String, error: Throwable): Unit
}
