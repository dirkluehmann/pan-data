/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io.csv

import com.univocity.parsers.common.processor.ColumnProcessor
import com.univocity.parsers.csv.{CsvParser, CsvWriterSettings, CsvWriter as UVWriter}
import pd.io.StringDecoding.{StringDecoder, autoDecode}
import pd.io.{ReadAdapter, StringDecoding, WriteAdapter}
import pd.{DataFrame, Series}

import java.io.*
import java.net.URL
import java.nio.charset.Charset
import java.util.List
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success, Using}

/**
  * Writing a DataFrame to CSV format.
  *
  * @param header
  *   If true, adds a header line with column names.
  * @param delimiter
  *   Delimiter character between columns.
  * @param lineSeparator
  *   Line separator.
  * @param quote
  *   Quote character.
  * @param charset
  *   Charset encoding.
  * @param nullEncoding
  *   Representation of a undefined (missing) value. The default is an empty string.
  * @param skipEmptyLines
  *   If false, rows are written even if all values are undefined (null).
  * @since 0.1.0
  */
class CsvWriter(
    df: DataFrame,
    header: Boolean = true,
    delimiter: Char = '\u0000',
    lineSeparator: String = "",
    quote: Char = '\u0000',
    charset: Charset = Charset.defaultCharset(),
    nullEncoding: String = "",
    skipEmptyLines: Boolean = false,
):

  /**
    * Writes the DataFrame to a file.
    *
    * @param path
    *   Path to file.
    * @return
    *   The unaltered DataFrame for chaining operations.
    * @since 0.1.0
    */
  def write(path: String): DataFrame =
    Using(BufferedWriter(FileWriter(path, charset)))(writer => write(writer)) match
      case Success(df) => df
      case Failure(e)  => throw e

  /**
    * Writes the DataFrame to a Writer.
    *
    * @param writer
    *   Writer instance.
    * @return
    *   The unaltered DataFrame for chaining operations.
    * @since 0.1.0
    */
  def write(writer: Writer): DataFrame =
    val settings = CsvWriterSettings()

    if delimiter != '\u0000' then settings.getFormat.setDelimiter(delimiter)
    if lineSeparator.nonEmpty then settings.getFormat.setLineSeparator(lineSeparator)
    if quote != '\u0000' then settings.getFormat.setQuote(quote)
    settings.setHeaderWritingEnabled(header)
    if header then settings.setHeaders(df.columns*)
    settings.setSkipEmptyLines(skipEmptyLines)

    val csvWriter = UVWriter(writer, settings)

    val cols = df.columnArray
    val length = cols.length
    val it = df.indexIterator

    while it.hasNext do
      val row = it.next()
      var c = 0
      val array = new Array[String](length)
      while c < length do
        array(c) = cols(c)(row).map(_.toString).orNull
        c = c + 1
      csvWriter.writeRow(array)

    df

/**
  * Writing a DataFrame to CSV format.
  *
  * @since 0.1.0
  */
object CsvWriter:

  class Adapter(df: DataFrame):
    /**
      * Writing DataFrame objects to CSV format.
      *
      * @since 0.1.0
      */
    def csv: CsvWriter = CsvWriter(df)

    /**
      * Writing a DataFrame to CSV format.
      *
      * @param header
      *   If true, adds a header line with column names.
      * @param delimiter
      *   Delimiter character between columns.
      * @param lineSeparator
      *   Line separator.
      * @param quote
      *   Quote character.
      * @param charset
      *   Charset encoding.
      * @param nullEncoding
      *   Representation of a undefined (missing) value. The default is an empty string.
      * @param skipEmptyLines
      *   If false, rows are written even if all values are undefined (null).
      * @since 0.1.0
      */
    def csv(
        header: Boolean = true,
        delimiter: Char = '\u0000',
        lineSeparator: String = "",
        quote: Char = '\u0000',
        charset: Charset = Charset.defaultCharset(),
        nullEncoding: String = "",
        skipEmptyLines: Boolean = true,
    ): CsvWriter =
      CsvWriter(df, header, delimiter, lineSeparator, quote, charset, nullEncoding, skipEmptyLines)
