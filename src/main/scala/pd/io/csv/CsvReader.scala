/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io.csv

import com.univocity.parsers.common.processor.ColumnProcessor
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import pd.io.StringDecoding.{StringDecoder, autoDecode}
import pd.io.{ReadAdapter, StringDecoding}
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
  * Reading a DataFrame from CSV format.
  *
  * @param header
  *   If true, reads column names from the header line.
  * @param delimiter
  *   Delimiter character between columns. If unset, inferred from file.
  * @param lineSeparator
  *   Line separator. If unset, inferred from file.
  * @param quote
  *   Quote character. If unset, inferred from file.
  * @param charset
  *   Charset decoding. If unset, uses the JVM default (UTF-8).
  * @param defaultDecoders
  *   Decoders for parsing the correct data type. If unset, uses the standard set.
  * @param columnDecoders
  *   Decoders for parsing the correct data type for specific columns.
  * @param nullEncodingEnabled
  *   If true, interprets the string set as `nullEncoding` as undefined (missing) value.
  * @param nullEncoding
  *   Representation of an undefined (missing) value. The default is an empty string.
  * @param skipEmptyLines
  *   If true, empty lines are not read.
  * @param rowLimit
  *   The maximal number of lines to be read, where -1 (or smaller) is treated as unlimited rows.
  * @param rowsSkipped
  *   The number of initial lines to be skipped, where 0 (or smaller) does not skip any initial lines.
  * @since 0.1.0
  */
class CsvReader(
    header: Boolean = true,
    delimiter: Char = '\u0000',
    lineSeparator: String = "",
    quote: Char = '\u0000',
    charset: Charset = Charset.defaultCharset(),
    defaultDecoders: Array[StringDecoder] = StringDecoding.defaultDecoders,
    columnDecoders: Map[String, StringDecoder] = Map(),
    nullEncodingEnabled: Boolean = true,
    nullEncoding: String = "",
    skipEmptyLines: Boolean = true,
    rowLimit: Long = -1,
    rowsSkipped: Long = -1,
):

  /**
    * Reads a DataFrame from a URL.
    *
    * @param url
    *   URL.
    * @return
    *   The DataFrame.
    * @since 0.1.0
    */
  def read(url: URL): DataFrame =
    Using(BufferedReader(InputStreamReader(url.openStream, charset)))(reader => read(reader)) match
      case Success(df) => df
      case Failure(e)  => throw e

  /**
    * Reads a DataFrame from a file.
    *
    * @param path
    *   Path to file.
    * @return
    *   The DataFrame.
    * @since 0.1.0
    */
  def read(path: String): DataFrame =
    Using(BufferedReader(FileReader(path, charset)))(reader => read(reader)) match
      case Success(df) => df
      case Failure(e)  => throw e

  /**
    * Reads a DataFrame from a Reader instance.
    *
    * @param reader
    *   Reader.
    * @return
    *   The DataFrame.
    * @since 0.1.0
    */
  def read(reader: Reader): DataFrame =
    // define settings
    val settings = CsvReader.defaultSettings
    if delimiter == '\u0000' then settings.setDelimiterDetectionEnabled(true)
    else
      settings.setDelimiterDetectionEnabled(false)
      settings.getFormat.setDelimiter(delimiter)

    if lineSeparator.isEmpty then settings.setLineSeparatorDetectionEnabled(true)
    else
      settings.setLineSeparatorDetectionEnabled(false)
      settings.getFormat.setLineSeparator(lineSeparator)

    if quote == '\u0000' then settings.setQuoteDetectionEnabled(true)
    else
      settings.setQuoteDetectionEnabled(false)
      settings.getFormat.setQuote(quote)

    if header then settings.setHeaderExtractionEnabled(true)
    else settings.setHeaderExtractionEnabled(false)

    settings.setSkipEmptyLines(skipEmptyLines)
    if rowLimit > -1 then settings.setNumberOfRecordsToRead(rowLimit)
    if rowsSkipped > -1 then settings.setNumberOfRowsToSkip(rowsSkipped)

    val processor = ColumnProcessor()
    settings.setProcessor(processor)
    val parser = new CsvParser(settings)
    parser.parse(reader)

    // respect header
    val headers: Array[String] =
      if header then
        processor.getHeaders match
          case null => Array()
          case x    => x
      else Array()

    // type decoding of columns
    val values = processor.getColumnValuesAsList.asScala
    def decode(name: String, ix: Int): Series[Any] =
      val decoders = columnDecoders.get(name) match
        case Some(d: StringDecoder) => Array(d)
        case None                   => defaultDecoders
      val series = Series.from(values(ix).asScala).as(name)
      autoDecode(
        if nullEncodingEnabled then if nullEncoding == "" then series else series(series != nullEncoding).dense
        else series.fill(""),
        decoders,
      )

    // pass content to DataFrame
    if values == null then DataFrame.empty
    else
      val added = mutable.ArrayBuffer[String]()
      val columnCount = values.size
      DataFrame(
        (0 until columnCount)
          .map(ix =>
            if ix < headers.length && headers(ix) != null && headers(ix).nonEmpty && !added.contains(
                headers(ix)
              )
            then
              val colName: String = headers(ix)
              added.append(colName)
              decode(colName, ix)
            else decode(ix.toString, ix)
          )*
      )

  /**
    * Reads a DataFrame from the resource directory.
    *
    * @param path
    *   Path to file (relative to the resource directory).
    * @return
    *   The DataFrame.
    * @since 0.1.0
    */
  def readResource(path: String): DataFrame =
    try
      Using(InputStreamReader(getClass.getResourceAsStream(s"/$path"), charset))(reader => read(reader)) match
        case Success(df) => df
        case Failure(e)  => throw e
    catch case _: NullPointerException => throw FileNotFoundException(s"$path (No such resource)")

/**
  * Reading a DataFrame from CSV format.
  *
  * @since 0.1.0
  */
object CsvReader:
  private def defaultSettings: CsvParserSettings =
    val settings = CsvParserSettings()
    settings.setDelimiterDetectionEnabled(true)
    settings.setQuoteDetectionEnabled(true)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setHeaderExtractionEnabled(true)
    settings

  class Adapter:
    /**
      * Reading a DataFrame from CSV format.
      *
      * @since 0.1.0
      */
    def csv: CsvReader = CsvReader()

    /**
      * Reading a DataFrame from CSV format.
      *
      * @param header
      *   If true, reads column names from the header line.
      * @param delimiter
      *   Delimiter character between columns. If unset, inferred from file.
      * @param lineSeparator
      *   Line separator. If unset, inferred from file.
      * @param quote
      *   Quote character. If unset, inferred from file.
      * @param charset
      *   Charset decoding. If unset, uses the JVM default (UTF-8).
      * @param defaultDecoders
      *   Decoders for parsing the correct data type. If unset, uses the standard set.
      * @param columnDecoders
      *   Decoders for parsing the correct data type for specific columns.
      * @param nullEncodingEnabled
      *   If true, interprets the string set as `nullEncoding` as undefined (missing) value.
      * @param nullEncoding
      *   Representation of an undefined (missing) value. The default is an empty string.
      * @param skipEmptyLines
      *   If true, empty lines are not read.
      * @param rowLimit
      *   The maximal number of lines to be read, where -1 (or smaller) is treated as unlimited rows.
      * @param rowsSkipped
      *   The number of initial lines to be skipped, where 0 (or smaller) does not skip any initial lines.
      * @since 0.1.0
      */
    def csv(
        header: Boolean = true,
        delimiter: Char = '\u0000',
        lineSeparator: String = "",
        quote: Char = '\u0000',
        charset: Charset = Charset.defaultCharset(),
        defaultDecoders: Array[StringDecoder] = StringDecoding.defaultDecoders,
        columnDecoders: Map[String, StringDecoder] = Map(),
        nullEncodingEnabled: Boolean = true,
        nullEncoding: String = "",
        skipEmptyLines: Boolean = true,
        rowLimit: Long = -1,
        rowsSkipped: Long = -1,
    ): CsvReader =
      CsvReader(
        header,
        delimiter,
        lineSeparator,
        quote,
        charset,
        defaultDecoders,
        columnDecoders,
        nullEncodingEnabled,
        nullEncoding,
        skipEmptyLines,
        rowLimit,
        rowsSkipped,
      )
