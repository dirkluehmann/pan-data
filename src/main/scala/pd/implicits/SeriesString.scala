/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.Series
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData

import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import scala.annotation.targetName
import scala.reflect.ClassTag

/**
  * Series extension for type String.
  *
  * @since 0.1.0
  */
class SeriesString private[pd] (data: SeriesData[String], index: BaseIndex, name: String)
    extends Series[String](data, index, name):

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: Series[String]): Series[String] = map(series, _ + _)

  /** @since 0.1.0 */
  @targetName("plus")
  def +(s: String): Series[String] = map(_ + s)

  /** @since 0.1.0 */
  @targetName("plus")
  def +[T: ClassTag](series: Series[T]): Series[String] = map(series, _ + _.toString)

  /**
    * @see
    *   [[String.contains]]
    * @since 0.1.0
    */
  def contains(s: String): Series[Boolean] = map(_.contains(s))

  /**
    * @see
    *   [[String.compareToIgnoreCase]]
    * @since 0.1.0
    */
  def compareIgnoreCase(s: String): Series[Boolean] = map(_.compareToIgnoreCase(s) == 0)

  /**
    * @see
    *   [[String.compareToIgnoreCase]]
    * @since 0.1.0
    */
  def compareIgnoreCase(series: Series[String]): Series[Boolean] = map(series, _.compareToIgnoreCase(_) == 0)

  /**
    * @see
    *   [[String.endsWith]]
    * @since 0.1.0
    */
  def endsWith(s: String): Series[Boolean] = map(_.endsWith(s))

  /**
    * @see
    *   [[String.toLowerCase]]
    * @since 0.1.0
    */
  def lower: Series[String] = map(_.toLowerCase)

  /**
    * @see
    *   [[String.hashCode]]
    * @since 0.1.0
    */
  def hash: Series[Int] = map(_.hashCode)

  /**
    * @see
    *   [[String.indexOf]]
    * @since 0.1.0
    */
  def indexOf(s: String): Series[Int] = map(_.indexOf(s))

  /**
    * @return
    *   True, if String is empty.
    * @since 0.1.0
    */
  def isEmptyString: Series[Boolean] = map(_.isEmpty)

  /**
    * @see
    *   [[String.lastIndexOf]]
    * @since 0.1.0
    */
  def lastIndexOf(s: String): Series[Int] = map(_.lastIndexOf(s))

  /**
    * @see
    *   [[String.length]]
    * @since 0.1.0
    */
  def lengths: Series[Int] = map(_.length)

  /**
    * @see
    *   [[String.matches]]
    * @since 0.1.0
    */
  def matches(regex: String): Series[Boolean] = map(_.matches(regex))

  /**
    * @return
    *   True, if String is not empty.
    * @since 0.1.0
    */
  def nonEmptyString: Series[Boolean] = map(!_.isEmpty)

  /**
    * @see
    *   [[String.replace]]
    * @since 0.1.0
    */
  def replace(target: String, replacement: String): Series[String] =
    map(_.replace(target, replacement))

  /**
    * @see
    *   [[String.replaceAll]]
    * @since 0.1.0
    */
  def replaceAll(regex: String, replacement: String): Series[String] =
    map(_.replaceAll(regex, replacement))

  /**
    * @see
    *   [[String.replaceFirst]]
    * @since 0.1.0
    */
  def replaceFirst(regex: String, replacement: String): Series[String] =
    map(_.replaceFirst(regex, replacement))

  /**
    * @see
    *   [[String.split]]
    * @since 0.1.0
    */
  def split(regex: String, limit: Int = 0): Series[Array[String]] = map(_.split(regex, limit))

  /**
    * @see
    *   [[String.startsWith]]
    * @since 0.1.0
    */
  def startsWith(s: String, beginIndex: Int = 0): Series[Boolean] = map(_.startsWith(s, beginIndex))

  /**
    * @return
    *   Sub string or empty string if out of bounds.
    * @see
    *   [[String.substring]]
    * @since 0.1.0
    */
  def substring(beginIndex: Int): Series[String] =
    map(s => if beginIndex >= s.length then "" else s.substring(beginIndex max 0))

  /**
    * @return
    *   Sub string or empty string if out of bounds.
    * @see
    *   [[String.substring]]
    * @since 0.1.0
    */
  def substring(beginIndex: Int, endIndex: Int): Series[String] = map(s =>
    val len = s.length
    if beginIndex >= len then "" else s.substring(beginIndex max 0, endIndex min len)
  )

  /**
    * @return
    *   Parsed Boolean Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Boolean.
    * @since 0.1.0
    */
  def toBoolean: Series[Boolean] = map(_.toBoolean)

  /**
    * @return
    *   Parsed Double Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Double.
    * @since 0.1.0
    */
  def toDouble: Series[Double] = map(_.toDouble)

  /**
    * @return
    *   Parsed Float Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Float.
    * @since 0.1.0
    */
  def toFloat: Series[Float] = map(_.toFloat)

  /**
    * @return
    *   Parsed Int Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Int.
    * @since 0.1.0
    */
  def toInt: Series[Int] = map(_.toInt)

  /**
    * @return
    *   Parsed Long Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Long.
    * @since 0.1.0
    */
  def toLong: Series[Long] = map(_.toLong)

  /**
    * @return
    *   Parsed LocalDate Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDate]]
    * @since 0.1.0
    */
  def toLocalDate: Series[LocalDate] = map(LocalDate.parse)

  /**
    * @return
    *   Parsed LocalDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDateTime]]
    * @since 0.1.0
    */
  def toLocalDateTime: Series[LocalDateTime] = map(LocalDateTime.parse)

  /**
    * @return
    *   Parsed LocalTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalTime]]
    * @since 0.1.0
    */
  def toLocalTime: Series[LocalTime] = map(LocalTime.parse)

  /**
    * @return
    *   Parsed ZonedDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.ZonedDateTime]]
    * @since 0.1.0
    */
  def toZonedDateTime: Series[ZonedDateTime] = map(ZonedDateTime.parse)

  /**
    * @see
    *   [[String.trim]]
    * @since 0.1.0
    */
  def trimString: Series[String] = map(_.trim)

  /**
    * @see
    *   [[String.toUpperCase]]
    * @since 0.1.0
    */
  def upper: Series[String] = map(_.toUpperCase)

/**
  * Series extension for type String.
  *
  * @since 0.1.0
  */
object SeriesString:

  private[pd] object DefaultOrder extends Ordering[String]:

    /** @since 0.1.0 */
    def compare(x: String, y: String) = x.compareToIgnoreCase(y)
