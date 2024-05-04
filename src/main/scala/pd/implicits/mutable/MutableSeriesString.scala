/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.{MutableSeries, Series}
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData

import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import scala.annotation.targetName
import scala.reflect.ClassTag

/**
  * MutableSeries extension for type String.
  *
  * @param instance
  *   Private mutable instance.
  * @since 0.1.0
  */
class MutableSeriesString private[pd] (instance: MutableSeries[String]):

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(series: Series[String]): Unit = inner = inner + series

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(s: String): Unit = inner = inner + s

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=[T: ClassTag](series: Series[T]): Unit = inner = inner + series

  /**
    * @see
    *   [[String.toLowerCase]]
    * @since 0.1.0
    */
  def lower: Unit = inner = inner.lower

  /**
    * @see
    *   [[String.replace]]
    * @since 0.1.0
    */
  def replace(target: String, replacement: String): Unit = inner = inner.replace(target, replacement)

  /**
    * @see
    *   [[String.replaceAll]]
    * @since 0.1.0
    */
  def replaceAll(regex: String, replacement: String): Unit = inner = inner.replaceAll(regex, replacement)

  /**
    * @see
    *   [[String.replaceFirst]]
    * @since 0.1.0
    */
  def replaceFirst(regex: String, replacement: String): Unit = inner = inner.replaceFirst(regex, replacement)

  /**
    * @return
    *   Sub string or empty string if out of bounds.
    * @see
    *   [[String.substring]]
    * @since 0.1.0
    */
  def substring(beginIndex: Int): Unit = inner = inner.substring(beginIndex)

  /**
    * @return
    *   Sub string or empty string if out of bounds.
    * @see
    *   [[String.substring]]
    * @since 0.1.0
    */
  def substring(beginIndex: Int, endIndex: Int): Unit = inner = inner.substring(beginIndex, endIndex)

  /**
    * @see
    *   [[String.trim]]
    * @since 0.1.0
    */
  def trimString: Unit = inner = inner.trimString

  /**
    * @see
    *   [[String.toUpperCase]]
    * @since 0.1.0
    */
  def upper: Unit = inner = inner.upper

  // *** NON-MUTATING METHODS ***

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: Series[String]): Series[String] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(s: String): Series[String] = inner + s

  /** @since 0.1.0 */
  @targetName("plus")
  def +[T: ClassTag](series: Series[T]): Series[String] = inner + series

  /**
    * @see
    *   [[String.contains]]
    * @since 0.1.0
    */
  def contains(s: String): Series[Boolean] = inner.contains(s)

  /**
    * @see
    *   [[String.compareToIgnoreCase]]
    * @since 0.1.0
    */
  def compareIgnoreCase(s: String): Series[Boolean] = inner.compareIgnoreCase(s)

  /**
    * @see
    *   [[String.compareToIgnoreCase]]
    * @since 0.1.0
    */
  def compareIgnoreCase(series: Series[String]): Series[Boolean] = inner.compareIgnoreCase(series)

  /**
    * @see
    *   [[String.endsWith]]
    * @since 0.1.0
    */
  def endsWith(s: String): Series[Boolean] = inner.endsWith(s)

  /**
    * @see
    *   [[String.hashCode]]
    * @since 0.1.0
    */
  def hash: Series[Int] = inner.hash

  /**
    * @see
    *   [[String.indexOf]]
    * @since 0.1.0
    */
  def indexOf(s: String): Series[Int] = inner.indexOf(s)

  /**
    * @return
    *   True, if String is empty.
    * @since 0.1.0
    */
  def isEmptyString: Series[Boolean] = inner.isEmptyString

  /**
    * @see
    *   [[String.lastIndexOf]]
    * @since 0.1.0
    */
  def lastIndexOf(s: String): Series[Int] = inner.lastIndexOf(s)

  /**
    * @see
    *   [[String.length]]
    * @since 0.1.0
    */
  def lengths: Series[Int] = inner.lengths

  /**
    * @see
    *   [[String.matches]]
    * @since 0.1.0
    */
  def matches(regex: String): Series[Boolean] = inner.matches(regex)

  /**
    * @return
    *   True, if String is not empty.
    * @since 0.1.0
    */
  def nonEmptyString: Series[Boolean] = inner.nonEmptyString

  /**
    * @see
    *   [[String.split]]
    * @since 0.1.0
    */
  def split(regex: String, limit: Int = 0): Series[Array[String]] = inner.split(regex, limit)

  /**
    * @see
    *   [[String.startsWith]]
    * @since 0.1.0
    */
  def startsWith(s: String, beginIndex: Int = 0): Series[Boolean] = inner.startsWith(s, beginIndex)

  /**
    * @return
    *   Parsed Boolean Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Boolean.
    * @since 0.1.0
    */
  def toBoolean: Series[Boolean] = inner.toBoolean

  /**
    * @return
    *   Parsed Double Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Double.
    * @since 0.1.0
    */
  def toDouble: Series[Double] = inner.toDouble

  /**
    * @return
    *   Parsed Float Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Float.
    * @since 0.1.0
    */
  def toFloat: Series[Float] = inner.toFloat

  /**
    * @return
    *   Parsed Int Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Int.
    * @since 0.1.0
    */
  def toInt: Series[Int] = inner.toInt

  /**
    * @return
    *   Parsed Long Series.
    * @throws java.lang.NumberFormatException
    *   If the string does not contain a parsable Long.
    * @since 0.1.0
    */
  def toLong: Series[Long] = inner.toLong

  /**
    * @return
    *   Parsed LocalDate Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDate]]
    * @since 0.1.0
    */
  def toLocalDate: Series[LocalDate] = inner.toLocalDate

  /**
    * @return
    *   Parsed LocalDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDateTime]]
    * @since 0.1.0
    */
  def toLocalDateTime: Series[LocalDateTime] = inner.toLocalDateTime

  /**
    * @return
    *   Parsed LocalTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalTime]]
    * @since 0.1.0
    */
  def toLocalTime: Series[LocalTime] = inner.toLocalTime

  /**
    * @return
    *   Parsed ZonedDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.ZonedDateTime]]
    * @since 0.1.0
    */
  def toZonedDateTime: Series[ZonedDateTime] = inner.toZonedDateTime

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private def inner: Series[String] = instance.inner

  /** @since 0.1.0 */
  private def inner_=(series: Series[String]): Unit = instance.inner = series
