/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.exception.{IllegalOperation, SeriesCastException}
import pd.implicits.SeriesAny
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData
import pd.{MutableSeries, Order, Series}

import java.time.*
import scala.annotation.targetName

/**
  * MutableSeries extension for type Any.
  *
  * @param instance
  *   Private mutable instance.
  * @since 0.1.0
  */
class MutableSeriesAny private[pd] (instance: MutableSeries[Any]):

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(series: Series[?]): Unit = inner = inner + series

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(v: Double): Unit = inner = inner + v

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(v: Int): Unit = inner = inner + v

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(series: Series[?]): Unit = inner = inner - series

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(v: Double): Unit = inner = inner - v

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(v: Int): Unit = inner = inner - v

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(series: Series[?]): Unit = inner = inner * series

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(v: Double): Unit = inner = inner * v

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(v: Int): Unit = inner = inner * v

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(series: Series[?]): Unit = inner = inner / series

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(v: Double): Unit = inner = inner / v

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(v: Int): Unit = inner = inner / v

  /** @since 0.1.0 */
  def sortValues: Unit = inner = inner.sortValues

  /** @since 0.1.0 */
  def sortValues(order: Order): Unit = inner = inner.sortValues(order)

  // *** NON-MUTATING METHODS ***

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: Series[?]): Series[Any] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Any] = inner + v

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Any] = inner + v

  /** @since 0.1.0 */
  @targetName("plus")
  def +(s: String): Series[String] = inner + s

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: Series[?]): Series[Any] = inner - series

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Any] = inner - v

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Any] = inner - v

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: Series[?]): Series[Any] = inner * series

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Any] = inner * v

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Any] = inner * v

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: Series[?]): Series[Any] = inner / series

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Any] = inner / v

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Any] = inner / v

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: Series[?]): Series[Boolean] = inner < series

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: Series[?]): Series[Boolean] = inner <= series

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: Series[?]): Series[Boolean] = inner >= series

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: Series[?]): Series[Boolean] = inner > series

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Double): Series[Boolean] = inner < v

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Double): Series[Boolean] = inner <= v

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Double): Series[Boolean] = inner >= v

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Double): Series[Boolean] = inner > v

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Int): Series[Boolean] = inner < v

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Int): Series[Boolean] = inner <= v

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Int): Series[Boolean] = inner >= v

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Int): Series[Boolean] = inner > v

  /** @since 0.1.0 */
  def asBoolean: MutableSeries[Boolean] = inner.asBoolean.mutable

  /** @since 0.1.0 */
  def asDouble: MutableSeries[Double] = inner.asDouble.mutable

  /** @since 0.1.0 */
  def asInt: MutableSeries[Int] = inner.asInt.mutable

  /** @since 0.1.0 */
  def asString: MutableSeries[String] = inner.asString.mutable

  /** @since 0.1.0 */
  @targetName("sum")
  def sum: Double = inner.sum

  /** @since 0.1.0 */
  def toBoolean: MutableSeries[Boolean] = inner.toBoolean.mutable

  /** @since 0.1.0 */
  def toDouble: MutableSeries[Double] = inner.toDouble.mutable

  /** @since 0.1.0 */
  def toInt: MutableSeries[Int] = inner.toInt.mutable

  /**
    * @return
    *   Parsed LocalDate Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDate]]
    * @since 0.1.0
    */
  def toLocalDate: MutableSeries[LocalDate] = inner.toLocalDate.mutable

  /**
    * @return
    *   Parsed LocalDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDateTime]]
    * @since 0.1.0
    */
  def toLocalDateTime: MutableSeries[LocalDateTime] = inner.toLocalDateTime.mutable

  /**
    * @return
    *   Parsed LocalTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalTime]]
    * @since 0.1.0
    */
  def toLocalTime: MutableSeries[LocalTime] = inner.toLocalTime.mutable

  /**
    * @return
    *   Parsed ZonedDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.ZonedDateTime]]
    * @since 0.1.0
    */
  def toZonedDateTime: MutableSeries[ZonedDateTime] = inner.toZonedDateTime.mutable

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private def inner: Series[Any] = instance.inner

  /** @since 0.1.0 */
  private def inner_=(series: Series[Any]): Unit = instance.inner = series
