/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.exception.*
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData
import pd.{Order, Series}

import java.time.*
import scala.annotation.targetName

/**
  * Series extension for type Any.
  *
  * @since 0.1.0
  */
class SeriesAny private[pd] (data: SeriesData[Any], index: BaseIndex, name: String = "")
    extends Series[Any](data, index, name):

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: Series[?]): Series[Any] =
    if isDouble then
      if series.isDouble then toAny(toD + asD(series))
      else if series.isInt then toAny(toD + asI(series))
      else throw IllegalOperation(this, series, "+")
    else if isInt then
      if series.isDouble then toAny(toI + asD(series))
      else if series.isInt then toAny(toI + asI(series))
      else throw IllegalOperation(this, series, "+")
    else if isString then toAny(toS + series.str)
    else throw IllegalOperation(this, series, "+")

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Any] =
    if isDouble then toAny(toD + v)
    else if isInt then toAny(toI + v)
    else throw IllegalOperation(this, "+")

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Any] =
    if isDouble then toAny(toD + v)
    else if isInt then toAny(toI + v)
    else throw IllegalOperation(this, "+")

  /** @since 0.1.0 */
  @targetName("plus")
  def +(s: String): Series[String] = str + s

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: Series[?]): Series[Any] =
    if isDouble then
      if series.isDouble then toAny(toD - asD(series))
      else if series.isInt then toAny(toD - asI(series))
      else throw IllegalOperation(this, series, "-")
    else if isInt then
      if series.isDouble then toAny(toI - asD(series))
      else if series.isInt then toAny(toI - asI(series))
      else throw IllegalOperation(this, series, "-")
    else throw IllegalOperation(this, series, "-")

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Any] =
    if isDouble then toAny(toD - v)
    else if isInt then toAny(toI - v)
    else throw IllegalOperation(this, "-")

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Any] =
    if isDouble then toAny(toD - v)
    else if isInt then toAny(toI - v)
    else throw IllegalOperation(this, "-")

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: Series[?]): Series[Any] =
    if isDouble then
      if series.isDouble then toAny(toD * asD(series))
      else if series.isInt then toAny(toD * asI(series))
      else throw IllegalOperation(this, series, "*")
    else if isInt then
      if series.isDouble then toAny(toI * asD(series))
      else if series.isInt then toAny(toI * asI(series))
      else throw IllegalOperation(this, series, "*")
    else throw IllegalOperation(this, series, "*")

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Any] =
    if isDouble then toAny(toD * v)
    else if isInt then toAny(toI * v)
    else throw IllegalOperation(this, "*")

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Any] =
    if isDouble then toAny(toD * v)
    else if isInt then toAny(toI * v)
    else throw IllegalOperation(this, "*")

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: Series[?]): Series[Any] =
    if isDouble then
      if series.isDouble then toAny(toD / asD(series))
      else if series.isInt then toAny(toD / asI(series))
      else throw IllegalOperation(this, series, "/")
    else if isInt then
      if series.isDouble then toAny(toI / asD(series))
      else if series.isInt then toAny(toI / asI(series))
      else throw IllegalOperation(this, series, "/")
    else throw IllegalOperation(this, series, "/")

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Any] =
    if isDouble then toAny(toD / v)
    else if isInt then toAny(toI / v)
    else throw IllegalOperation(this, "/")

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Any] =
    if isDouble then toAny(toD / v)
    else if isInt then toAny(toI / v)
    else throw IllegalOperation(this, "/")

  // ***********

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: Series[?]): Series[Boolean] =
    if isDouble then
      if series.isDouble then toD < asD(series)
      else throw IllegalOperation(this, series, "<")
    else if isInt then
      if series.isInt then toI < asI(series)
      else throw IllegalOperation(this, series, "<")
    else throw IllegalOperation(this, series, "<")

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: Series[?]): Series[Boolean] =
    if isDouble then
      if series.isDouble then toD <= asD(series)
      else throw IllegalOperation(this, series, "<=")
    else if isInt then
      if series.isInt then toI <= asI(series)
      else throw IllegalOperation(this, series, "<=")
    else throw IllegalOperation(this, series, "<=")

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: Series[?]): Series[Boolean] =
    if isDouble then
      if series.isDouble then toD >= asD(series)
      else throw IllegalOperation(this, series, ">=")
    else if isInt then
      if series.isInt then toI >= asI(series)
      else throw IllegalOperation(this, series, ">=")
    else throw IllegalOperation(this, series, ">=")

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: Series[?]): Series[Boolean] =
    if isDouble then
      if series.isDouble then toD > asD(series)
      else throw IllegalOperation(this, series, ">")
    else if isInt then
      if series.isInt then toI > asI(series)
      else throw IllegalOperation(this, series, ">")
    else throw IllegalOperation(this, series, ">")

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Double): Series[Boolean] =
    if isDouble then toD < v
    else throw IllegalOperation(this, "<")

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Double): Series[Boolean] =
    if isDouble then toD <= v
    else throw IllegalOperation(this, "<=")

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Double): Series[Boolean] =
    if isDouble then toD >= v
    else throw IllegalOperation(this, ">=")

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Double): Series[Boolean] =
    if isDouble then toD > v
    else throw IllegalOperation(this, ">")

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Int): Series[Boolean] =
    if isInt then toI < v
    else throw IllegalOperation(this, "<")

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Int): Series[Boolean] =
    if isInt then toI <= v
    else throw IllegalOperation(this, "<=")

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Int): Series[Boolean] =
    if isInt then toI >= v
    else throw IllegalOperation(this, ">=")

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Int): Series[Boolean] =
    if isInt then toI > v
    else throw IllegalOperation(this, ">")

  /** @since 0.1.0 */
  def asBoolean: Series[Boolean] =
    if isBoolean then toB else throw SeriesCastException(this, "Boolean")

  /** @since 0.1.0 */
  def asDouble: Series[Double] =
    if isDouble then toD else throw SeriesCastException(this, "Double")

  /** @since 0.1.0 */
  def asInt: Series[Int] =
    if isInt then toI else throw SeriesCastException(this, "Int")

  /** @since 0.1.0 */
  def asString: Series[String] =
    if isString then toS else throw SeriesCastException(this, "String")

  /** @since 0.1.0 */
  def sortValues: Series[Any] = sortValues(Order.asc)

  /** @since 0.1.0 */
  def sortValues(order: Order): Series[Any] =
    if isBoolean then SeriesAny.toAny(toB.sorted(order))
    else if isDouble then SeriesAny.toAny(toD.sorted(order))
    else if isInt then SeriesAny.toAny(toI.sorted(order))
    else if isString then SeriesAny.toAny(toS.sorted(order))
    else throw IllegalOperation(this, "sortValues")

  /** @since 0.1.0 */
  @targetName("sum")
  def sum: Any =
    if isDouble then toD.sum
    else if isInt then toI.sum
    else throw IllegalOperation(this, "sum")

  /** @since 0.1.0 */
  def toBoolean: Series[Boolean] =
    if isBoolean then toB
    else if isString then toS.toBoolean
    else throw SeriesCastException(this, "Boolean")

  /** @since 0.1.0 */
  def toDouble: Series[Double] =
    if isDouble then toD
    else if isInt then toI.toDouble
    else if isString then toS.toDouble
    else if isBoolean then toB.toDouble
    else throw SeriesCastException(this, "Double")

  /** @since 0.1.0 */
  def toInt: Series[Int] =
    if isInt then toI
    else if isString then toS.toInt
    else if isBoolean then toB.toInt
    else throw SeriesCastException(this, "Int")

  /**
    * @return
    *   Parsed LocalDate Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDate]]
    * @since 0.1.0
    */
  def toLocalDate: Series[LocalDate] =
    if typeMatch(LocalDate.MIN) then asInstanceOf[Series[LocalDate]]
    else map(v => LocalDate.parse(v.toString))

  /**
    * @return
    *   Parsed LocalDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalDateTime]]
    * @since 0.1.0
    */
  def toLocalDateTime: Series[LocalDateTime] =
    if typeMatch(LocalDateTime.MIN) then asInstanceOf[Series[LocalDateTime]]
    else map(v => LocalDateTime.parse(v.toString))

  /**
    * @return
    *   Parsed LocalTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.LocalTime]]
    * @since 0.1.0
    */
  def toLocalTime: Series[LocalTime] =
    if typeMatch(LocalTime.MIN) then asInstanceOf[Series[LocalTime]]
    else map(v => LocalTime.parse(v.toString))

  /**
    * @return
    *   Parsed ZonedDateTime Series.
    * @throws java.time.format.DateTimeParseException
    *   If the string cannot be parsed.
    * @see
    *   [[java.time.ZonedDateTime]]
    * @since 0.1.0
    */
  def toZonedDateTime: Series[ZonedDateTime] =
    if typeMatch(LocalDateTime.MIN.atOffset(ZoneOffset.UTC).toZonedDateTime) then asInstanceOf[Series[ZonedDateTime]]
    else map(v => ZonedDateTime.parse(v.toString))

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private[pd] inline def asB(s: Series[?]): Series[Boolean] = s.asInstanceOf[Series[Boolean]]

  /** @since 0.1.0 */
  private[pd] inline def asD(s: Series[?]): Series[Double] = s.asInstanceOf[Series[Double]]

  /** @since 0.1.0 */
  private[pd] inline def asI(s: Series[?]): Series[Int] = s.asInstanceOf[Series[Int]]

  /** @since 0.1.0 */
  private[pd] inline def asS(s: Series[?]): Series[String] = s.asInstanceOf[Series[String]]

  /** @since 0.1.0 */
  private[pd] inline def toAny(series: Series[?]): Series[Any] = series.asInstanceOf[Series[Any]]

  /** @since 0.1.0 */
  private[pd] inline def toB: Series[Boolean] = asInstanceOf[Series[Boolean]]

  /** @since 0.1.0 */
  private[pd] inline def toD: Series[Double] = asInstanceOf[Series[Double]]

  /** @since 0.1.0 */
  private[pd] inline def toI: Series[Int] = asInstanceOf[Series[Int]]

  /** @since 0.1.0 */
  private[pd] inline def toS: Series[String] = asInstanceOf[Series[String]]

/**
  * Series extension for type Any.
  *
  * @since 0.1.0
  */
private[pd] object SeriesAny:

  class Prefix(v: Double):

    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v + toD(series))
      else if series.isInt then toAny(v + toI(series))
      else throw IllegalOperation(series, "+")

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v - toD(series))
      else if series.isInt then toAny(v - toI(series))
      else throw IllegalOperation(series, "-")

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v * toD(series))
      else if series.isInt then toAny(v * toI(series))
      else throw IllegalOperation(series, "*")

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v / toD(series))
      else if series.isInt then toAny(v / toI(series))
      else throw IllegalOperation(series, "/")

  class PrefixInt(v: Int):

    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v + toD(series))
      else if series.isInt then toAny(v + toI(series))
      else throw IllegalOperation(series, "+")

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v - toD(series))
      else if series.isInt then toAny(v - toI(series))
      else throw IllegalOperation(series, "-")

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v * toD(series))
      else if series.isInt then toAny(v * toI(series))
      else throw IllegalOperation(series, "*")

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Any]): Series[Any] =
      if series.isDouble then toAny(v / toD(series))
      else if series.isInt then toAny(v / toI(series))
      else throw IllegalOperation(series, "/")

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private[pd] inline def toAny(series: Series[?]): Series[Any] = series.asInstanceOf[Series[Any]]

  /** @since 0.1.0 */
  private[pd] inline def toB(series: Series[?]): Series[Boolean] = series.asInstanceOf[Series[Boolean]]

  /** @since 0.1.0 */
  private[pd] inline def toD(series: Series[?]): Series[Double] = series.asInstanceOf[Series[Double]]

  /** @since 0.1.0 */
  private[pd] inline def toI(series: Series[?]): Series[Int] = series.asInstanceOf[Series[Int]]
