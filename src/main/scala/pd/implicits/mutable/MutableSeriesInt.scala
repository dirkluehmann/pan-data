/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.implicits.{SeriesDouble, SeriesInt}
import pd.{MutableSeries, Series}

import scala.annotation.targetName

/**
  * MutableSeries extension for type Int.
  *
  * @param instance
  *   Private mutable instance.
  * @since 0.1.0
  */
class MutableSeriesInt private[pd] (instance: MutableSeries[Int]):

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(series: SeriesInt): Unit = inner = inner + series

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(v: Int): Unit = inner = inner + v

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(series: SeriesInt): Unit = inner = inner - series

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(v: Int): Unit = inner = inner - v

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(series: SeriesInt): Unit = inner = inner * series

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(v: Int): Unit = inner = inner * v

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(series: SeriesInt): Unit = inner = inner / series

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(v: Int): Unit = inner = inner / v

  // *** NON-MUTATING METHODS ***

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesInt): Series[Int] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesDouble): Series[Double] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Int] = inner + v

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Double] = inner + v

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesInt): Series[Int] = inner - series

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesDouble): Series[Double] = inner - series

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Int] = inner - v

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Double] = inner - v

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesInt): Series[Int] = inner * series

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesDouble): Series[Double] = inner * series

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Int] = inner * v

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Double] = inner * v

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesInt): Series[Int] = inner / series

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesDouble): Series[Double] = inner / series

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Int] = inner / v

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Double] = inner / v

  /** @since 0.1.0 */
  @targetName("rem")
  def %(v: Int): Series[Int] = inner % v
  def mod(v: Int): Series[Int] = inner % v

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: SeriesInt): Series[Boolean] = inner < series

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: SeriesInt): Series[Boolean] = inner <= series

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: SeriesInt): Series[Boolean] = inner >= series

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: SeriesInt): Series[Boolean] = inner > series

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
  def toDouble: Series[Double] = inner.toDouble

  // *** AGGREGATION ***

  /** @since 0.1.0 */
  def sum: Int = inner.sum

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private def inner: Series[Int] = instance.inner

  /** @since 0.1.0 */
  private def inner_=(series: Series[Int]): Unit = instance.inner = series
