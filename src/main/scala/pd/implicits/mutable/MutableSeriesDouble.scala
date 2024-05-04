/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.{MutableSeries, Series}
import pd.implicits.SeriesDouble
import pd.implicits.SeriesInt

import scala.annotation.targetName

/**
  * MutableSeries extension for type Double.
  *
  * @param instance
  *   Private mutable instance.
  * @since 0.1.0
  */
class MutableSeriesDouble private[pd] (instance: MutableSeries[Double]):

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(series: SeriesDouble): Unit = inner = inner + series

  /** @since 0.1.0 */
  @targetName("plusAssign")
  def +=(v: Double): Unit = inner = inner + v

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(series: SeriesDouble): Unit = inner = inner - series

  /** @since 0.1.0 */
  @targetName("minusAssign")
  def -=(v: Double): Unit = inner = inner - v

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(series: SeriesDouble): Unit = inner = inner * series

  /** @since 0.1.0 */
  @targetName("timesAssign")
  def *=(v: Double): Unit = inner = inner * v

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(series: SeriesDouble): Unit = inner = inner / series

  /** @since 0.1.0 */
  @targetName("divAssign")
  def /=(v: Double): Unit = inner = inner / v

  /** @since 0.1.0 */
  def clip(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity): Unit =
    inner = inner.clip(lower, upper)

  /** @since 0.1.0 */
  def rint: Unit = inner = inner.rint

  // *** NON-MUTATING METHODS ***

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesDouble): Series[Double] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesInt): Series[Double] = inner + series

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Double] = inner + v

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Double] = inner + v

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesDouble): Series[Double] = inner - series

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesInt): Series[Double] = inner - series

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Double] = inner - v

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Double] = inner - v

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesDouble): Series[Double] = inner * series

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesInt): Series[Double] = inner * series

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Double] = inner * v

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Double] = inner * v

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesDouble): Series[Double] = inner / series

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesInt): Series[Double] = inner / series

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Double] = inner / v

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Double] = inner / v

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: SeriesDouble): Series[Boolean] = inner < series

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: SeriesDouble): Series[Boolean] = inner <= series

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: SeriesDouble): Series[Boolean] = inner >= series

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: SeriesDouble): Series[Boolean] = inner > series

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
  def inRange(lower: Double, upper: Double): Series[Boolean] = inner.inRange(lower, upper)

  /** @since 0.1.0 */
  def inRangeExclusive(lower: Double, upper: Double): Series[Boolean] = inner.inRangeExclusive(lower, upper)

  /** @since 0.1.0 */
  def inRangeLowerExclusive(lower: Double, upper: Double): Series[Boolean] = inner.inRangeLowerExclusive(lower, upper)

  /** @since 0.1.0 */
  def inRangeUpperExclusive(lower: Double, upper: Double): Series[Boolean] = inner.inRangeUpperExclusive(lower, upper)

  /** @since 0.1.0 */
  def round: Series[Int] = inner.round

  // *** AGGREGATION ***

  /** @since 0.1.0 */
  def countNotNaN: Int = inner.countNotNaN

  /** @since 0.1.0 */
  def max: Double = inner.max

  /** @since 0.1.0 */
  def mad: Double = inner.mad

  /** @since 0.1.0 */
  def mean: Double = inner.mean

  /** @since 0.1.0 */
  def min: Double = inner.min

  /** @since 0.1.0 */
  def sum: Double = inner.sum

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private def inner: Series[Double] = instance.inner

  /** @since 0.1.0 */
  private def inner_=(series: Series[Double]): Unit = instance.inner = series
