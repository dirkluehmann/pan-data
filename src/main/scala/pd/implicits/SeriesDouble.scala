/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.Series
import pd.Series.toSeriesDouble
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData

import scala.annotation.targetName
import scala.math

/**
  * Series extension for type Double.
  *
  * @since 0.1.0
  */
class SeriesDouble private[pd] (data: SeriesData[Double], index: BaseIndex, name: String)
    extends Series[Double](data, index, name):

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesDouble): Series[Double] = mapDD(series, _ + _, '+')

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesInt): Series[Double] = mapDI(series, _ + _, '+')

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Double] = mapD(_ + v)

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Double] = mapD(_ + v)

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesDouble): Series[Double] = mapDD(series, _ - _, '-')

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesInt): Series[Double] = mapDI(series, _ - _, '-')

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Double] = mapD(_ - v)

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Double] = mapD(_ - v)

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesDouble): Series[Double] = mapDD(series, _ * _, '*')

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesInt): Series[Double] = mapDI(series, _ * _, '*')

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Double] = mapD(_ * v)

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Double] = mapD(_ * v)

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesDouble): Series[Double] = mapDD(series, _ / _, '/')

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesInt): Series[Double] = mapDI(series, _ / _, '/')

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Double] = mapD(_ / v)

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Double] = mapD(_ / v)

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: SeriesDouble): Series[Boolean] = mapDD2B(series, _ < _)

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: SeriesDouble): Series[Boolean] = mapDD2B(series, _ <= _)

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: SeriesDouble): Series[Boolean] = mapDD2B(series, _ >= _)

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: SeriesDouble): Series[Boolean] = mapDD2B(series, _ > _)

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Double): Series[Boolean] = mapD2B(_ < v)

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Double): Series[Boolean] = mapD2B(_ <= v)

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Double): Series[Boolean] = mapD2B(_ >= v)

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Double): Series[Boolean] = mapD2B(_ > v)

  /** @since 0.1.0 */
  def inRange(lower: Double, upper: Double): Series[Boolean] = mapD2B(x => x >= lower && x <= upper)

  /** @since 0.1.0 */
  def inRangeExclusive(lower: Double, upper: Double): Series[Boolean] = mapD2B(x => x > lower && x < upper)

  /** @since 0.1.0 */
  def inRangeLowerExclusive(lower: Double, upper: Double): Series[Boolean] = mapD2B(x => x > lower && x <= upper)

  /** @since 0.1.0 */
  def inRangeUpperExclusive(lower: Double, upper: Double): Series[Boolean] = mapD2B(x => x >= lower && x < upper)

  /** @since 0.1.0 */
  def clip(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity): Series[Double] =
    if lower > upper then throw new IllegalArgumentException("Lower boundary is larger than upper boundary.")
    mapD(x => if x < lower then lower else if x > upper then upper else x)

  /** @since 0.1.0 */
  def rint: Series[Double] = mapD(math.rint)

  /** @since 0.1.0 */
  def round: Series[Int] = mapD2I(_.round.toInt)

  // *** AGGREGATION ***

  /** @since 0.1.0 */
  def countNotNaN: Int = ops.aggD2I(this, 0, (x, y) => x + 1)

  /** @since 0.1.0 */
  def max: Double = ops.aggD(this, Double.NegativeInfinity, math.max)

  /** @since 0.1.0 */
  def mad: Double =
    val meanValue = mean
    ops.aggD(this, 0.0, (value, x) => value + math.abs(x - meanValue)) / count

  /** @since 0.1.0 */
  def mean: Double = sum / countNotNaN

  /** @since 0.1.0 */
  def min: Double = ops.aggD(this, Double.PositiveInfinity, math.min)

  /** @since 0.1.0 */
  def sum: Double = ops.aggD(this, 0.0, _ + _)

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private[pd] inline def mapD(f: Double => Double): Series[Double] = ops.mapD(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapD2B(f: Double => Boolean): Series[Boolean] = ops.mapD2B(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapD2I(f: Double => Int): Series[Int] = ops.mapD2I(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapDD(series: Series[Double], f: (Double, Double) => Double, op: Char): Series[Double] =
    Series.ext(ops.mapDD(this, series, f), op, series)

  /** @since 0.1.0 */
  private[pd] inline def mapDD2B(series: Series[Double], f: (Double, Double) => Boolean): Series[Boolean] =
    ops.mapDD2B(this, series, f)

  /** @since 0.1.0 */
  private[pd] inline def mapDI(series: Series[Int], f: (Double, Int) => Double, op: Char): Series[Double] =
    Series.ext(ops.mapDI(this, series, f), op, series)

/**
  * Series extension for type Double.
  *
  * @since 0.1.0
  */
private[pd] object SeriesDouble:

  class Prefix(v: Double):

    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Double]): Series[Double] = series.mapD(v + _)

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Double]): Series[Double] = series.mapD(v - _)

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Double]): Series[Double] = series.mapD(v * _)

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Double]): Series[Double] = series.mapD(v / _)

  class PrefixInt(v: Int):
    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Double]): Series[Double] = series.mapD(v + _)

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Double]): Series[Double] = series.mapD(v - _)

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Double]): Series[Double] = series.mapD(v * _)

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Double]): Series[Double] = series.mapD(v / _)
