/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.Series
import pd.Series.toSeriesInt
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData

import scala.annotation.targetName

/**
  * Series extension for type Int.
  *
  * @since 0.1.0
  */
class SeriesInt private[pd] (data: SeriesData[Int], index: BaseIndex, name: String)
    extends Series[Int](data, index, name):

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesInt): Series[Int] = mapII(series, _ + _, '+')

  /** @since 0.1.0 */
  @targetName("plus")
  def +(series: SeriesDouble): Series[Double] = mapID(series, _ + _, '+')

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Int): Series[Int] = mapI(_ + v)

  /** @since 0.1.0 */
  @targetName("plus")
  def +(v: Double): Series[Double] = mapI2D(_ + v)

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesInt): Series[Int] = mapII(series, _ - _, '-')

  /** @since 0.1.0 */
  @targetName("minus")
  def -(series: SeriesDouble): Series[Double] = mapID(series, _ - _, '-')

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Int): Series[Int] = mapI(_ - v)

  /** @since 0.1.0 */
  @targetName("minus")
  def -(v: Double): Series[Double] = mapI2D(_ - v)

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesInt): Series[Int] = mapII(series, _ * _, '*')

  /** @since 0.1.0 */
  @targetName("times")
  def *(series: SeriesDouble): Series[Double] = mapID(series, _ * _, '*')

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Int): Series[Int] = mapI(_ * v)

  /** @since 0.1.0 */
  @targetName("times")
  def *(v: Double): Series[Double] = mapI2D(_ * v)

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesInt): Series[Int] = mapII(series, _ / _, '/')

  /** @since 0.1.0 */
  @targetName("div")
  def /(series: SeriesDouble): Series[Double] = mapID(series, _ / _, '/')

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Int): Series[Int] = mapI(_ / v)

  /** @since 0.1.0 */
  @targetName("div")
  def /(v: Double): Series[Double] = mapI2D(_ / v)

  /** @since 0.1.0 */
  @targetName("rem")
  def %(v: Int): Series[Int] = mapI(_ % v)

  /** @since 0.1.0 */
  def mod(v: Int): Series[Int] = mapI(_ % v)

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(series: SeriesInt): Series[Boolean] = mapII2B(series, _ < _)

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(series: SeriesInt): Series[Boolean] = mapII2B(series, _ <= _)

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(series: SeriesInt): Series[Boolean] = mapII2B(series, _ >= _)

  /** @since 0.1.0 */
  @targetName("greater")
  def >(series: SeriesInt): Series[Boolean] = mapII2B(series, _ > _)

  /** @since 0.1.0 */
  @targetName("smaller")
  def <(v: Int): Series[Boolean] = mapI2B(_ < v)

  /** @since 0.1.0 */
  @targetName("smallerEqual")
  def <=(v: Int): Series[Boolean] = mapI2B(_ <= v)

  /** @since 0.1.0 */
  @targetName("greaterEqual")
  def >=(v: Int): Series[Boolean] = mapI2B(_ >= v)

  /** @since 0.1.0 */
  @targetName("greater")
  def >(v: Int): Series[Boolean] = mapI2B(_ > v)

  /** @since 0.1.0 */
  def toDouble: Series[Double] = mapI2D(_.toDouble)

  // *** AGGREGATION ***

  /** @since 0.1.0 */
  def max: Int = ops.aggI(this, Int.MinValue, math.max)

  /** @since 0.1.0 */
  def min: Int = ops.aggI(this, Int.MaxValue, math.min)

  /** @since 0.1.0 */
  def sum: Int = ops.aggI(this, 0, _ + _)

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private[pd] inline def mapI(f: Int => Int): Series[Int] = ops.mapI(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapI2B(f: Int => Boolean): Series[Boolean] = ops.mapI2B(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapI2D(f: Int => Double): Series[Double] = ops.mapI2D(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapII(series: Series[Int], f: (Int, Int) => Int, op: Char): Series[Int] =
    Series.ext(ops.mapII(this, series, f), op, series)

  /** @since 0.1.0 */
  private[pd] inline def mapII2B(series: Series[Int], f: (Int, Int) => Boolean): Series[Boolean] =
    ops.mapII2B(this, series, f)

  /** @since 0.1.0 */
  private[pd] inline def mapID(series: Series[Double], f: (Int, Double) => Double, op: Char): Series[Double] =
    Series.ext(ops.mapID(this, series, f), op, series)

/**
  * Series extension for type Int.
  *
  * @since 0.1.0
  */
object SeriesInt:

  class Prefix(v: Int):

    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Int]): Series[Int] = series.mapI(v + _)

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Int]): Series[Int] = series.mapI(v - _)

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Int]): Series[Int] = series.mapI(v * _)

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Int]): Series[Int] = series.mapI(v / _)

  class PrefixDouble(v: Double):
    /** @since 0.1.0 */
    @targetName("plus")
    def +(series: Series[Int]): Series[Double] = series.mapI2D(v + _)

    /** @since 0.1.0 */
    @targetName("minus")
    def -(series: Series[Int]): Series[Double] = series.mapI2D(v - _)

    /** @since 0.1.0 */
    @targetName("times")
    def *(series: Series[Int]): Series[Double] = series.mapI2D(v * _)

    /** @since 0.1.0 */
    @targetName("div")
    def /(series: Series[Int]): Series[Double] = series.mapI2D(v / _)
