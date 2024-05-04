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

import scala.annotation.targetName

/**
  * Series extension for type Boolean.
  *
  * @since 0.1.0
  */
class SeriesBoolean private[pd] (data: SeriesData[Boolean], index: BaseIndex, name: String = "")
    extends Series[Boolean](data, index, name):

  /** @since 0.1.0 */
  @targetName("andOperator")
  def &&(series: SeriesBoolean): Series[Boolean] = mapBB(series, _ && _, '&')

  /** @since 0.1.0 */
  @targetName("orOperator")
  def ||(series: SeriesBoolean): Series[Boolean] = mapBB(series, _ || _, '|')

  /** @since 0.1.0 */
  @targetName("notOperator")
  def unary_! : Series[Boolean] = mapB(!_)

  /**
    * Determines if all (non-null) values are true.
    *
    * @return
    *   False if any value is false and true otherwise.
    * @since 0.1.0
    */
  def all: Boolean = countFalse == 0

  /**
    * Determines if all values are true and none are null.
    *
    * @return
    *   True all value are true and false otherwise. False if index is a slice.
    * @since 0.1.0
    */
  def allStrict: Boolean = countTrue == index.length

  /** @since 0.1.0 */
  def and(series: SeriesBoolean): Series[Boolean] = mapBB(series, _ && _, '&')

  /** @since 0.1.0 */
  def any: Boolean = countTrue > 0

  /** @since 0.1.0 */
  def countFalse: Int = ops.countFalse(this)

  /** @since 0.1.0 */
  def countTrue: Int = ops.countTrue(this)

  /** @since 0.1.0 */
  def indicesTrue: Array[Int] = ops.toIndex(this)

  /** @since 0.1.0 */
  def max: Boolean = any

  /** @since 0.1.0 */
  def min: Boolean = all

  /** @since 0.1.0 */
  def or(series: SeriesBoolean): Series[Boolean] = mapBB(series, _ || _, '|')

  /** @since 0.1.0 */
  def sum: Int = countTrue

  /** @since 0.1.0 */
  def toDouble: Series[Double] = map(b => if b then 1.0 else 0.0)

  /** @since 0.1.0 */
  def toInt: Series[Int] = map(b => if b then 1 else 0)

  /** @since 0.1.0 */
  private[pd] inline def mapB(f: Boolean => Boolean): Series[Boolean] = ops.mapB(this, f)

  /** @since 0.1.0 */
  private[pd] inline def mapBB(series: Series[Boolean], f: (Boolean, Boolean) => Boolean, op: Char): Series[Boolean] =
    Series.ext(ops.mapBB(this, series, f), op, series)
