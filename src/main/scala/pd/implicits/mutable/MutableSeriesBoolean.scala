/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.implicits.SeriesBoolean
import pd.{MutableSeries, Series}

import scala.annotation.targetName

/**
  * MutableSeries extension for type Boolean.
  *
  * @param instance
  *   Private mutable instance.
  * @since 0.1.0
  */
class MutableSeriesBoolean private[pd] (instance: MutableSeries[Boolean]):

  /** @since 0.1.0 */
  @targetName("andAssign")
  def &&=(series: SeriesBoolean): Unit = inner = inner && series

  /** @since 0.1.0 */
  @targetName("orAssign")
  def ||=(series: SeriesBoolean): Unit = inner = inner || series

  /** @since 0.1.0 */
  def invert: Unit = inner = !inner

  // *** NON-MUTATING METHODS ***

  /** @since 0.1.0 */
  @targetName("andOperator")
  def &&(series: SeriesBoolean): Series[Boolean] = inner && series

  /** @since 0.1.0 */
  @targetName("orOperator")
  def ||(series: SeriesBoolean): Series[Boolean] = inner || series

  /** @since 0.1.0 */
  @targetName("notOperator")
  def unary_! : Series[Boolean] = !inner

  /**
    * Determines if all (non-null) values are true.
    *
    * @return
    *   False if any value is false and true otherwise.
    * @since 0.1.0
    */
  def all: Boolean = inner.all

  /**
    * Determines if all values are true and none are null.
    *
    * @return
    *   True all value are true and false otherwise. False if index is a slice.
    * @since 0.1.0
    */
  def allStrict: Boolean = allStrict

  /** @since 0.1.0 */
  def and(series: SeriesBoolean): Series[Boolean] = inner and series

  /** @since 0.1.0 */
  def any: Boolean = inner.any

  /** @since 0.1.0 */
  def countFalse: Int = inner.countFalse

  /** @since 0.1.0 */
  def countTrue: Int = inner.countTrue

  /** @since 0.1.0 */
  def indicesTrue: Array[Int] = inner.indicesTrue

  /** @since 0.1.0 */
  def max: Boolean = inner.max

  /** @since 0.1.0 */
  def min: Boolean = inner.min

  /** @since 0.1.0 */
  def or(series: SeriesBoolean): Series[Boolean] = inner or series

  /** @since 0.1.0 */
  def sum: Int = inner.sum

  /** @since 0.1.0 */
  def toDouble: Series[Double] = inner.toDouble

  /** @since 0.1.0 */
  def toInt: Series[Int] = inner.toInt

  // *** PRIVATE ***

  /** @since 0.1.0 */
  private def inner: Series[Boolean] = instance.inner

  /** @since 0.1.0 */
  private def inner_=(series: Series[Boolean]): Unit = instance.inner = series
