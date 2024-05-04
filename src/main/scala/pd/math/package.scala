/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.math

/**
  * Mathematical functions for Series.
  *
  * It covers the Scala standard [[scala.math]] library as well as additional functions (e.g. for statistics).
  *
  * @see
  *   [[scala.math]]
  * @since 0.1.0
  */

import pd.Series
import pd.implicits.{SeriesDouble, SeriesInt}

import scala.math as scalar

// *** MATH LIBRARY FUNCTIONS *** //

/** @since 0.1.0 */
def abs(series: SeriesDouble): Series[Double] = series.mapD(scalar.abs)

/** @since 0.1.0 */
def ceil(series: SeriesDouble): Series[Double] = series.mapD(scalar.ceil)

/** @since 0.1.0 */
def floor(series: SeriesDouble): Series[Double] = series.mapD(scalar.floor)

/** @since 0.1.0 */
def rint(series: SeriesDouble): Series[Double] = series.mapD(scalar.rint)

/** @since 0.1.0 */
def round(series: SeriesDouble): Series[Int] = series.round

/** @since 0.1.0 */
def sqrt(series: SeriesDouble): Series[Double] = series.mapD(scalar.sqrt)

/** @since 0.1.0 */
def abs(series: SeriesInt): Series[Int] = series.mapI(scalar.abs)

// *** STATISTIC FUNCTIONS *** //
/** @since 0.1.0 */
def max(series: SeriesDouble): Double = series.max

/** @since 0.1.0 */
def min(series: SeriesDouble): Double = series.min

/** @since 0.1.0 */
def mean(series: SeriesDouble): Double = series.mean

/** @since 0.1.0 */
def mad(series: SeriesDouble): Double = series.mad

/** @since 0.1.0 */
def sum(series: SeriesDouble): Double = series.sum

/** @since 0.1.0 */
def max(series: SeriesInt): Int = series.max

/** @since 0.1.0 */
def min(series: SeriesInt): Int = series.min

/** @since 0.1.0 */
def sum(series: SeriesInt): Int = series.sum
