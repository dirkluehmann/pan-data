/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.func

import pd.Series
import pd.implicits.{SeriesDouble, SeriesInt}

/**
  * Applies a row-wise function on a Series.
  *
  * @param a
  *   Series.
  * @param f
  *   Row-wise function which takes as arguments elements of both Series. If one of the elements in a row is undefined
  *   the result in undefined.
  * @return
  *   Transformed Series.
  * @since 0.1.0
  */
def map(a: SeriesDouble, f: Double => Double): Series[Double] = a.ops.mapD(a, f)
def map(a: SeriesDouble, b: SeriesDouble, f: (Double, Double) => Double): Series[Double] = a.ops.mapDD(a, b, f)
def map(a: SeriesDouble, b: SeriesInt, f: (Double, Int) => Double): Series[Double] = a.ops.mapDI(a, b, f)
def map(a: SeriesInt, f: Int => Int): Series[Int] = a.ops.mapI(a, f)
def mapToDouble(a: SeriesInt, f: Int => Double): Series[Double] = a.ops.mapI2D(a, f)
def map(a: SeriesInt, b: SeriesInt, f: (Int, Int) => Int): Series[Int] = a.ops.mapII(a, b, f)
def map(a: SeriesInt, b: SeriesDouble, f: (Int, Double) => Double): Series[Double] = a.ops.mapID(a, b, f)
