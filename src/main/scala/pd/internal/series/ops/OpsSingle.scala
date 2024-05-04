/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.series.ops

import pd.Series
import pd.exception.*
import pd.internal.series.SeriesData

import scala.reflect.ClassTag

private[pd] class OpsSingle extends OpsCommon:

  // ***************************************************************************
  // *** MAP ***
  // ***************************************************************************

  protected inline def mapSeries[T, R: ClassTag](series: Series[T], f: T => R): Series[R] =
    val data = mapData(series, f)
    Series[R](data, series.index, series.name)

  protected inline def mapSeries[T, T2, R: ClassTag](
      series: Series[T],
      series2: Series[T2],
      f: (T, T2) => R,
  ): Series[R] =
    val data = mapData(series, series2, f)
    Series[R](data, series.index, series.name)

  def mapB(s: Series[Boolean], f: Boolean => Boolean): Series[Boolean] = mapSeries(s, f)
  def mapBB(s: Series[Boolean], s2: Series[Boolean], f: (Boolean, Boolean) => Boolean): Series[Boolean] =
    mapSeries(s, s2, f)
  def mapD(s: Series[Double], f: Double => Double): Series[Double] = mapSeries(s, f)
  def mapD2B(s: Series[Double], f: Double => Boolean): Series[Boolean] = mapSeries(s, f)
  def mapD2I(s: Series[Double], f: Double => Int): Series[Int] = mapSeries(s, f)
  def mapDD(s: Series[Double], s2: Series[Double], f: (Double, Double) => Double): Series[Double] = mapSeries(s, s2, f)
  def mapDD2B(s: Series[Double], s2: Series[Double], f: (Double, Double) => Boolean): Series[Boolean] =
    mapSeries(s, s2, f)
  def mapDI(s: Series[Double], s2: Series[Int], f: (Double, Int) => Double): Series[Double] = mapSeries(s, s2, f)
  def mapI(s: Series[Int], f: Int => Int): Series[Int] = mapSeries(s, f)
  def mapI2B(s: Series[Int], f: Int => Boolean): Series[Boolean] = mapSeries(s, f)
  def mapI2D(s: Series[Int], f: Int => Double): Series[Double] = mapSeries(s, f)
  def mapII(s: Series[Int], s2: Series[Int], f: (Int, Int) => Int): Series[Int] = mapSeries(s, s2, f)
  def mapII2B(s: Series[Int], s2: Series[Int], f: (Int, Int) => Boolean): Series[Boolean] = mapSeries(s, s2, f)
  def mapID(s: Series[Int], s2: Series[Double], f: (Int, Double) => Double): Series[Double] = mapSeries(s, s2, f)

  // ***************************************************************************
  // *** PRIVATE METHODS ***
  // ***************************************************************************

  /**
    * Creates series data from an operation. Allow for specialization via inline.
    *
    * @param series
    *   Series.
    * @param f
    *   Operation.
    * @tparam T
    *   Type of series.
    * @tparam R
    *   Result type.
    * @return
    *   Created SeriesData.
    * @since 0.1.0
    */
  private inline def mapData[T, R: ClassTag](
      series: Series[T],
      f: T => R,
  ): SeriesData[R] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    val array = new Array[R](size)

    if !series.hasUndefined then
      var ix = 0
      while ix < size do
        val v = f(dataVector(ix))
        if v == null then throw MapToNullException()
        array(ix) = v
        ix += 1
      SeriesData(array, null)
    else
      val dataMask = data.mask
      val mask = Array.fill[Boolean](size)(false)
      var inserted = 0
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if dataMask == null || dataMask(ix) then
          val v = f(dataVector(ix))
          if v == null then throw MapToNullException()
          mask(ix) = true
          array(ix) = v
          inserted += 1
      val containsNull = inserted < size
      SeriesData(array, if containsNull then mask else null)

  /**
    * Creates series data from an operation on two Series. Allow for specialization via inline.
    *
    * @param series
    *   Series.
    * @param series2
    *   Series with same index base as `series`.
    * @param f
    *   Operation.
    * @tparam T
    *   Type of series.
    * @tparam T2
    *   Type of second series.
    * @tparam R
    *   Result type.
    * @return
    *   Created SeriesData.
    * @since 0.1.0
    */
  private[pd] inline def mapData[T, T2, R: ClassTag](
      series: Series[T],
      series2: Series[T2],
      f: (T, T2) => R,
  ): SeriesData[R] =
    if !series.index.hasSameBase(series2.index) then throw BaseIndexException(series, series2)

    val dataVector = series.data.vector
    val dataVector2 = series2.data.vector
    val size = series.data.length
    val array = new Array[R](size)
    var ix = 0

    if !series.hasUndefined && !series2.hasUndefined then
      while ix < size do
        val v = f(dataVector(ix), dataVector2(ix))
        if v == null then throw MapToNullException()
        array(ix) = v
        ix += 1
      SeriesData(array, null)
    else
      val dataMask = series.data.mask
      val dataMask2 = series2.data.mask
      val index2 = series2.index
      val index2Bijective = index2.isBijective
      val mask = Array.fill[Boolean](size)(false)
      var inserted = 0
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && (dataMask2 == null || dataMask2(ix)) &&
          (index2Bijective || index2.isContained(ix))
        then
          val v = f(dataVector(ix), dataVector2(ix))
          if v == null then throw MapToNullException()
          mask(ix) = true
          array(ix) = v
          inserted += 1
      val containsNull = inserted < size
      SeriesData(array, if containsNull then mask else null)
