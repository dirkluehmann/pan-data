/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.series.ops

import pd.Series
import pd.exception.BaseIndexException
import pd.internal.index.UniformIndex
import pd.internal.series.{SeriesData, SeriesOps}

abstract class OpsCommon extends SeriesOps:

  // ***************************************************************************
  // *** AGG ***
  // ***************************************************************************

  protected[pd] inline def aggSeries[T, R](series: Series[T], start: R, f: (R, T) => R): R =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = start
    var ix = 0

    if series.index.isBase && !series.hasUndefined then
      while ix < size do
        result = f(result, dataVector(ix))
        ix = ix + 1
      result
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if dataMask == null || dataMask(ix) then result = f(result, dataVector(ix))
      result

  def aggD(s: Series[Double], start: Double, f: (Double, Double) => Double): Double = aggSeries(s, start, f)
  def aggD2I(s: Series[Double], start: Int, f: (Int, Double) => Int): Int = aggSeries(s, start, f)
  def aggI(s: Series[Int], start: Int, f: (Int, Int) => Int): Int = aggSeries(s, start, f)

  // ***************************************************************************
  // *** COMPARE ***
  // ***************************************************************************

  def compareNulls[T, T2](series: Series[T], series2: Series[T2]): Boolean =
    if !series.index.hasSameBase(series2.index) then throw BaseIndexException(Seq(series, series2))
    // note: do not rely on hasUndefined since this is unsafe for MutableSeries
    val size = series.data.length
    var ix = 0
    val dataMask = series.data.mask
    val dataMask2 = series2.data.mask
    val index = series.index
    val indexBijective = index.isBijective
    val index2 = series2.index
    val index2Bijective = index2.isBijective

    while ix < size do
      val exists = (dataMask == null || dataMask(ix)) && (indexBijective || index.isContained(ix))
      val exists2 = (dataMask2 == null || dataMask2(ix)) && (index2Bijective || index2.isContained(ix))
      if exists != exists2 then return false
      ix += 1

    true

  // ***************************************************************************
  // *** COUNT ***
  // ***************************************************************************

  inline def count[T](series: Series[T]): Int =
    if !series.hasUndefined then series.index.length
    else
      val data = series.data
      var cnt = 0
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if dataMask == null || dataMask(ix) then cnt = cnt + 1
      cnt

  def countD(series: Series[Double]): Int =
    val data = series.data
    val dataMask = data.mask
    val dataVector = data.vector
    var cnt = 0

    if series.index.isBijective then
      val size = data.length
      var ix = 0
      while ix < size do
        if (dataMask == null || dataMask(ix)) && !dataVector(ix).isNaN then cnt += 1
        ix += 1
    else
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && !dataVector(ix).isNaN then cnt = cnt + 1
    cnt

  def countFalse(series: Series[Boolean]): Int =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = 0
    var ix = 0

    if !series.hasUndefined then
      while ix < size do
        if !dataVector(ix) then result += 1
        ix += 1
      result
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && !dataVector(ix) then result += 1
      result

  def countTrue(series: Series[Boolean]): Int =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = 0
    var ix = 0

    if !series.hasUndefined then
      while ix < size do
        if dataVector(ix) then result += 1
        ix += 1
      result
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && dataVector(ix) then result += 1
      result

  // ***************************************************************************
  // *** DENSE/FILL/UPDATE ***
  // ***************************************************************************

  protected[pd] inline def denseSeries[T](series: Series[T]): Series[T] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    val array = data.createArray(size)

    if !series.hasUndefined then Series[T](data, series.index.base, series.name)
    else
      val dataMask = data.mask
      val mask = Array.fill[Boolean](size)(false)
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if dataMask == null || dataMask(ix) then
          array(ix) = dataVector(ix)
          mask(ix) = true

      Series[T](SeriesData(array, mask), series.index.base, series.name)

  def denseB(s: Series[Boolean]): Series[Boolean] = denseSeries(s)
  def denseD(s: Series[Double]): Series[Double] = denseSeries(s)
  def denseI(s: Series[Int]): Series[Int] = denseSeries(s)

  protected[pd] inline def fillSeries[T](series: Series[T], series2: Series[T]): Series[T] =
    if !series.index.hasSameBase(series2.index) then throw BaseIndexException(Seq(series, series2))
    if !series.hasUndefined then Series[T](series.data, series.index.base, series.name)
    else
      val dataVector = series.data.vector
      val dataVector2 = series2.data.vector
      val size = series.data.length
      val array = series.data.createArray(size)
      var ix = 0
      val dataMask = series.data.mask
      val dataMask2 = series2.data.mask
      val index = series.index
      val indexBijective = index.isBijective
      val index2 = series2.index
      val index2Bijective = index2.isBijective

      val mask = Array.fill[Boolean](size)(false)
      var inserted = 0
      while ix < size do
        if (dataMask == null || dataMask(ix)) && (indexBijective || index.isContained(ix)) then
          mask(ix) = true
          array(ix) = dataVector(ix)
          inserted += 1
        else if (dataMask2 == null || dataMask2(ix)) && (index2Bijective || index2.isContained(ix)) then
          mask(ix) = true
          array(ix) = dataVector2(ix)
          inserted += 1
        ix += 1

      val containsNull = inserted < size
      Series[T](SeriesData(array, if containsNull then mask else null), series.index.base, series.name)

  def fillB(s: Series[Boolean], s2: Series[Boolean]): Series[Boolean] = fillSeries(s, s2)
  def fillD(s: Series[Double], s2: Series[Double]): Series[Double] = fillSeries(s, s2)
  def fillI(s: Series[Int], s2: Series[Int]): Series[Int] = fillSeries(s, s2)

  protected[pd] inline def fillSeries[T](series: Series[T], v: T): Series[T] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    val array = data.createArray(size)

    if !series.hasUndefined then series
    else
      val dataMask = data.mask
      val mask = Array.fill[Boolean](size)(false)
      var inserted = 0
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        mask(ix) = true
        if dataMask == null || dataMask(ix) then array(ix) = dataVector(ix)
        else array(ix) = v
        inserted += 1
      Series[T](SeriesData(array, if !series.index.isBijective then mask else null), series.index, series.name)

  def fillB(s: Series[Boolean], v: Boolean): Series[Boolean] = fillSeries(s, v)
  def fillD(s: Series[Double], v: Double): Series[Double] = fillSeries(s, v)
  def fillI(s: Series[Int], v: Int): Series[Int] = fillSeries(s, v)

  protected[pd] inline def fillAllSeries[T](series: Series[T], v: T): Series[T] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    val array = data.createArray(size)

    if !series.hasUndefined then Series[T](data, series.index.base, series.name)
    else
      var ix = 0
      val dataMask = data.mask
      while ix < size do
        array(ix) = if dataMask == null || dataMask(ix) then dataVector(ix) else v
        ix += 1
      Series[T](SeriesData(array, null), series.index.base, series.name)

  def fillAllB(s: Series[Boolean], v: Boolean): Series[Boolean] = fillAllSeries(s, v)
  def fillAllD(s: Series[Double], v: Double): Series[Double] = fillAllSeries(s, v)
  def fillAllI(s: Series[Int], v: Int): Series[Int] = fillAllSeries(s, v)

  def update[T](series: Series[T], series2: Series[T]): Series[T] =
    if !series.index.hasSameBase(series2.index) then throw BaseIndexException(Seq(series, series2))
    if !series.hasUndefined then Series[T](series.data, series2.index, series2.name)
    else
      val dataVector = series.data.vector
      val dataVector2 = series2.data.vector
      val size = series.data.length
      val array = dataVector2.clone
      val dataMask = series.data.mask
      val dataMask2 = series2.data.mask
      val index = series.index
      val indexBijective = index.isBijective

      val mask = Array.fill[Boolean](size)(false)
      var inserted = 0
      val it = series2.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && (indexBijective || index.isContained(ix)) then
          mask(ix) = true
          array(ix) = dataVector(ix)
          inserted += 1
        else if dataMask2 == null || dataMask2(ix) then
          mask(ix) = true
          inserted += 1

      val containsNull = inserted < size
      Series[T](SeriesData(array, if containsNull then mask else null), series2.index, series2.name)

  // ***************************************************************************
  // *** FIND/FIRST/LAST ***
  // ***************************************************************************

  protected[pd] inline def findSeries[T](series: Series[T], f: T => Boolean): Option[Int] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = -1
    var ix = 0

    if series.index.isBase && !series.hasUndefined then
      while ix < size do
        if f(dataVector(ix)) then
          result = ix
          ix = size
        ix = ix + 1
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext && result == -1 do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && f(dataVector(ix)) then result = ix

    if result == -1 then None else Some(result)

  def findB(s: Series[Boolean], f: Boolean => Boolean): Option[Int] = findSeries(s, f)
  def findD(s: Series[Double], f: Double => Boolean): Option[Int] = findSeries(s, f)
  def findI(s: Series[Int], f: Int => Boolean): Option[Int] = findSeries(s, f)

  protected[pd] inline def firstSeries[T](series: Series[T], value: T): Option[Int] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = -1
    var ix = 0

    if series.index.isBase && !series.hasUndefined then
      while ix < size do
        if dataVector(ix) == value then
          result = ix
          ix = size
        ix = ix + 1
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext && result == -1 do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && (dataVector(ix) == value) then result = ix

    if result == -1 then None else Some(result)

  def firstB(s: Series[Boolean], v: Boolean): Option[Int] = firstSeries(s, v)
  def firstD(s: Series[Double], v: Double): Option[Int] = firstSeries(s, v)
  def firstI(s: Series[Int], v: Int): Option[Int] = firstSeries(s, v)

  protected[pd] inline def lastSeries[T](series: Series[T], value: T): Option[Int] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    var result = -1
    var ix = size - 1

    if series.index.isBase && !series.hasUndefined then
      while ix >= 0 do
        if dataVector(ix) == value then
          result = ix
          ix = 0
        ix = ix - 1
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && (dataVector(ix) == value) then result = ix

    if result == -1 then None else Some(result)

  def lastB(s: Series[Boolean], v: Boolean): Option[Int] = lastSeries(s, v)
  def lastD(s: Series[Double], v: Double): Option[Int] = lastSeries(s, v)
  def lastI(s: Series[Int], v: Int): Option[Int] = lastSeries(s, v)

  // ***************************************************************************
  // *** INDEX OPERATIONS ***
  // ***************************************************************************

  def extract[T](series: Series[T], indices: Array[Int]): Series[T] =
    val size = indices.length
    val dataVector = series.data.vector
    val dataMask = series.data.mask
    val array = series.data.createArray(size)
    val mask = Array.fill[Boolean](size)(true)
    var containsNull = false
    var i = 0

    while i < size do
      val ix = indices(i)
      if ix != -1 && (dataMask == null || dataMask(ix)) then array(i) = dataVector(ix)
      else
        mask(i) = false
        containsNull = true
      i += 1

    Series[T](SeriesData(array, if containsNull then mask else null), UniformIndex(size), series.name)

  def firstIndex(series: Series[?]): Option[Int] =
    if series.index.isBase && !series.hasUndefined && series.index.nonEmpty then Some(0)
    else
      var result = -1
      val dataMask = series.data.mask
      val it = series.index.iterator
      while it.hasNext && result == -1 do
        val ix = it.next
        if dataMask == null || dataMask(ix) then result = ix
      if result == -1 then None else Some(result)

  def lastIndex(series: Series[?]): Option[Int] =
    if series.index.isBase && !series.hasUndefined && series.index.nonEmpty then Some(series.data.length - 1)
    else
      var result = -1
      val dataMask = series.data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if dataMask == null || dataMask(ix) then result = ix
      if result == -1 then None else Some(result)

  def toIndex(series: Series[Boolean]): Array[Int] =
    val data = series.data
    val size = data.length
    val dataVector = data.vector
    val arraySize = countTrue(series)
    val array = new Array[Int](arraySize)
    var arrayIx = 0
    var ix = 0

    if series.index.isBase && !series.hasUndefined then
      while ix < size do
        if dataVector(ix) then
          array(arrayIx) = ix
          arrayIx += 1
        ix += 1
    else
      val dataMask = data.mask
      val it = series.index.iterator
      while it.hasNext do
        val ix = it.next
        if (dataMask == null || dataMask(ix)) && dataVector(ix) then
          array(arrayIx) = ix
          arrayIx += 1

    require(arraySize == arrayIx)
    array

  def union[T](series: Array[Series[T]]): Series[T] =
    val n = series.length
    var ni = 0
    val length = series.map(_.length).sum
    var ix = 0
    var containsNull = false
    val vector = series.head.data.createArray(length)
    val mask = Array.fill(length)(true)

    while ni < n do
      val it = series(ni).indexIterator
      val vec = series(ni).data.vector
      val m = series(ni).data.mask
      it.foreach(is =>
        if m == null || m(is) then vector(ix) = vec(is)
        else
          mask(ix) = false
          containsNull = true
        ix = ix + 1
      )
      ni = ni + 1

    Series(SeriesData(vector, if containsNull then mask else null), UniformIndex(length), series.head.name)
