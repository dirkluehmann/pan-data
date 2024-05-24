/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.series

import pd.Series
import pd.exception.SeriesCastException

import scala.reflect.ClassTag

private[pd] abstract class SeriesOps:

  // ***************************************************************************
  // *** AGG ***
  // ***************************************************************************

  /**
    * Aggregates over a Series.
    *
    * @param s
    *   Series.
    * @param start
    *   Start value for aggregation.
    * @param f
    *   Aggregating operation which fulfills associativity and commutativity. The latter properties are required for
    *   partition-wise evaluation.
    * @tparam T
    *   Type of series.
    * @tparam R
    *   Result type.
    * @return
    *   Created Series.
    * @since 0.1.0
    */
  // todo more specialization
  def agg[T, R](s: Series[T], start: R, f: (R, T) => R): R =
    if s.isDouble then
      start match
        case start: Double => return aggD(toD(s), start, f.asInstanceOf[(Double, Double) => Double]).asInstanceOf[R]
        case start: Int    => return aggD2I(toD(s), start, f.asInstanceOf[(Int, Double) => Int]).asInstanceOf[R]
        case _             =>
    else if s.isInt then
      start match
        case start: Int => return aggI(toI(s), start, f.asInstanceOf[(Int, Int) => Int]).asInstanceOf[R]
        case _          =>
    // generic default for all other combinations
    aggSeries(s, start, f)

  def aggD(s: Series[Double], start: Double, f: (Double, Double) => Double): Double
  def aggD2I(s: Series[Double], start: Int, f: (Int, Double) => Int): Int
  def aggI(s: Series[Int], start: Int, f: (Int, Int) => Int): Int
  protected def aggSeries[T, R](series: Series[T], start: R, f: (R, T) => R): R

  // ***************************************************************************
  // *** COMPARE ***
  // ***************************************************************************

  /**
    * Returns false if null values (either in mask or by slicing) are not matching for all elements.
    *
    * @param series
    *   Series.
    * @param series2
    *   Series to compare with.
    * @tparam T
    *   Type of series.
    * @tparam T2
    *   Type of series2.
    * @return
    *   True if missing values are equal, false otherwise.
    * @since 0.1.0
    */
  def compareNulls[T, T2](series: Series[T], series2: Series[T2]): Boolean

  // ***************************************************************************
  // *** COUNT ***
  // ***************************************************************************

  /**
    * Counts number of non-null elements.
    *
    * @param series
    *   Series.
    * @tparam T
    *   Type of series.
    * @return
    *   Number of non-null elements.
    * @since 0.1.0
    */
  def count[T](series: Series[T]): Int

  def countD(series: Series[Double]): Int
  def countFalse(series: Series[Boolean]): Int
  def countTrue(series: Series[Boolean]): Int

  // ***************************************************************************
  // *** DENSE/FILL/UPDATE ***
  // ***************************************************************************

  /**
    * Returns a Series with uniform index where all missing values are represented by the mask.
    *
    * @param s
    *   Series.
    * @tparam T
    *   Type of Series.
    * @return
    *   Series with uniform index.
    * @since 0.1.0
    */
  def dense[T](s: Series[T]): Series[T] =
    if s.isBoolean then to[T](denseB(toB(s)))
    else if s.isInt then to[T](denseI(toI(s)))
    else if s.isDouble then to[T](denseD(toD(s)))
    else denseSeries(s)

  def denseB(s: Series[Boolean]): Series[Boolean]
  def denseD(s: Series[Double]): Series[Double]
  def denseI(s: Series[Int]): Series[Int]
  protected def denseSeries[T](s: Series[T]): Series[T]

  /**
    * Creates series data from coalesce on two Series.
    *
    * @param s
    *   Series.
    * @param s2
    *   Series with the same base index as `series`.
    * @tparam T
    *   Type of series.
    * @return
    *   Series with data from the first series or if null from the second. The index is expanded to the uniform base
    *   index.
    * @since 0.1.0
    */
  def fill[T](s: Series[T], s2: Series[T]): Series[T] =
    if s.isBoolean then if s2.isBoolean then to[T](fillB(toB(s), toB(s2))) else throw SeriesCastException(s, "Boolean")
    else if s.isDouble then
      if s2.isDouble then to[T](fillD(toD(s), toD(s2))) else throw SeriesCastException(s, "Double")
    else if s.isInt then if s2.isInt then to[T](fillI(toI(s), toI(s2))) else throw SeriesCastException(s, "Int")
    else fillSeries(s, s2)

  def fillB(s: Series[Boolean], s2: Series[Boolean]): Series[Boolean]
  def fillD(s: Series[Double], s2: Series[Double]): Series[Double]
  def fillI(s: Series[Int], s2: Series[Int]): Series[Int]
  protected def fillSeries[T](s1: Series[T], s2: Series[T]): Series[T]

  /**
    * Fills unset values of a Series with regards to the current index.
    *
    * @param s
    *   Series.
    * @param value
    *   Value for filling unset values.
    * @tparam T
    *   Type of series.
    * @return
    *   Series.
    * @since 0.1.0
    */
  // format: off
  def fill[T](s: Series[T], value: T): Series[T] =
    value match
      case v: Boolean => if s.isBoolean then to[T](fillB(toB(s), v)) else throw SeriesCastException(s, "Boolean")
      case v: Int => if s.isInt then to[T](fillI(toI(s), v)) else throw SeriesCastException(s, "Int")
      case v: Double => if s.isDouble then to[T](fillD(toD(s), v)) else throw SeriesCastException(s, "Double")
      case v: Any => fillSeries(s, v)

  def fillB(s: Series[Boolean], v: Boolean): Series[Boolean]
  def fillD(s: Series[Double], v: Double): Series[Double]
  def fillI(s: Series[Int], v: Int): Series[Int]
  protected def fillSeries[T](s: Series[T], value: T): Series[T]

  /**
   * Fills unset values of a Series ignoring the current index assuming that the Series is dense.
   * The index is restored to a uniform index.
   *
   * @param s
   *   Series.
   * @param value
   *   Value for filling unset values.
   * @tparam T
   *   Type of series.
   * @return
   *   Series.
   * @since 0.1.0
    */
  def fillAll[T](s: Series[T], value: T): Series[T] =
    value match
      case v: Boolean => if s.isBoolean then to[T](fillAllB(toB(s), v)) else throw SeriesCastException(s, "Boolean")
      case v: Int => if s.isInt then to[T](fillAllI(toI(s), v)) else throw SeriesCastException(s, "Int")
      case v: Double => if s.isDouble then to[T](fillAllD(toD(s), v)) else throw SeriesCastException(s, "Double")
      case v: Any => fillAllSeries(s, v)

  def fillAllB(s: Series[Boolean], v: Boolean): Series[Boolean]
  def fillAllD(s: Series[Double], v: Double): Series[Double]
  def fillAllI(s: Series[Int], v: Int): Series[Int]
  protected def fillAllSeries[T](s: Series[T], value: T): Series[T]
  // format: on

  /**
    * Updates the series2 with values from series (for values in series2.index only). The inner type of the returned
    * Series is of type of series2 regardless of the type parameter T.
    *
    * @param series
    *   Series.
    * @param series2
    *   Series with the same base index as `series`.
    * @tparam T
    *   Type of series.
    * @return
    *   Series with data from the first series or if null from the second. The index is the index of series2.
    * @since 0.1.0
    */
  // todo specialization
  def update[T](series: Series[T], series2: Series[T]): Series[T]

  // ***************************************************************************
  // *** FIND/FIRST/LAST ***
  // ***************************************************************************

  def find[T](s: Series[T], f: T => Boolean): Option[Int] =
    if s.isBoolean then findB(toB(s), f.asInstanceOf[Boolean => Boolean])
    else if s.isInt then findI(toI(s), f.asInstanceOf[Int => Boolean])
    else if s.isDouble then findD(toD(s), f.asInstanceOf[Double => Boolean])
    else findSeries(s, f)

  def findB(s: Series[Boolean], f: Boolean => Boolean): Option[Int]
  def findD(s: Series[Double], f: Double => Boolean): Option[Int]
  def findI(s: Series[Int], f: Int => Boolean): Option[Int]
  protected def findSeries[T](series: Series[T], f: T => Boolean): Option[Int]

  def first[T](s: Series[T], value: T): Option[Int] =
    if s.isBoolean then firstB(toB(s), value.asInstanceOf[Boolean])
    else if s.isInt then firstI(toI(s), value.asInstanceOf[Int])
    else if s.isDouble then firstD(toD(s), value.asInstanceOf[Double])
    else firstSeries(s, value)

  def firstB(s: Series[Boolean], v: Boolean): Option[Int]
  def firstD(s: Series[Double], v: Double): Option[Int]
  def firstI(s: Series[Int], v: Int): Option[Int]
  protected def firstSeries[T](series: Series[T], value: T): Option[Int]

  def last[T](s: Series[T], value: T): Option[Int] =
    if s.isBoolean then lastB(toB(s), value.asInstanceOf[Boolean])
    else if s.isInt then lastI(toI(s), value.asInstanceOf[Int])
    else if s.isDouble then lastD(toD(s), value.asInstanceOf[Double])
    else lastSeries(s, value)

  def lastB(s: Series[Boolean], v: Boolean): Option[Int]
  def lastD(s: Series[Double], v: Double): Option[Int]
  def lastI(s: Series[Int], v: Int): Option[Int]
  protected def lastSeries[T](series: Series[T], value: T): Option[Int]

  // ***************************************************************************
  // *** MAP ***
  // ***************************************************************************

  /**
    * Creates Series from an operation.
    *
    * @param s
    *   Series.
    * @param f
    *   Operation.
    * @tparam T
    *   Type of series.
    * @tparam R
    *   Result type.
    * @return
    *   Created Series with index of `series`.
    * @since 0.1.0
    */
  // todo more specializations, check if return is preserved
  def map[T, R: ClassTag](s: Series[T], f: T => R): Series[R] =
    if s.isDouble then {
      val returned = Series.empty[R]
      if returned.isDouble then return to[R](mapD(toD(s), f.asInstanceOf[Double => Double]))
      else if returned.isBoolean then return to[R](mapD2B(toD(s), f.asInstanceOf[Double => Boolean]))
    } else if s.isInt then {
      val returned = Series.empty[R]
      if returned.isInt then return to[R](mapI(toI(s), f.asInstanceOf[Int => Int]))
      else if returned.isDouble then return to[R](mapI2D(toI(s), f.asInstanceOf[Int => Double]))
    }
    // generic default for all other combinations
    mapSeries(s, f)

  /**
    * Creates Series from an operation on two Series.
    *
    * @param s
    *   Series.
    * @param s2
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
    *   Created Series with index of `series`.
    * @since 0.1.0
    */
  // todo more specializations, check if return is preserved
  def map[T, T2, R: ClassTag](s: Series[T], s2: Series[T2], f: (T, T2) => R): Series[R] =
    if s.isDouble then {
      val returned = Series.empty[R]
      if s2.isDouble then
        if returned.isDouble then return to[R](mapDD(toD(s), toD(s2), f.asInstanceOf[(Double, Double) => Double]))
        else if returned.isBoolean then
          return to[R](mapDD2B(toD(s), toD(s2), f.asInstanceOf[(Double, Double) => Boolean]))
      else if s2.isInt then
        if returned.isDouble then return to[R](mapDI(toD(s), toI(s2), f.asInstanceOf[(Double, Int) => Double]))
    } else if s.isInt then {
      val returned = Series.empty[R]
      if s2.isInt then
        if returned.isInt then return to[R](mapII(toI(s), toI(s2), f.asInstanceOf[(Int, Int) => Int]))
      else if s2.isDouble then
        if returned.isDouble then return to[R](mapID(toI(s), toD(s2), f.asInstanceOf[(Int, Double) => Double]))
    }
    // generic default for all other combinations
    mapSeries(s, s2, f)

  def mapB(s: Series[Boolean], f: Boolean => Boolean): Series[Boolean]
  def mapBB(s: Series[Boolean], s2: Series[Boolean], f: (Boolean, Boolean) => Boolean): Series[Boolean]
  def mapD(s: Series[Double], f: Double => Double): Series[Double]
  def mapD2B(s: Series[Double], f: Double => Boolean): Series[Boolean]
  def mapD2I(s: Series[Double], f: Double => Int): Series[Int]
  def mapDD(s: Series[Double], s2: Series[Double], f: (Double, Double) => Double): Series[Double]
  def mapDD2B(s: Series[Double], s2: Series[Double], f: (Double, Double) => Boolean): Series[Boolean]
  def mapDI(s: Series[Double], s2: Series[Int], f: (Double, Int) => Double): Series[Double]
  def mapI(s: Series[Int], f: Int => Int): Series[Int]
  def mapI2B(s: Series[Int], f: Int => Boolean): Series[Boolean]
  def mapI2D(s: Series[Int], f: Int => Double): Series[Double]
  def mapII(s: Series[Int], s2: Series[Int], f: (Int, Int) => Int): Series[Int]
  def mapII2B(s: Series[Int], s2: Series[Int], f: (Int, Int) => Boolean): Series[Boolean]
  def mapID(s: Series[Int], s2: Series[Double], f: (Int, Double) => Double): Series[Double]
  protected def mapSeries[T, R: ClassTag](s: Series[T], f: T => R): Series[R]
  protected def mapSeries[T, T2, R: ClassTag](s: Series[T], s2: Series[T2], f: (T, T2) => R): Series[R]

  // ***************************************************************************
  // *** INDEX OPERATIONS ***
  // ***************************************************************************

  def extract[T](series: Series[T], indices: Array[Int]): Series[T]
  def firstIndex(series: Series[?]): Option[Int]
  def toIndex(series: Series[Boolean]): Array[Int]
  def lastIndex(series: Series[?]): Option[Int]
  def union[T](series: Array[Series[T]]): Series[T]

  // ***************************************************************************
  // *** PRIVATE ***
  // ***************************************************************************

  private[pd] inline def toB(s: Series[?]): Series[Boolean] = s.asInstanceOf[Series[Boolean]]
  private[pd] inline def toD(s: Series[?]): Series[Double] = s.asInstanceOf[Series[Double]]
  private[pd] inline def toI(s: Series[?]): Series[Int] = s.asInstanceOf[Series[Int]]
  private[pd] inline def toS(s: Series[?]): Series[String] = s.asInstanceOf[Series[String]]
  private[pd] inline def to[T](s: Series[?]): Series[T] = s.asInstanceOf[Series[T]]
