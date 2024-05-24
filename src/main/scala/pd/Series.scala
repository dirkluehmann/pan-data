/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.*
import pd.implicits.*
import pd.internal.index.*
import pd.internal.series.*
import pd.internal.series.ops.*
import pd.internal.utils.StringUtils.asElements
import pd.internal.utils.{RequireType, StringUtils, TypeString}

import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import scala.annotation.{nowarn, targetName}
import scala.collection
import scala.collection.JavaConverters.*
import scala.language.{dynamics, implicitConversions}
import scala.math.Ordering.StringOrdering
import scala.reflect.{ClassTag, TypeTest, Typeable, classTag}

/**
  * A Series is a column of data. It has an index and may have undefined (missing) values.
  *
  * @see
  *   - <a href="https://pan-data.org/scala/basics/creating-a-series.html">Creating a Series</a>
  *   - <a href="https://pan-data.org/scala/basics/working-with-series.html">Working with Series </a>
  *   - <a href="https://pan-data.org/scala/basics/custom-series-operations.html">Custom Series Operations</a>
  *   - <a href="https://pan-data.org/scala/basics/index-operations.html">Index Operations</a>
  *   - <a href="https://pan-data.org/scala/basics/muteable.html">Mutable Access</a>
  * @since 0.1.0
  */
class Series[T] private[pd] (
    private[pd] val data: SeriesData[T],
    private[pd] val index: BaseIndex,
    val name: String = "",
) extends IndexOps[Series[T]]:

  /**
    * Concatenates the Series on the left and the DataFrame on the right hand side.
    *
    * @param df
    *   DataFrame.
    * @return
    *   DataFrame, where the Series is the first column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the right DataFrame must be included in the left Series.
    *   - Data on the right hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the left operand.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("concat")
  def |(df: DataFrame): DataFrame =
    DataFrame.fromSeries(this) | df

  /**
    * Concatenates Series on the left and right hand side.
    *
    * @param series
    *   Series.
    * @return
    *   DataFrame with two columns, where `series` is the last column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the right Series must be included in the left one.
    *   - Data on the right hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the left operand.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("concat")
  def |(series: Series[?]): DataFrame =
    DataFrame.fromSeries(this) | series

  /**
    * Concatenates the DataFrame on the left and the Series on the right hand side.
    *
    * @param df
    *   DataFrame.
    * @return
    *   DataFrame, where the Series is the last column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the left DataFrame must be included in the right Series.
    *   - Data on the left hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the right operand.
    *   - The operators `|` and `::` are equivalent if indices on the left and right side are equal.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("prepend")
  def ::(df: DataFrame): DataFrame =
    df :: DataFrame.fromSeries(this)

  /**
    * Concatenates the Series on the left and the Series on the right hand side.
    *
    * @param series
    *   DataFrame.
    * @return
    *   DataFrame, where the Series `series` is the first column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the left Series must be included in the right Series.
    *   - Data on the left hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the right operand.
    *   - The operators `|` and `::` are equivalent if indices on the left and right side are equal.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("prepend")
  def ::[T2](series: Series[T2]): DataFrame =
    series :: DataFrame.fromSeries(this)

  /**
    * Prepends a `string` to a Series, where the Series is converted to a String Series if required.
    *
    * @param string
    *   String to be prepended.
    * @return
    *   String Series.
    * @see
    *   [[https://pan-data.org/scala/basics/working-with-series.html]]
    * @since 0.1.0
    */
  @targetName("prepend")
  def +:(string: String): Series[String] = str.map(string + _)

  /**
    * Compares two series row-by-row returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if one of the values is not defined (null) or is not in the index.
    *
    * @param series
    *   Series to compare with.
    * @return
    *   Boolean Series with index of left operand.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==[T2](series: Series[T2]): Series[Boolean] = map(series, _ == _)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if the values of the series is not defined (null).
    *
    * Value to compare with.
    * @param v
    *   Value.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==[T2](v: T2): Series[Boolean] = map(_ == v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if the values of the Series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==(v: Boolean): Series[Boolean] = map(_ == v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if the value of the Series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==(v: Double): Series[Boolean] = map(_ == v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if the value of the Series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==(v: Int): Series[Boolean] = map(_ == v)

  /**
    * Compares two series row-by-row returning a boolean Series with entries
    *   - `true` if values are not equal,
    *   - `false` if values are equal,
    *   - `null` if one of the values is not defined (null) or is not in the index.
    *
    * @param series
    *   Series to compare with.
    * @return
    *   Boolean Series with index of left operand.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("notEqualsByRow")
  def !=[T2](series: Series[T2]): Series[Boolean] = map(series, _ != _)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are not equal,
    *   - `false` if values are equal,
    *   - `null` if the value of the series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("notEqualsByRow")
  def !=[T2](v: T2): Series[Boolean] = map(_ != v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are not equal,
    *   - `false` if values are equal,
    *   - `null` if the value of the series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("notEqualsByRow")
  def !=(v: Boolean): Series[Boolean] = map(_ != v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are not equal,
    *   - `false` if values are equal,
    *   - `null` if the value of the series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("notEqualsByRow")
  def !=(v: Double): Series[Boolean] = map(_ != v)

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are not equal,
    *   - `false` if values are equal,
    *   - `null` if the value of the series is not defined (null).
    *
    * @param v
    *   Value to compare with.
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("notEqualByRow")
  def !=(v: Int): Series[Boolean] = map(_ != v)

  /**
    * Aggregates over values row-by-row.
    *
    * @param start
    *   Start value.
    * @param f
    *   Aggregation function `(aggregatedValue, rowValue) => newAggregatedValue`.
    * @return
    *   Aggregated value.
    * @note
    *   The aggregation function should not depend on the traversal order.
    * @see
    *   [[https://pan-data.org/scala/basics/custom-series-operations.html]]
    * @since 0.1.0
    */
  def agg[R](start: R, f: (R, T) => R): R = ops.agg(this, start, f)

  /**
    * Returns the value at index `ix` as an Option, i.e. `Some(value)` or `None`.
    *
    * @param ix
    *   Row index.
    * @return
    *   Value as an Option. `None` if the value is undefined or `ix` not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply(ix: Int): Option[T] = if index.contains(ix) then data(ix) else None

  /**
    * Returns the value at index `ix` as an Option, i.e. `Some(value)` or `None`.
    *
    * @param ix
    *   Option of row index.
    * @return
    *   Value as an Option. `None` if `ix` is `None`, the value is undefined or `ix` not in the index.
    * @throws IndexBoundsException
    *   If the value of `ix` is not part of the base index.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply(ix: Option[Int]): Option[T] = if ix.isDefined && index.contains(ix.get) then data(ix.get) else None

  /**
    * Returns the value at index `ix` or the value `default` if undefined.
    *
    * @param ix
    *   Row index.
    * @param default
    *   Default value.
    * @return
    *   Value or `default` if the value is not defined or `ix` not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   Due to performance, this method should be preferred over `apply(ix: Int): Option[T]`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  inline def apply(ix: Int, default: => T): T =
    if index.contains(ix) then
      if data.getMask(ix) then data.get(ix)
      else default
    else default

  /**
    * Returns the value at index `ix` or the value `default` if undefined.
    *
    * @param ix
    *   Option of row index.
    * @param default
    *   Default value.
    * @return
    *   Value or `default` if `ix` is `None`, the value is not defined or `ix` not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   Due to performance, this method should be preferred over `apply(ix: Option[Int]): Option[T]`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  inline def apply(ix: Option[Int], default: => T): T =
    if ix.isDefined && index.contains(ix.get) then
      val i = ix.get
      if data.getMask(i) then data.get(i)
      else default
    else default

  /**
    * Casts the Series into another type.
    *
    * @return
    *   Instance of Series[T2].
    * @throws SeriesCastException
    *   If inner type does not equal the new type `T2`.
    * @note
    *   When casting from Any to native types, consider using [[pd.implicits.SeriesAny.asBoolean]],
    *   [[pd.implicits.SeriesAny.asDouble]], [[pd.implicits.SeriesAny.asInt]], etc. for more performance.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  def as[T2: Typeable: ClassTag]: Series[T2] =
    if (!isType[T2]) throw SeriesCastException(this, TypeString.mapType(classTag[T2].toString))
    Series[T2](data.asInstanceOf[SeriesData[T2]], index, name)

  /**
    * Renames the Series.
    *
    * @param name
    *   New name.
    * @return
    *   Series with new name.
    * @since 0.1.0
    */
  def as(name: String): Series[T] = Series(data, index, name)

  /**
    * Returns the value at index `ix` as an Option, i.e. `Some(value)` or `None`.
    *
    * @param ix
    *   Row index.
    * @return
    *   Value as an Option. `None` if the value is not set or not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @throws SeriesCastException
    *   If inner type is not instance of the new type `T2`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def as[T2: Typeable](ix: Int): Option[T2] = apply(ix).asInstanceOf[Option[T2]]

  /**
    * Returns the value at index `ix` as an Option, i.e. `Some(value)` or `None`.
    *
    * @param ix
    *   Option of row index.
    * @return
    *   Value as an Option. `None` if `ix` is `None`, the value is not set or not in the index.
    * @throws IndexBoundsException
    *   If the value of `ix` is not part of the base index.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def as[T2: Typeable: ClassTag](ix: Option[Int]): Option[T2] = apply(ix).asInstanceOf[Option[T2]]

  /**
    * Returns the value at index `ix` or the value `default` if not set.
    *
    * @param ix
    *   Row index.
    * @param default
    *   Default value.
    * @return
    *   Value or `default` if the value is not set or not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   Due to performance, this method should be preferred over `as[T2](ix: Int): Option[T2]`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */

  def as[T2: Typeable: ClassTag](ix: Int, default: => T2): T2 =
    if index.contains(ix) then
      if data.getMask(ix) then data.get(ix).asInstanceOf[T2]
      else default
    else default

  /**
    * Returns the value at index `ix` or the value `default` if not set.
    *
    * @param ix
    *   Option of row index.
    * @param default
    *   Default value.
    * @return
    *   Value or `default` if `ix` is `None`, the value is not set or not in the index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   Due to performance, this method should be preferred over `as[T2](ix: Option[Int]): Option[T2]`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def as[T2: Typeable: ClassTag](ix: Option[Int], default: => T2): T2 =
    if ix.isDefined && index.contains(ix.get) then
      val i = ix.get
      if data.getMask(i) then data.get(i).asInstanceOf[T2]
      else default
    else default

  /**
    * Series as instance of `Series[Any]`.
    *
    * @return
    *   Series of type Any.
    * @note
    *   Only the outer type of the Series is altered.
    * @since 0.1.0
    */
  def asAny: Series[Any] = asInstanceOf[Series[Any]]

  /**
    * Counts the number of defined (non-null) elements. For Double Series, also [[Double.NaN]] is accounted as not
    * defined.
    *
    * @return
    *   Number of defined (non-null) elements.
    * @see
    *   [[https://pan-data.org/scala/basics/working-with-series.html]]
    * @since 0.1.0
    */
  def count: Int =
    if isDouble then ops.countD(this.asInstanceOf[Series[Double]])
    else ops.count(this)

  /**
    * Keeps only defined elements and removes all index positions with undefined elements.
    *
    * @return
    *   Equivalent Series without undefined elements and trimmed index.
    * @since 0.1.0
    */
  def defined: Series[T] =
    if isDefined then this
    else
      // noinspection ConvertibleToMethodValue
      withIndex(SeqIndex.unchecked(index.base, index.iterator.filter(data.getMask(_)).toArray))

  /**
    * Converts the Series into a Series with a uniform (dense) index, which equals the base index. Missing indices are
    * replaced by explicit undefined values (masked). The (accessible) data is not changed.
    *
    * @return
    *   Same Series but with an uniform index.
    * @see
    *   [[https://pan-data.org/scala/advanced/indices.html]]
    * @since 0.1.0
    */
  def dense: Series[T] = ops.dense[T](this)

  /**
    * Prints the Series as a table with an index column and annotated column types.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param colWidth
    *   The width of the column.
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
    * @since 0.1.0
    */
  def display(
      n: Int = Settings.printRowLength,
      colWidth: Int = Settings.printColWidthSeries,
  ): Unit =
    println(
      toString(n = n, colWidth = colWidth)
    )

  /**
    * Series with unique (defined) values in the Series.
    *
    * @return
    *   Series with distinct values.
    * @since 0.1.0
    */
  def distinct: Series[T] =
    val distinctValues = unique
    new Series[T](SeriesData(distinctValues), UniformIndex(distinctValues.length), name)

  /**
    * Indicates if two Series are equal with respect to
    *   - name,
    *   - index and
    *   - values in all rows.
    *
    * @param that
    *   Series (or object) to compare with.
    * @return
    *   True if equal and false otherwise.
    * @note
    *   - Comparing an undefined value (`null`) with undefined gives true, whereas undefined with not undefined is
    *     false.
    *   - Comparing Double.NaN with Double.NaN gives true, whereas Double.NaN with undefined (`null`) or any defined
    *     value gives false.
    *   - The method differentiates between a missing index and an undefined (`null`) value.
    *   - If the object is not a Series it returns `false`.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @nowarn
  override def equals(that: Any): Boolean =
    that match {
      case that: Series[?] =>
        canEqual(that) && name == that.name && hasSameIndex(that.asInstanceOf[Series[T]]) &&
          equalsByValue(that.asInstanceOf[Series[T]])
      case that: MutableSeries[?] =>
        canEqual(that) && name == that.name && hasSameIndex(that.inner.asInstanceOf[Series[T]]) &&
          equalsByValue(that.inner.asInstanceOf[Series[T]])
      case _ => false
    }

  /**
    * Indicates if two Series are equal with respect to (type and) values in all rows.
    *
    * @param series
    *   Series (or object) to compare with.
    * @return
    * @note
    *   - Comparing an undefined value (`null`) with undefined gives true, whereas undefined with not undefined is
    *     false.
    *   - Comparing Double.NaN with Double.NaN gives true, whereas Double.NaN with undefined (`null`) or any defined
    *     value gives false.
    *   - The method does not differentiate between a undefined value in the index or in the data.
    *   - The comparison ignores different names and indices. The index order is therefore ignored.
    *   - If the two Series have different base indices the result is false.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  def equalsByValue[T2](series: Series[T2]): Boolean =
    index.hasSameBase(series.index) && hasSameDefined(series) && typeEquals(series) && (
      if isDouble then
        this
          .asInstanceOf[Series[Double]]
          .map(series.asInstanceOf[Series[Double]], (a, b) => a == b || (a.isNaN && b.isNaN))
          .all
      else map(series, _ == _).all
    )

  /**
    * Indicates if a value is contained in the Series.
    *
    * @param value
    *   Value to search for.
    *
    * @return
    *   Boolean flag if the the value exists.
    * @since 0.1.0
    */
  def exists(value: T): Boolean = first(value).isDefined

  /**
    * Replaces undefined (null) values by the values of `series` for the current index. This is also known as coalesce
    * operation.
    *
    * @param series
    *   Series with the same base index.
    * @return
    *   Series with data from the first series or if undefined from the second. The original index is not altered.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @since 0.1.0
    */
  def fill[T2 <: T](series: Series[T2]): Series[T] =
    requireTypeMatch(series)
    ops.fill(this, series.asInstanceOf[Series[T]]).withIndex(index)

  /**
    * Fills undefined (null) values of a Series with respect to the current index.
    *
    * @param value
    *   Value for filling non-set values.
    * @return
    *   Series with undefined values filled. The original index is preserved.
    * @since 0.1.0
    */
  def fill(value: T): Series[T] =
    requireTypeMatch(value)
    ops.fill[T](this, value)

  /**
    * Replaces undefined (null) values by the values of `series` also known as coalesce operation. The index is expanded
    * to the uniform base index.
    *
    * @param series
    *   Series with the same base index.
    * @return
    *   Series with data from the first series or if undefined from the second. The index is expanded to the uniform
    *   base index.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @since 0.1.0
    */
  def fillAll[T2 <: T](series: Series[T2]): Series[T] =
    requireTypeMatch(series)
    ops.fill(this, series.asInstanceOf[Series[T]])

  /**
    * Fills undefined (null) values of a Series with respect to the base index.
    *
    * @param value
    *   Value for filling undefined values.
    * @return
    *   Series with undefined values filled. The index is expanded to the uniform base index.
    * @since 0.1.0
    */
  def fillAll(value: T): Series[T] =
    requireTypeMatch(value)
    ops.fillAll[T](dense, value)

  /**
    * Returns the index position of the first defined (non-null) value and for boolean Series the first occurrence of
    * the value `true`.
    *
    * @return
    *   Index of first non-null entry or None if none of the entries are defined. For boolean Series, the index of the
    *   first occurrence of `true` or None if all values are `false`.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def first: Option[Int] =
    if isBoolean then ops.firstB(this.asInstanceOf[Series[Boolean]], true) else ops.firstIndex(this)

  /**
    * Returns the index position of the first element where the condition is satisfied or None if not found.
    *
    * @param f
    *   Predicate function.
    * @return
    *   Index position or None if the predicate is never true.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def first(f: T => Boolean): Option[Int] = ops.find[T](this, f)

  /**
    * Returns the index position of the first occurrence of a value or None if not found.
    *
    * @param value
    *   Value to search for.
    * @return
    *   Index position or None if not found.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def first(value: T): Option[Int] = ops.first[T](this, value)

  /**
    * Returns the value at the index position, where a value occurs the first time in another Series or None if the
    * value is not found.
    *
    * @param series
    *   Series which is searched for the value.
    * @param value
    *   Value to search for.
    * @return
    *   Option of the value at the index position.
    * @throws BaseIndexException
    *   If the base indices are not equal.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def first[T2](series: Series[T2], value: T2): Option[T] =
    if !index.hasSameBase(series.index) then throw BaseIndexException(this, series)
    else apply(series.first(value))

  /**
    * Returns the value at the index position, where a value occurs the first time in another Series or `default` if the
    * value is not found.
    *
    * @param series
    *   Series which is searched for the value.
    * @param value
    *   Value to search for.
    * @param default
    *   value if value is not found.
    * @return
    *   Value a the index position or the `default` value if not found.
    * @throws BaseIndexException
    *   If the base indices are not equal.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def first[T2](series: Series[T2], value: T2, default: => T): T =
    if !index.hasSameBase(series.index) then throw BaseIndexException(this, series)
    else apply(series.first(value), default)

  /**
    * Tests whether the condition holds for all elements.
    *
    * @param f
    *   Predicate function to satisfy.
    * @return
    *   False if at least one (non-null) element does not satisfy the condition, true otherwise.
    * @see
    *   [[forallStrictly]]
    * @since 0.1.0
    */
  def forall(f: T => Boolean): Boolean = ops.find(this, x => !f(x)) match
    case Some(_) => false
    case None    => true

  /**
    * Tests whether the condition holds for all elements.
    *
    * @param f
    *   Predicate function to satisfy.
    * @return
    *   False if at least one element does not satisfy the condition or is not set (null), true otherwise.
    * @see
    *   [[forall]]
    * @since 0.1.0
    */
  def forallStrictly(f: T => Boolean): Boolean = !hasUndefined && forall(f)

  /**
    * Returns the value at index `ix`. Throws an exception if the value is not set or not in the index.
    *
    * @param ix
    *   Row index.
    * @return
    *   Value for the index.
    * @throws NoSuchElementException
    *   If he value is not set (null), `ix` is not in the index or if `ix` is not part of the base index.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  inline def get(ix: Int): T =
    try
      if index.contains(ix) then
        if data.getMask(ix) then data.get(ix)
        else throw new NoSuchElementException(s"No value for index position $ix.")
      else throw new NoSuchElementException(s"Invalid index position $ix.")
    catch case _ => throw new NoSuchElementException(s"Invalid index position $ix.")

  /**
    * Returns the value at index `ix`. Throws an exception if the value is not defined or `ix` not in the index.
    *
    * @param ix
    *   Option of row index.
    * @return
    *   Value for the index.
    * @throws NoSuchElementException
    *   If he value is undefined (null), `ix` is not in the index, `ix` is None or if `ix` is not part of the base
    *   index.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  inline def get(ix: Option[Int]): T =
    try
      if ix.isDefined && index.contains(ix.get) then
        val i = ix.get
        if data.getMask(i) then data.get(i)
        else throw new NoSuchElementException(s"No value for index position $i.")
      else throw new NoSuchElementException(s"Invalid index position $ix.")
    catch case _ => throw new NoSuchElementException(s"Invalid index position $ix.")

  /**
    * Hash code.
    *
    * @return
    *   Hash code derived from index length and name.
    * @since 0.1.0
    */
  override def hashCode: Int = 31 * name.## + index.length.##

  /**
    * Indicates if two Series have the same defined values as well as the same undefined (null) values, where the values
    * themselves are not compared.
    *
    * @param series
    *   Series to compare with.
    * @return
    *   True if the undefined and defined values are the same for both Series.
    * @note
    *   - The method does not differentiate between a missing value in the index and a undefined value (null).
    *   - The comparison ignores different names and indices.
    *   - If the two Series have different base indices the result is false.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  def hasSameDefined[T2](series: Series[T2]): Boolean =
    index.hasSameBase(series.index) && ops.compareNulls(this, series)

  /**
    * Compares if the indices of two Series are equal.
    *
    * @param series
    *   Series to compare with.
    * @return
    *   True if indices are equal and false otherwise.
    * @see
    *   [[https://pan-data.org/scala/advanced/indices.html]]
    * @since 0.1.0
    */
  def hasSameIndex[T2](series: Series[T2]): Boolean = index.equals(series.index)

  /**
    * Returns true if at least one of the elements is undefined or the index has less elements than the base index
    * (slicing of the Series).
    *
    * @return
    *   False if an element is defined for each base index element or otherwise true.
    * @see
    *   [[isDefined]] for negation.
    * @since 0.1.0
    */
  def hasUndefined: Boolean = !isDefined

  /**
    * Returns the first value as an Option, i.e. `Some(value)` or `None` if the first value is undefined.
    *
    * @return
    *   Value as an Option.
    * @note
    *   The result depends on the index order.
    * @since 0.1.0
    */
  def headOption: Option[T] = if index.isEmpty then None else apply(index.head(1).toSeqIndex.indices(0))

  /**
    * Returns the first (defined) value as an Option, i.e. `Some(value)` or `None` if all values are undefined.
    *
    * @return
    *   Value as an Option, which is `None` if the Series an no defined values.
    * @note
    *   The result depends on the index order.
    * @since 0.1.0
    */
  def headValue: Option[T] = apply(ops.firstIndex(this))

  /**
    * Indicates if the index is empty.
    *
    * @return
    *   True if the index has at least one element or otherwise false.
    * @since 0.1.0
    */
  def indexEmpty: Boolean = index.isEmpty

  /**
    * Iterator over the index.
    *
    * @return
    *   Iterator of index positions.
    * @note
    *   For better performance consider using methods such as [[agg]] or [[map]].
    * @since 0.1.0
    */
  def indexIterator: Iterator[Int] = index.iterator

  /**
    * Indicates if the index is not empty.
    *
    * @return
    *   True if the index has no elements or otherwise false.
    * @since 0.1.0
    */
  def indexNonEmpty: Boolean = index.nonEmpty

  /**
    * Information string on the Series.
    *
    * @return
    *   Info string.
    * @since 0.1.0
    */
  def info: String =
    s"""Series '$name':
         |  Series with ${asElements(index.length)}.
         |  ${data.describe}
         |  The Series has a ${index.describe}
         |""".stripMargin('|')

  /**
    * Returns true if the data does not contains undefined values and the index includes all index positions of the base
    * index.
    *
    * @return
    *   Boolean if elements are defined for all index positions.
    * @see
    *   [[hasUndefined]] for negation.
    * @since 0.1.0
    */
  def isDefined: Boolean = !data.containsNull && index.isBijective

  /**
    * Indicates if a Series has no defined elements.
    *
    * @return
    *   True if either the index is empty or all values are undefined. Otherwise false is returned.
    * @since 0.1.0
    */
  def isEmpty: Boolean = index.isEmpty || ops.firstIndex(this).isEmpty

  /**
    * Checks if the Series is of type `T2`.
    *
    * @return
    *   True if values are instance of `T2` or false otherwise.
    * @note
    *   For better performance, the type check is only performed on the [[head]] of the Series except for the types
    *   Boolean, Double, Int and String. Checking the Series with regards to other types on a Series with mixed data can
    *   have misleading results. Use [[isTypeStrictly]] for testing.
    * @since 0.1.0
    */
  def isType[T2: Typeable]: Boolean =

    true match
      case _: T2 =>
        1 match
          case _: T2 => return true // must be of type Any (no TypeTest for AnyRef and AnyVal)
          case _     => return isBoolean
      case _ =>

    1.0 match
      case _: T2 => return isDouble
      case _     =>

    1 match
      case _: T2 => return isInt
      case _     =>

    "" match
      case _: T2 => return isString
      case _     =>

    headOption match
      case Some(_: T2) => true
      case _           => false

  /**
    * Checks if the Series is of type `T2` and all values are defined.
    *
    * @return
    *   True if values are instance of `T2` and all values are defined. False otherwise.
    * @note
    *   For better performance, the type check is only performed on the [[head]] of the Series except for the types
    *   Boolean, Double, Int and String. Checking the Series with regards to other types on a Series with mixed data can
    *   have misleading results. Use [[isTypeStrictly]] for testing.
    * @since 0.1.0
    */
  def isTypeAll[T2: Typeable]: Boolean = isDefined && isType[T2]

  /**
    * Checks if the Series is of type `T2` and all values are defined.
    *
    * @return
    *   True if every value is instance of `T2` and all values are defined. False otherwise.
    * @note
    *   The type check is performed on all values with causes a slower performance except for the types Any, Boolean,
    *   Double, Int and String.
    * @since 0.1.0
    */
  def isTypeAllStrictly[T2: Typeable]: Boolean = isDefined && isTypeStrictly[T2]

  /**
    * Checks if the Series is of type `T2`.
    *
    * @return
    *   True if every value is instance of `T2` or false otherwise.
    * @note
    *   The type check is performed on all values with causes a slower performance except for the types Any, Boolean,
    *   Double, Int and String.
    * @since 0.1.0
    */
  def isTypeStrictly[T2: Typeable]: Boolean =

    true match
      case _: T2 =>
        1 match
          case _: T2 => return true // must be of type Any (no TypeTest for AnyRef and AnyVal)
          case _     => return isBoolean
      case _ =>

    1.0 match
      case _: T2 => return isDouble
      case _     =>

    1 match
      case _: T2 => return isInt
      case _     =>

    "" match
      case _: T2 => return isString
      case _     =>

    forall(_ match
      case _: T2 => true
      case _     => false
    )

  /**
    * Iterator over the values of the Series ordered by the index.
    *
    * @return
    *   Iterator of type `Option[T]` returning `Some(value)` if the value is defined and `None` if undefined.
    * @note
    *   For better performance consider using methods such as [[agg]] or [[map]].
    * @since 0.1.0
    */
  def iterator: Iterator[Option[T]] = index.iterator.map(data(_))

  /**
    * Returns the index position of the last set (non-null) value and for boolean Series the last occurrence of the
    * value `true`.
    *
    * @return
    *   Index of last non-null entry or None if none of the entries are set. For boolean Series, the index of the last
    *   occurrence of `true` or None if all values are `false`.
    * @note
    *   The result depends on the index order.
    * @see
    *   https://pan-data.org/scala/basics/extracting-values.html
    * @since 0.1.0
    */
  def last: Option[Int] = if isBoolean then ops.lastB(this.asInstanceOf[Series[Boolean]], true) else ops.lastIndex(this)

  /**
    * Returns the index position of the last occurrence of a value or None if not found.
    *
    * @param value
    *   Value to search for.
    * @return
    *   Index position or None if not found.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def last(value: T): Option[Int] = ops.last[T](this, value)

  /**
    * Returns the value at the index position, where a value occurs the last time in another Series or None if the value
    * is not found.
    *
    * @param series
    *   Series which is searched for the value.
    * @param value
    *   Value to search for.
    * @return
    *   Option of the value at the index position.
    * @throws BaseIndexException
    *   If the base indices are not equal.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def last[T2](series: Series[T2], value: T2): Option[T] =
    if !index.hasSameBase(series.index) then throw BaseIndexException(this, series)
    else apply(series.last(value))

  /**
    * Returns the value at the index position, where a value occurs the last time in another Series or `default` if the
    * value is not found.
    *
    * @param series
    *   Series which is searched for the value.
    * @param value
    *   Value to search for.
    * @param default
    *   value if value is not found.
    * @return
    *   Value a the index position or the `default` value if not found.
    * @throws BaseIndexException
    *   If the base indices are not equal.
    * @note
    *   The result depends on the index order.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def last[T2](series: Series[T2], value: T2, default: => T): T =
    if !index.hasSameBase(series.index) then throw BaseIndexException(this, series)
    else apply(series.last(value), default)

  /**
    * Number of rows.
    *
    * @return
    *   Length of the Series, i.e. number of elements in the index.
    * @see
    *   [[numRows]]
    * @since 0.1.0
    */
  def length: Int = index.length

  /**
    * Applies a function on each element of the Series.
    *
    * @param f
    *   Row-wise function.
    * @return
    *   Transformed Series of type `R`.
    * @see
    *   [[https://pan-data.org/scala/basics/custom-series-operations.html]]
    * @since 0.1.0
    */
  def map[R: ClassTag](f: T => R): Series[R] = ops.map(this, f)

  /**
    * Applies a row-wise function on two Series.
    *
    * @param series
    *   Second Series.
    * @param f
    *   Row-wise function which takes as arguments elements of both Series. If one of the elements in a row is undefined
    *   the result in undefined.
    * @return
    *   Transformed Series of type `R`.
    * @since 0.1.0
    */
  def map[T2, R: ClassTag](series: Series[T2], f: (T, T2) => R): Series[R] = ops.map(this, series, f)

  /**
    * A mutable copy of the Series.
    *
    * @return
    *   A [[MutableSeries]].
    * @note
    *   - For creating a [[MutableSeries]] from values preferably use {{{Series.mutable(1,2,3)}}} instead of
    *     {{{Series(1,2,3).mutable}}} for performance reasons.
    *   - For reference types the copy references the same objects which may cause side effects for mutable objects.
    * @since 0.1.0
    */
  def mutable: MutableSeries[T] = new MutableSeries[T](new Series[T](data.clone, index, name))

  /**
    * Indicates if a Series has defined elements.
    *
    * @return
    *   True if at least one defined element exists. Otherwise false is returned.
    * @since 0.1.0
    */
  def nonEmpty: Boolean = !isEmpty

  /**
    * Number of rows.
    *
    * @return
    *   Length of the Series, i.e. number of elements in the index.
    * @see
    *   [[length]]
    * @since 0.1.0
    */
  def numRows: Int = index.length

  /**
    * Number of rows of the underlying data vector.
    *
    * @return
    *   Number of elements in the base index.
    * @since 0.1.0
    */
  def numRowsBase: Int = index.base.length

  /**
    * Replaces undefined (null) values by the values of `series` for the current index. Synonym for [[fill]].
    *
    * @param series
    *   Series with the same base index.
    * @return
    *   Series with data from the first series or if undefined from the second. The original index is not altered.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @see
    *   [[fill]]
    * @since 0.1.0
    */
  def orElse[T2 <: T](series: Series[T2]): Series[T] = fill(series)

  /**
    * Fills undefined (null) values of a Series with respect to the current index. Synonym for [[fill]].
    *
    * @param value
    *   Value for filling non-set values.
    * @return
    *   Series with undefined values filled. The original index is preserved.
    * @see
    *   [[fill]]
    * @since 0.1.0
    */
  def orElse(value: T): Series[T] = fill(value)

  /**
    * Resets the index to a `UniformIndex` with index positions 0 to `numRows - 1` while keeping the order of the
    * elements. If the current index is not a uniform index, the data is copied into a new vector with the order of the
    * current index.
    *
    * @return
    *   Series with a `UniformIndex`.
    * @see
    *   [[sortIndex]] for sorting the index by index positions.
    * @since 0.1.0
    */
  def resetIndex: Series[T] =
    if index.isInstanceOf[UniformIndex] then this else extract(index.toSeqIndex.indices)

  /**
    * Prints the Series as a table.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param annotateIndex
    *   If true, the an index column is displayed.
    * @param annotateType
    *   If true, the type for each column in displayed.
    * @param colWidth
    *   The width of the column.
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
    * @since 0.1.0
    */
  def show(
      n: Int = Settings.printRowLength,
      annotateIndex: Boolean = false,
      annotateType: Boolean = false,
      colWidth: Int = Settings.printColWidthSeries,
  ): Unit = println(
    toString(n, colWidth = colWidth, annotateIndex = annotateIndex, annotateType = annotateType)
  )

  /**
    * Prints all values of the Series.
    *
    * @param n
    *   The maximal numbers of rows to be printed or -1 (default) for all values.
    * @see
    *   [[https://pan-data.org/scala/basics/working-with-series.html]]
    * @since 0.1.0
    */
  def showValues(
      n: Int = -1
  ): Unit =
    require(n >= -1)
    val len = if n == -1 then index.length else n
    println(index.overWithIndex(data).take(len).map(t => s"${t._2.getOrElse("null").toString}").mkString("\n"))

  /**
    * Sorts the Series in ascending order.
    *
    * @param ordering
    *   Implicit ordering that must exist for the type `T`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted(implicit ordering: Ordering[T]): Series[T] = sorted(Order.asc)

  /**
    * Sorts the Series.
    *
    * @param order
    *   Order for sorting. Possible values are [[Order.asc]], [[Order.desc]], [[Order.ascNullsFirst]] and
    *   [[Order.descNullsFirst]].
    * @param ordering
    *   Implicit ordering that must exist for the type `T`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted(order: Order)(implicit ordering: Ordering[T]): Series[T] =
    if isString && ordering.isInstanceOf[StringOrdering] then
      asInstanceOf[Series[String]].sort(order)(SeriesString.DefaultOrder).asInstanceOf[Series[T]]
    else sort(order)

  /**
    * Converts the Series row-wise into a String Series.
    *
    * @return
    *   Series of type String.
    * @note
    *   - The operation maps each element via `toString` to a String.
    *   - If the Series is of type String the Series is returned.
    * @since 0.1.0
    */
  def str: Series[String] =
    if isString then this.asInstanceOf[Series[String]]
    else map(_.toString)

  /**
    * Copies the Series into an array.
    *
    * @return
    *   Array of type `Option[T]` with [[numRows]] elements.
    * @since 0.1.0
    */
  def toArray: Array[Option[T]] = iterator.toArray

  /**
    * Copies the Series into an array.
    *
    * @return
    *   Array of type `T`. If `T` is a native type, a native array is returned.
    * @since 0.1.0
    */
  def toFlatArray: Array[T] = defined.resetIndexOrCopy.data.vector

  /**
    * Copies the Series into a List.
    *
    * @return
    *   List of type `T`.
    * @since 0.1.0
    */
  def toFlatList: List[T] = toFlatArray.toList

  /**
    * Copies the Series into a sequence.
    *
    * @return
    *   Sequence of type `T`.
    * @since 0.1.0
    */
  def toFlatSeq: Seq[T] = toFlatArray.toSeq

  /**
    * Copies the Series into a List.
    *
    * @return
    *   List of type `Option[T]` with [[numRows]] elements.
    * @since 0.1.0
    */
  def toList: List[Option[T]] = iterator.toList

  /**
    * Copies the Series into a sequence with [[numRows]] elements.
    *
    * @return
    *   Sequence of type `Option[T]`.
    * @since 0.1.0
    */
  def toSeq: Seq[Option[T]] = iterator.toSeq

  /**
    * Renders the Series as a table using default parameters.
    *
    * @return
    *   Formatted table.
    * @since 0.1.0
    */
  override def toString: String = "\n" + toString()

  /**
    * Renders the Series as a table.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param colWidth
    *   The width of each column.
    * @param annotateIndex
    *   If true, the an index column is displayed.
    * @param annotateType
    *   If true, the type for each column in displayed.
    * @return
    *   Formatted table.
    * @since 0.1.0
    */
  def toString(
      n: Int = Settings.printRowLength,
      colWidth: Int = Settings.printColWidthSeries,
      annotateIndex: Boolean = true,
      annotateType: Boolean = true,
  ): String = DataFrame
    .fromSeries(if name.isEmpty then as("Series") else this)
    .toString(n, 0, annotateIndex, annotateType, colWidth)

  /**
    * Returns the (internal) runtime Scala type of the Series.
    *
    * @return
    *   Type name such as "Int", "MyClass", "Any", "Array[Double]", "List", "Seq", etc.
    * @note
    *   - There is no deep type reflection for 2nd order types except for `Array`.
    *   - When casting a Series to Any (via [[Series.asAny]]), the internal type of the Series does not change.
    * @since 0.1.0
    */
  def typeString: String = data.typeString()

  /**
    * Appends row-wise one or multiple Series. The method materializes all indices into a uniform index.
    *
    * @param series
    *   Series to be appended.
    * @return
    *   Series with a uniform index.
    * @throws SeriesCastException
    *   If the underlying types are not a subtype.
    * @since 0.1.0
    */
  def union[T2 <: T](series: Series[T2]*): Series[T] =
    if series.isEmpty then this.resetIndex
    else
      try ops.union(this +: series.map(_.asInstanceOf[Series[T]]).toArray)
      catch
        case _: ClassCastException | _: ArrayStoreException =>
          throw SeriesCastException(this, series)

  /**
    * Array with unique (defined) values in the Series.
    *
    * @return
    *   Array with distinct values.
    * @since 0.1.0
    */
  def unique: Array[T] = defined.resetIndex.data.vector.distinct

  /**
    * Updates the series with values from the second series if the values are defined.
    *
    * @param series
    *   Series with the same base index.
    * @return
    *   Series with data from parameter `series` or if a value is undefined in `series` from the original series. The
    *   original index is not altered.
    * @since 0.1.0
    */
  def update[T2 <: T](series: Series[T2]): Series[T] =
    requireTypeMatch(series)
    ops.update(series.asInstanceOf[Series[T]], this)

  // *** PRIVATE ***

  /**
    * Indicates if an object can equal the series.
    *
    * @param a
    *   Object.
    * @return
    *   True if the object if a Series and false otherwise.
    * @since 0.1.0
    */
  private[pd] def canEqual(a: Any): Boolean = a.isInstanceOf[Series[?]] || a.isInstanceOf[MutableSeries[?]]

  /**
    * Extracts (copies) elements of a Series to a new Series with a UniformIndex.
    *
    * @param indices
    *   Array of indices to extract. The indices are neither checked with regards to boundaries nor require
    *   distinctness.
    * @note
    *   The index positions must be part of the current index and are not checked with regards to validity.
    * @return
    *   New Series with a UniformIndex and as many elements as the length of `indices`.
    * @since 0.1.0
    */
  private[pd] def extract(indices: Array[Int]): Series[T] = ops.extract[T](this, indices)

  /**
    * True if the Series data type is Boolean.
    *
    * @return
    *   True if the Series data type is Boolean.
    * @since 0.1.0
    */
  private[pd] inline def isBoolean: Boolean = data.vector.isInstanceOf[Array[Boolean]]

  /**
    * True if the Series data type is Double.
    *
    * @return
    *   True if the Series data type is Double.
    * @since 0.1.0
    */
  private[pd] inline def isDouble: Boolean = data.vector.isInstanceOf[Array[Double]]

  /**
    * True if the Series data type is Int.
    *
    * @return
    *   True if the Series data type is Int.
    * @since 0.1.0
    */
  private[pd] inline def isInt: Boolean = data.vector.isInstanceOf[Array[Int]]

  /**
    * True if the Series data type is String.
    *
    * @return
    *   True if the Series data type is String.
    * @since 0.1.0
    */
  private[pd] inline def isString: Boolean = data.vector.isInstanceOf[Array[String]]

  private[pd] def javaApply(ix: Integer, default: T): T = if ix == null then default else apply(ix, default)

  private[pd] def javaApply(ix: Option[Integer], default: T): T = if ix.isEmpty then default else apply(ix.get, default)

  private[pd] def javaGet(ix: Integer): T | Null =
    if ix == null then null
    else if index.contains(ix) then
      if data.getMask(ix) then data.get(ix)
      else null
    else null

  private[pd] def javaMapToT(f: T => T): Series[T] = ops.map(this, f)(ClassTag(data.vectorClass))

  private[pd] def javaMapToT(series: Series[T], f: (T, T) => T): Series[T] =
    ops.map(this, series, f)(ClassTag(data.vectorClass))

  /**
    * SeriesOps object for processing data. Multi-threading support is added for more than [[Settings.minThreadedRows]]
    * rows.
    *
    * @return
    *   SeriesOps.
    * @since 0.1.0
    */
  private[pd] inline def ops: SeriesOps =
    if Settings.threaded && numRows >= Settings.minThreadedRows then Series.opsThreaded else Series.opsSingle

  /**
    * Throws an exception if the type of `series` is not a (sub) type of `this`.
    *
    * @param series
    *   Series to be checked.
    * @throws SeriesCastException
    *   If the type of `series` is not a (sub) type.
    * @since 0.1.0
    */
  private[pd] def requireTypeMatch[T2](series: Series[T2]): Unit =
    if !typeMatch(series) then throw SeriesCastException(series, typeString)

  /**
    * Throws an exception if the `value` is not a (sub) type of the Series' type.
    *
    * @param value
    *   Value to be checked.
    * @throws ValueCastException
    *   If `value` is not a (sub) type.
    * @since 0.1.0
    */
  private[pd] def requireTypeMatch[T2](value: T2): Unit =
    if !typeMatch(value) then throw ValueCastException(value, typeString)

  /**
    * Resets the index to a [[UniformIndex]] with index positions 0 to `numRows - 1` while keeping the order of the
    * elements. The data is copied into a new vector with the order of the current index.
    *
    * @return
    *   Series with a [[UniformIndex]].
    * @since 0.1.0
    */
  private[pd] def resetIndexOrCopy: Series[T] =
    if index.isInstanceOf[UniformIndex] then new Series[T](data.clone, index, name)
    else extract(index.toSeqIndex.indices)

  /**
    * Name with prefix "Series ".
    *
    * @return
    *   Prefixed name of Series.
    * @since 0.1.0
    */
  private[pd] def seriesName: String = "Series" + (if name.isEmpty then "" else s" $name")

  /**
    * Sorts the Series.
    *
    * @param order
    *   Order for sorting.
    * @param ordering
    *   Implicit ordering that must exist for the type `T`.
    * @return
    *   Series with sorted index.
    * @since 0.1.0
    */
  private[pd] def sort(order: Order)(implicit ordering: Ordering[T]): Series[T] =
    withIndex(index.sorted[T](data -> order))

  /**
    * Determines if the type of `series` equals the type of `this`.
    *
    * @param series
    *   Series to be checked.
    * @return
    *   True if both types are the same and false otherwise.
    * @since 0.1.0
    */
  private[pd] def typeEquals[T2](series: Series[T2]): Boolean =
    val classThis = data.vectorClass
    val classSeries = series.data.vectorClass
    classThis == classSeries

  /**
    * Determines if the type of `series` is a (sub) type of `this`.
    *
    * @param series
    *   Series to be checked.
    * @return
    *   True if (sub) type and false otherwise.
    * @since 0.1.0
    */
  private[pd] def typeMatch[T2](series: Series[T2]): Boolean =
    val classThis = data.vectorClass
    val classSeries = series.data.vectorClass
    classThis == classSeries || classThis.isAssignableFrom(classSeries)

  /**
    * Determines if the `value` is a (sub) type of the Series' type.
    *
    * @param value
    *   Value to be checked.
    * @return
    *   True if (sub) type and false otherwise.
    * @since 0.1.0
    */
  private[pd] def typeMatch[T2](value: T2): Boolean =
    value match
      case _: Boolean => isBoolean
      case _: Byte    => data.vector.isInstanceOf[Array[Byte]]
      case _: Char    => data.vector.isInstanceOf[Array[Char]]
      case _: Double  => isDouble
      case _: Float   => data.vector.isInstanceOf[Array[Float]]
      case _: Int     => isInt
      case _: Long    => data.vector.isInstanceOf[Array[Long]]
      case _: Short   => data.vector.isInstanceOf[Array[Short]]
      case _ =>
        val classThis = data.vectorClass
        val classValue = if value == null then null else value.getClass
        classThis == classValue || classThis.isAssignableFrom(classValue)

  /**
    * Undefined index positions with respect to the current index.
    *
    * @return
    *   Array with undefined index positions in preserved order.
    * @since 0.1.0
    */
  private[pd] def undefinedIndices: Array[Int] =
    if isDefined then Array[Int]()
    else
      // noinspection ConvertibleToMethodValue
      index.iterator.filterNot(data.getMask(_)).toArray

  /**
    * Creates a new instance of the Series with the index `index` implementing the [[IndexOps]] interface.
    *
    * @param index
    *   New index.
    * @return
    *   New instance of the Series.
    * @since 0.1.0
    */
  private[pd] def withIndex(index: BaseIndex): Series[T] = new Series[T](data, index, name)

/**
  * @see
  *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
  * @since 0.1.0
  */
object Series:

  private[pd] var opsSingle: SeriesOps = OpsSingle()
  private[pd] var opsThreaded: SeriesOps = OpsThreaded()

  // *** SERIES IMPLICITS ***

  implicit def toSeriesAny(s: Series[Any]): SeriesAny = SeriesAny(s.data, s.index, s.name)
  implicit def toSeriesBoolean(s: Series[Boolean]): SeriesBoolean = SeriesBoolean(s.data, s.index, s.name)
  implicit def toSeriesDouble(s: Series[Double]): SeriesDouble = SeriesDouble(s.data, s.index, s.name)
  implicit def toSeriesInt(s: Series[Int]): SeriesInt = SeriesInt(s.data, s.index, s.name)
  implicit def toSeriesLocalDate(s: Series[LocalDate]): SeriesLocalDate = SeriesLocalDate(s.data, s.index, s.name)
  implicit def toSeriesLocalDateTime(s: Series[LocalDateTime]): SeriesLocalDateTime =
    SeriesLocalDateTime(s.data, s.index, s.name)
  implicit def toSeriesLocalTime(s: Series[LocalTime]): SeriesLocalTime = SeriesLocalTime(s.data, s.index, s.name)
  implicit def toSeriesString(s: Series[String]): SeriesString = SeriesString(s.data, s.index, s.name)
  implicit def toSeriesZonedDateTime(s: Series[ZonedDateTime]): SeriesZonedDateTime =
    SeriesZonedDateTime(s.data, s.index, s.name)

  // *** MUTABLE SERIES IMPLICITS ***

  implicit def toSeriesAny(s: MutableSeries[Any]): SeriesAny = s.copy
  implicit def toSeriesBoolean(s: MutableSeries[Boolean]): SeriesBoolean = s.copy
  implicit def toSeriesDouble(s: MutableSeries[Double]): SeriesDouble = s.copy
  implicit def toSeriesInt(s: MutableSeries[Int]): SeriesInt = s.copy
  implicit def toSeriesLocalDate(s: MutableSeries[LocalDate]): SeriesLocalDate = s.copy
  implicit def toSeriesLocalDateTime(s: MutableSeries[LocalDateTime]): SeriesLocalDateTime = s.copy
  implicit def toSeriesLocalTime(s: MutableSeries[LocalTime]): SeriesLocalTime = s.copy
  implicit def toSeriesString(s: MutableSeries[String]): SeriesString = s.copy
  implicit def toSeriesZonedDateTime(s: MutableSeries[ZonedDateTime]): SeriesZonedDateTime = s.copy

  // *** SCALAR IMPLICITS ***

  implicit inline def toSeriesAnyPrefix(v: Double): SeriesAny.Prefix = SeriesAny.Prefix(v)
  implicit inline def toSeriesAnyPrefixInt(v: Int): SeriesAny.PrefixInt = SeriesAny.PrefixInt(v)
  implicit inline def toSeriesDoublePrefix(v: Double): SeriesDouble.Prefix = SeriesDouble.Prefix(v)
  implicit inline def toSeriesDoublePrefixInt(v: Int): SeriesDouble.PrefixInt = SeriesDouble.PrefixInt(v)
  implicit inline def toSeriesIntPrefix(v: Int): SeriesInt.Prefix = SeriesInt.Prefix(v)
  implicit inline def toSeriesIntPrefixDouble(v: Double): SeriesInt.PrefixDouble = SeriesInt.PrefixDouble(v)

  implicit def arrayToSeries[T: ClassTag](collection: Array[T]): Series[T] = Series(collection*)
  implicit def seqToSeries[T: ClassTag](collection: Seq[T]): Series[T] = Series(collection*)

  /**
    * Creates a Series from values. Undefined values are symbolized by `null`.
    *
    * @param values
    *   Values of the Series.
    * @return
    *   Created Series.
    * @note
    *   - The Series is `null` safe, i.e. `null` values are not stored but represented internally by a mask.
    *   - The type of, e.g., `Series(1, null, 3)` is of type Series[Int] and is implemented as a primitive type.
    *   - Automatic type casting in the argument list works only without `null` values, i.e. Series(1, 1.1) and
    *     Series(1, null, 1.1) is of type Series[Double] whereas Series(1, null ,1.1) is of type Series[Any].
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
    * @since 0.1.0
    */
  def apply[T: ClassTag](values: (T | Null)*): Series[T] =
    new Series[T](data = SeriesData.fromCollectionWithNull(values), UniformIndex(values.length))

  /**
    * Creates a Series with a name, where a second parameter takes the values (see [[SeriesBuilder.apply]]).
    *
    * @param name
    *   Name of the Series.
    * @return
    *   SeriesBuilder which takes the values.
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
    * @since 0.1.0
    */
  def apply(name: String): SeriesBuilder = SeriesBuilder(name)

  /**
    * Creates an empty Series of type `T`.
    *
    * @return
    *   Empty Series.
    * @since 0.1.0
    */
  def empty[T: ClassTag]: Series[T] = new Series[T](SeriesData.empty[T], UniformIndex(0))

  /**
    * Creates an empty Series of type `T`.
    *
    * @param length
    *   Length of the Series.
    * @return
    *   Empty Series.
    * @since 0.1.0
    */
  def empty[T: ClassTag](length: Int): Series[T] =
    if length >= 0 then new Series[T](SeriesData.empty[T](length), UniformIndex(length))
    else throw IllegalIndex(s"Index length cannot be negative (found $length).")

  /**
    * Creates an Series of length `length` with the given value.
    *
    * @param length
    *   Length of the Series.
    * @param value
    *   Value for all elements.
    * @return
    *   New Series.
    * @since 0.1.0
    */
  def fill[T: ClassTag](length: Int)(value: T): Series[T] =
    if length >= 0 then new Series[T](SeriesData(Array.fill[T](length)(value)), UniformIndex(length))
    else throw IllegalIndex(s"Index length cannot be negative (found $length).")

  /**
    * Creates a Series from a collection of data.
    *
    * @param data
    *   Collection.
    * @return
    *   Created Series.
    * @since 0.1.0
    */
  def from[T](data: Array[T]): Series[T] =
    new Series[T](data = SeriesData.fromArray(data), UniformIndex(data.length))

  /**
    * Creates a Series from a collection of data.
    *
    * @param data
    *   Collection.
    * @return
    *   Created Series.
    * @since 0.1.0
    */
  def from[T: ClassTag](data: collection.mutable.Buffer[T]): Series[T] =
    new Series[T](data = SeriesData.fromCollection(data), UniformIndex(data.length))

  /**
    * Creates a Series from a collection of data.
    *
    * @param data
    *   Collection.
    * @return
    *   Created Series.
    * @since 0.1.0
    */
  def from[T: ClassTag](data: Seq[T]): Series[T] =
    new Series[T](data = SeriesData.fromCollection(data), UniformIndex(data.length))

  /**
    * Creates a Series from a collection of Option data, where the Option is unpacked.
    *
    * @param data
    *   Collection of type `Option[T]`.
    * @return
    *   Created Series of type `T`.
    * @since 0.1.0
    */
  @targetName("fromOption")
  def from[T: ClassTag](data: Array[Option[T]]): Series[T] =
    new Series[T](data = SeriesData.fromOptionCollection(data), UniformIndex(data.length))

  /**
    * Creates a Series from a collection of Option data, where the Option is unpacked.
    *
    * @param data
    *   Collection of type `Option[T]`.
    * @return
    *   Created Series of `type T`.
    * @since 0.1.0
    */
  @targetName("fromOption")
  def from[T: ClassTag](data: Seq[Option[T]]): Series[T] =
    new Series[T](data = SeriesData.fromOptionCollection(data), UniformIndex(data.length))

  /**
    * Creates a mutable Series (see [[MutableSeriesBuilder]]).
    *
    * @return
    *   [[MutableSeriesBuilder]].
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]] [[https://pan-data.org/scala/basics/mutable.html]]
    * @since 0.1.0
    */
  def mutable: MutableSeriesBuilder = MutableSeriesBuilder()

  /**
    * Creates a mutable Series from values. Undefined values are symbolized by `null`.
    *
    * @param values
    *   Values of the Series.
    * @return
    *   Created [[MutableSeries]].
    * @note
    *   - The Series is `null` safe, i.e. `null` values are not stored but represented internally by a mask.
    *   - The type of, e.g., `Series(1, null, 3)` is of type Series[Int] and is implemented as a primitive type.
    *   - Automatic type casting in the argument list works only without `null` values, i.e. Series(1, 1.1) and
    *     Series(1, null, 1.1) is of type Series[Double] whereas Series(1, null ,1.1) is of type Series[Any].
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-series.html]] [[https://pan-data.org/scala/basics/mutable.html]]
    * @since 0.1.0
    */
  def mutable[T: ClassTag](values: (T | Null)*): MutableSeries[T] = new MutableSeries(apply(values*))

  /**
    * Appends row-wise one or multiple series to the first one. The method materializes all indices into a uniform
    * index.
    *
    * @param series
    *   Series to be appended.
    * @return
    *   Series with a uniform index.
    * @throws IllegalOperation
    *   If no series is passed.
    * @throws SeriesCastException
    *   If the underlying types are not a subtype.
    * @since 0.1.0
    */
  def union[T](series: Series[T]*): Series[T] =
    if series.isEmpty then throw IllegalOperation("Union expects at least one element.")
    else if series.length == 1 then series.head.resetIndex
    else
      try series.head.ops.union(series.toArray)
      catch
        case _: ClassCastException | _: ArrayStoreException =>
          throw SeriesCastException(series.head, series.tail)

  // *** PRIVATE ***

  /**
    * Creates a Series from a SeriesData object.
    *
    * @param data
    *   SeriesData.
    * @param index
    *   Index.
    * @param name
    *   Name of Series.
    * @return
    *   New Series.
    * @since 0.1.0
    */
  private[pd] def apply[T](data: SeriesData[T], index: BaseIndex, name: String): Series[T] =
    new Series(data, index, name)

  /**
    * Extends the name for numerical binary operations.
    *
    * @param s
    *   Series.
    * @param op
    *   Operation as single character.
    * @param s2
    *   Second series.
    * @return
    *   Renamed Series.
    * @since 0.1.0
    */
  private[pd] def ext[T](s: Series[T], op: Char, s2: Series[?]): Series[T] =
    if s2.name.isEmpty then s else s.as(s"${s.name}$op${s2.name}")

  /**
    * Wraps the array into a Series without copying.
    *
    * Warning: Ensure that the array is not used after wrapping. Otherwise no guarantees can be given with regards to
    * correct functionality, null safety, etc.
    *
    * @param array
    *   Array to be wrapped by a mutable Series.
    * @return
    *   [[Series]].
    * @note
    *   Arrays with reference types are checked for null values and masked if required.
    * @since 0.1.0
    */
  private[pd] def wrap[T](array: Array[T]): Series[T] =
    new Series(SeriesData.wrapArray(array), UniformIndex(array.length))

  // ** INNER CLASSES **

  class MutableSeriesBuilder(name: String = ""):

    /**
      * Creates an mutable Series of length `length` with the given value.
      *
      * @param length
      *   Length of the Series.
      * @param value
      *   Value for all elements.
      * @return
      *   New [[MutableSeries]].
      * @since 0.1.0
      */
    def fill[T: ClassTag](length: Int)(value: T): MutableSeries[T] = new MutableSeries(
      Series.fill(length)(value) as name
    )

    /**
      * Creates a mutable Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created [[MutableSeries]].
      * @since 0.1.0
      */
    def from[T: ClassTag](data: Array[T]): MutableSeries[T] = new MutableSeries(Series.from(data) as name)

    /**
      * Creates a mutable Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created [[MutableSeries]].
      * @since 0.1.0
      */
    def from[T: ClassTag](data: collection.mutable.Buffer[T]): MutableSeries[T] = new MutableSeries(
      Series.from(data) as name
    )

    /**
      * Creates a mutable Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created [[MutableSeries]].
      * @since 0.1.0
      */
    def from[T: ClassTag](data: Seq[T]): MutableSeries[T] = new MutableSeries(Series.from(data) as name)

    /**
      * Creates a mutable Series from a collection of Option data, where the Option is unpacked.
      *
      * @param data
      *   Collection of type `Option[T]`.
      * @return
      *   Created [[MutableSeries]] of type `T`.
      * @since 0.1.0
      */
    @targetName("fromOption")
    def from[T: ClassTag](data: Array[Option[T]]): MutableSeries[T] = new MutableSeries(Series.from(data) as name)

    /**
      * Creates a mutable Series from a collection of Option data, where the Option is unpacked.
      *
      * @param data
      *   Collection of type `Option[T]`.
      * @return
      *   Created [[MutableSeries]] of type `T`.
      * @since 0.1.0
      */
    @targetName("fromOption")
    def from[T: ClassTag](data: Seq[Option[T]]): MutableSeries[T] = new MutableSeries(Series.from(data) as name)

    /**
      * String representation.
      *
      * @return
      *   "MutableSeriesBuilder" with name of Series.
      * @since 0.1.0
      */
    override def toString: String = s"MutableSeriesBuilder($name)"

    /**
      * Wraps the array into a mutable Series without copying.
      *
      * Warning: Ensure that the array is not used after wrapping. Otherwise no guarantees can be given with regards to
      * correct functionality, null safety, etc.
      *
      * @param array
      *   Array to be wrapped by a mutable Series.
      * @return
      *   [[MutableSeries]].
      * @note
      *   Arrays with reference types are checked for null values and masked if required.
      * @since 0.1.0
      */
    def wrap[T](array: Array[T]): MutableSeries[T] =
      new MutableSeries[T](new Series(SeriesData.wrapArray(array), UniformIndex(array.length), name))

  class SeriesBuilder(name: String):

    /**
      * Creates a Series from values. Undefined values are symbolized by `null`.
      *
      * @param values
      *   Values of the Series.
      * @return
      *   Created Series.
      * @note
      *   - The Series is `null` safe, i.e. `null` values are not stored but represented internally by a mask.
      *   - The type of, e.g., `Series(1, null, 3)` is of type Series[Int] and is implemented as a primitive type.
      *   - Automatic type casting in the argument list works only without `null` values, i.e. Series(1, 1.1) and
      *     Series(1, null, 1.1) is of type Series[Double] whereas Series(1, null ,1.1) is of type Series[Any].
      * @see
      *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
      * @since 0.1.0
      */
    def apply[T: ClassTag](values: (T | Null)*): Series[T] =
      new Series[T](
        data = SeriesData.fromCollectionWithNull(values),
        UniformIndex(values.length),
        name,
      )

    /**
      * Creates an Series of length `length` with the given value.
      *
      * @param length
      *   Length of the Series.
      * @param value
      *   Value for all elements.
      * @return
      *   New Series.
      * @since 0.1.0
      */
    def fill[T: ClassTag](length: Int)(value: T): Series[T] = Series.fill(length)(value) as name

    /**
      * Creates a Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created Series.
      * @since 0.1.0
      */
    def from[T: ClassTag](data: Array[T]): Series[T] = Series.from(data) as name

    /**
      * Creates a Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created Series.
      * @since 0.1.0
      */
    def from[T: ClassTag](data: collection.mutable.Buffer[T]): Series[T] = Series.from(data) as name

    /**
      * Creates a Series from a collection of data.
      *
      * @param data
      *   Collection.
      * @return
      *   Created Series.
      * @since 0.1.0
      */
    def from[T: ClassTag](data: Seq[T]): Series[T] = Series.from(data) as name

    /**
      * Creates a Series from a collection of Option data, where the Option is unpacked.
      *
      * @param data
      *   Collection of type `Option[T]`.
      * @return
      *   Created Series of type `T`.
      * @since 0.1.0
      */
    @targetName("fromOption")
    def from[T: ClassTag](data: Array[Option[T]]): Series[T] = Series.from(data) as name

    /**
      * Creates a Series from a collection of Option data, where the Option is unpacked.
      *
      * @param data
      *   Collection of type `Option[T]`.
      * @return
      *   Created Series of type `T`.
      * @since 0.1.0
      */
    @targetName("fromOption")
    def from[T: ClassTag](data: Seq[Option[T]]): Series[T] = Series.from(data) as name

    /**
      * Creates a mutable Series (see [[MutableSeriesBuilder]]).
      *
      * @return
      *   [[MutableSeriesBuilder]].
      * @see
      *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
      *   [[https://pan-data.org/scala/basics/mutable.html]]
      * @since 0.1.0
      */
    def mutable: MutableSeriesBuilder = MutableSeriesBuilder(name)

    /**
      * Creates a mutable Series from values. Undefined values are symbolized by `null`.
      *
      * @param values
      *   Values of the Series.
      * @return
      *   Created [[MutableSeries]].
      * @note
      *   - The Series is `null` safe, i.e. `null` values are not stored but represented internally by a mask.
      *   - The type of, e.g., `Series(1, null, 3)` is of type Series[Int] and is implemented as a primitive type.
      *   - Automatic type casting in the argument list works only without `null` values, i.e. Series(1, 1.1) and
      *     Series(1, null, 1.1) is of type Series[Double] whereas Series(1, null ,1.1) is of type Series[Any].
      * @see
      *   [[https://pan-data.org/scala/basics/creating-a-series.html]]
      *   [[https://pan-data.org/scala/basics/mutable.html]]
      * @since 0.1.0
      */
    def mutable[T: ClassTag](values: (T | Null)*): MutableSeries[T] = new MutableSeries(apply(values*))

    /**
      * String representation.
      *
      * @return
      *   "SeriesBuilder" with name of Series.
      * @since 0.1.0
      */
    override def toString: String = s"SeriesBuilder($name)"

    /**
      * Wraps the array into a Series without copying.
      *
      * Warning: Ensure that the array is not used after wrapping. Otherwise no guarantees can be given with regards to
      * correct functionality, null safety, etc.
      *
      * @param array
      *   Array to be wrapped by a mutable Series.
      * @return
      *   [[Series]].
      * @note
      *   Arrays with reference types are checked for null values and masked if required.
      * @since 0.1.0
      */
    private[pd] def wrap[T](array: Array[T]): Series[T] =
      new Series(SeriesData.wrapArray(array), UniformIndex(array.length), name)
