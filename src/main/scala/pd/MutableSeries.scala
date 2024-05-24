/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.IndexBoundsException
import pd.implicits.mutable.*
import pd.internal.index.*
import pd.internal.series.SeriesData

import scala.annotation.targetName
import scala.language.implicitConversions
import scala.reflect.{ClassTag, Typeable}

/**
  * @see
  *   [[https://pan-data.org/scala/basics/muteable.html]]
  * @since 0.1.0
  */
class MutableSeries[T] private[pd] (private[pd] var inner: Series[T]) extends IndexOps[Unit]:

  // *** PUBLIC MUTABLE INTERFACE ***

  /**
    * Copies the data into an immutable Series.
    *
    * @return
    *   A Series object.
    * @see
    *   [[series]]
    * @since 0.1.0
    */
  def copy: Series[T] = new Series[T](inner.data.cloneSafely, inner.index, inner.name)

  /**
    * Puts the value at index `ix`. If `ix` is not in the index but within the base index, the index is expanded to the
    * uniform base index.
    *
    * @param ix
    *   Row index.
    * @return
    *   Value to be set.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   The access is constant time with the exception of the first `put` access to a sequential index or the (one-time)
    *   expansion of the index.
    * @see
    *   [[https://pan-data.org/scala/basics/mutable.html]]
    * @since 0.1.0
    */
  inline def put(ix: Int, value: T): Unit =
    if inner.index.isContained(ix) then
      inner.data.vector(ix) = value
      if inner.data.mask != null then inner.data.mask(ix) = true
    else if inner.index.base.isContained(ix) then
      inner = inner.dense
      inner.data.vector(ix) = value
      if inner.data.mask != null then inner.data.mask(ix) = true
    else throw IndexBoundsException(ix, inner.index.base)

  /**
    * Converts the MutableSeries into an immutable Series without copying its content, The data is "ejected" from the
    * mutable instance by overwriting it with an empty Series. After calling this method on instance `series`
    * {{{series.isEmpty}}} returns true.
    *
    * @return
    *   A Series object.
    * @see
    *   [[copy]]
    * @since 0.1.0
    */
  def series: Series[T] =
    val temp = inner
    inner = new Series[T](temp.data.createEmpty, UniformIndex(0))
    if temp.hasUndefined then
      if temp.data.mask.contains(false) then temp
      else
        new Series[T](
          new SeriesData[T](temp.data.vector.clone(), null),
          temp.index,
          temp.name,
        )
    else temp

  /**
    * Sets the value at index `ix`.
    *
    * @param ix
    *   Row index.
    * @return
    *   Value to be set.
    * @throws NoSuchElementException
    *   If `ix` is not in the index.
    * @note
    *   The access is constant time. Potentially the first `set` access to a sequential index can be non-constant time.
    * @see
    *   [[https://pan-data.org/scala/basics/mutable.html]]
    * @since 0.1.0
    */
  inline def set(ix: Int, value: T): Unit =
    if inner.index.isContained(ix) then
      inner.data.vector(ix) = value
      if inner.data.mask != null then inner.data.mask(ix) = true
    else throw new NoSuchElementException(s"Invalid index position $ix.")

  /**
    * Sets the value at index `ix` to undefined (null). If `ix` is not in the index but within the base index, the index
    * is expanded to the uniform base index.
    *
    * @param ix
    *   Row index.
    * @throws IndexBoundsException
    *   If `ix` is not part of the base index.
    * @note
    *   The access is constant time with the exception of the first `unset` access to a sequential index or the
    *   (one-time) expansion of the index.
    * @see
    *   [[https://pan-data.org/scala/basics/mutable.html]]
    * @since 0.1.0
    */
  def unset(ix: Int): Unit =
    if !inner.index.base.isContained(ix) then throw IndexBoundsException(ix, inner.index.base)
    else
      if !inner.index.isContained(ix) then inner = inner.dense
      if inner.data.mask == null then
        inner = new Series(
          new SeriesData[T](inner.data.vector, Array.fill[Boolean](inner.data.length)(true)),
          inner.index,
          inner.name,
        )
      inner.data.mask(ix) = false

  // *** PRIVATE MUTABLE INTERFACE ***

  /**
    * @return
    *   The index.
    * @since 0.1.0
    */
  private[pd] def index: BaseIndex = inner.index

  /**
    * Applies the index `index` implementing the [[IndexOps]] interface. A SeqIndex is converted to a MutableSeqIndex.
    *
    * @param index
    *   New index.
    * @since 0.1.0
    */
  private[pd] def withIndex(index: BaseIndex): Unit =
    inner = inner.withIndex(index)

  // *** REIMPLEMENTED MEMBERS FROM SERIES ***

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
  def ==[T2: ClassTag](series: Series[T2]): Series[Boolean] = inner == series

  /**
    * Compares the entries of the Series with the value `v` returning a boolean Series with entries
    *   - `true` if values are equal,
    *   - `false` if values are not equal,
    *   - `null` if the values of the series is not defined (null).
    *
    * Value to compare with.
    *
    * @return
    *   Boolean Series.
    * @see
    *   [[https://pan-data.org/scala/operations/comparison.html]]
    * @since 0.1.0
    */
  @targetName("equalsByRow")
  def ==[T2: ClassTag](v: T2): Series[Boolean] = inner == v

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
  def ==(v: Boolean): Series[Boolean] = inner == v

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
  def ==(v: Double): Series[Boolean] = inner == v

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
  def ==(v: Int): Series[Boolean] = inner == v

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
  def !=[T2: ClassTag](series: Series[T2]): Series[Boolean] = inner != series

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
  def !=[T2: ClassTag](v: T2): Series[Boolean] = inner != v

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
  def !=(v: Boolean): Series[Boolean] = inner != v

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
  def !=(v: Double): Series[Boolean] = inner != v

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
  def !=(v: Int): Series[Boolean] = inner != v

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
  def agg[R](start: R, f: (R, T) => R): R = inner.agg[R](start, f)

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
  def apply(ix: Int): Option[T] = inner.apply(ix)

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
  def apply(ix: Option[Int]): Option[T] = inner.apply(ix)

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
  inline def apply(ix: Int, default: => T): T = inner.apply(ix, default)

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
  inline def apply(ix: Option[Int], default: => T): T = inner.apply(ix, default)

  /**
    * Renames the Series.
    *
    * @param name
    *   New name.
    * @note
    *   Result: Series with new name.
    * @since 0.1.0
    */
  def as(name: String): Unit =
    inner = inner.as(name)

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
  def as[T2: Typeable: ClassTag](ix: Int): Option[T2] = inner.as[T2](ix)

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
  def as[T2: Typeable: ClassTag](ix: Option[Int]): Option[T2] = inner.as[T2](ix)

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

  def as[T2: Typeable: ClassTag](ix: Int, default: => T2): T2 = inner.as[T2](ix, default)

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
  def as[T2: Typeable: ClassTag](ix: Option[Int], default: => T2): T2 = inner.as[T2](ix, default)

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
  def count: Int = inner.count

  /**
    * Keeps only defined elements and removes all index positions with undefined elements.
    *
    * @note
    *   Result: Equivalent Series without undefined elements and trimmed index.
    * @since 0.1.0
    */
  // noinspection UnitMethodIsParameterless
  def defined: Unit =
    inner = inner.defined

  /**
    * Converts the Series into a Series with a uniform (dense) index, which equals the base index. Missing indices are
    * replaced by explicit undefined values (masked). The (accessible) data is not changed.
    *
    * @note
    *   Result: Same Series but with an uniform index.
    * @see
    *   [[https://pan-data.org/scala/advanced/indices.html]]
    * @since 0.1.0
    */
  // noinspection UnitMethodIsParameterless
  def dense: Unit =
    inner = inner.dense

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
    inner.display(n, colWidth)

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
  override def equals(that: Any): Boolean = inner.equals(that)

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
  def equalsByValue[T2: ClassTag](series: Series[T2]): Boolean = inner.equalsByValue[T2](series)

  /**
    * Indicates if a value is contained in the Series.
    *
    * @param value
    *   Value to search for.
    * @return
    *   Boolean flag if the the value exists.
    * @since 0.1.0
    */
  def exists(value: T): Boolean = inner.exists(value)

  /**
    * Replaces undefined (null) values by the values of `series` for the current index. This is also known as coalesce
    * operation.
    *
    * @param series
    *   Series with the same base index.
    * @note
    *   Result: Series with data from the first series or if undefined from the second. The original index is not
    *   altered.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @since 0.1.0
    */
  def fill[T2 <: T](series: Series[T2]): Unit =
    inner = inner.fill(series)

  /**
    * Fills undefined (null) values of a Series with respect to the current index.
    *
    * @param value
    *   Value for filling non-set values.
    * @note
    *   Result: Series with undefined values filled. The original index is preserved.
    * @since 0.1.0
    */
  def fill(value: T): Unit =
    inner = inner.fill(value)

  /**
    * Replaces undefined (null) values by the values of `series` also known as coalesce operation. The index is expanded
    * to the uniform base index.
    *
    * @param series
    *   Series with the same base index.
    * @note
    *   Result: Series with data from the first series or if undefined from the second. The index is expanded to the
    *   uniform base index.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @since 0.1.0
    */
  def fillAll[T2 <: T](series: Series[T2]): Unit =
    inner = inner.fillAll(series)

  /**
    * Fills undefined (null) values of a Series with respect to the base index.
    *
    * @param value
    *   Value for filling undefined values.
    * @note
    *   Result: Series with undefined values filled. The index is expanded to the uniform base index.
    * @since 0.1.0
    */
  def fillAll(value: T): Unit =
    inner = inner.fillAll(value)

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
  def first: Option[Int] = inner.first

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
  def first(f: T => Boolean): Option[Int] = inner.first(f)

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
  def first(value: T): Option[Int] = inner.first(value)

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
  def first[T2](series: Series[T2], value: T2): Option[T] = inner.first(series, value)

  /**
    * Returns the value at the index position, where a value occurs the first time in another Series or None if the
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
  def first[T2](series: Series[T2], value: T2, default: => T): T = inner.first(series, value, default)

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
  def forall(f: T => Boolean): Boolean = inner.forall(f)

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
  def forallStrictly(f: T => Boolean): Boolean = inner.forallStrictly(f)

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
  inline def get(ix: Int): T = inner.get(ix)

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
  inline def get(ix: Option[Int]): T = inner.get(ix)

  /**
    * Hash code.
    *
    * @return
    *   Hash code derived from index length and name.
    * @since 0.1.0
    */
  override def hashCode: Int = inner.hashCode

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
  def hasSameDefined[T2: ClassTag](series: Series[T2]): Boolean = inner.hasSameDefined(series)

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
  def hasSameIndex[T2: ClassTag](series: Series[T2]): Boolean = inner.hasSameIndex(series)

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
  def hasUndefined: Boolean = inner.hasUndefined

  /**
    * Returns the first value as an Option, i.e. `Some(value)` or `None` if the first value is undefined.
    *
    * @return
    *   Value as an Option.
    * @note
    *   The result depends on the index order.
    * @since 0.1.0
    */
  def headOption: Option[T] = inner.headOption

  /**
    * Returns the first (defined) value as an Option, i.e. `Some(value)` or `None` if all values are undefined.
    *
    * @return
    *   Value as an Option, which is `None` if the Series an no defined values.
    * @note
    *   The result depends on the index order.
    * @since 0.1.0
    */
  def headValue: Option[T] = inner.headValue

  /**
    * Indicates if the index is empty.
    *
    * @return
    *   True if the index has at least one element or otherwise false.
    * @since 0.1.0
    */
  def indexEmpty: Boolean = inner.indexEmpty

  /**
    * Iterator over the index.
    *
    * @return
    *   Iterator of index positions.
    * @note
    *   For better performance consider using methods such as [[agg]] or [[map]].
    * @since 0.1.0
    */
  def indexIterator: Iterator[Int] = inner.indexIterator

  /**
    * Indicates if the index is not empty.
    *
    * @return
    *   True if the index has no elements or otherwise false.
    * @since 0.1.0
    */
  def indexNonEmpty: Boolean = inner.indexNonEmpty

  /**
    * Information string on the Series.
    *
    * @return
    *   Info string.
    * @since 0.1.0
    */
  def info: String = inner.info

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
  def isDefined: Boolean = inner.isDefined

  /**
    * Indicates if a Series has no defined elements.
    *
    * @return
    *   True if either the index is empty or all values are undefined. Otherwise false is returned.
    * @since 0.1.0
    */
  def isEmpty: Boolean = inner.isEmpty

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
  def isType[T2: Typeable]: Boolean = inner.isType[T2]

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
  def isTypeAll[T2: Typeable]: Boolean = inner.isTypeAll[T2]

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
  def isTypeAllStrictly[T2: Typeable]: Boolean = inner.isTypeAllStrictly[T2]

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
  def isTypeStrictly[T2: Typeable]: Boolean = inner.isTypeStrictly[T2]

  /**
    * Iterator over the values of the Series ordered by the index.
    *
    * @return
    *   Iterator of type `Option[T]` returning `Some(value)` if the value is defined and `None` if undefined.
    * @note
    *   For better performance consider using methods such as [[agg]] or [[map]].
    * @since 0.1.0
    */
  def iterator: Iterator[Option[T]] = inner.iterator

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
  def last: Option[Int] = inner.last

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
  def last(value: T): Option[Int] = inner.last(value)

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
  def last[T2](series: Series[T2], value: T2): Option[T] = inner.last(series, value)

  /**
    * Returns the value at the index position, where a value occurs the last time in another Series or None if the value
    * is not found.
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
  def last[T2](series: Series[T2], value: T2, default: => T): T = inner.last(series, value, default)

  /**
    * Number of rows.
    *
    * @return
    *   Length of the Series, i.e. number of elements in the index.
    * @see
    *   [[numRows]]
    * @since 0.1.0
    */
  def length: Int = inner.length

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
  def map[R: ClassTag](f: T => R): Series[R] = inner.map(f)

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
  def map[T2: ClassTag, R: ClassTag](series: Series[T2], f: (T, T2) => R): Series[R] = inner.map(series, f)

  /**
    * Name of the MutableSeries.
    *
    * @return
    *   Name. If no name is set the name is an empty string.
    * @since 0.1.0
    */
  def name: String = inner.name

  /**
    * Indicates if a Series has defined elements.
    *
    * @return
    *   True if at least one defined element exists. Otherwise false is returned.
    * @since 0.1.0
    */
  def nonEmpty: Boolean = inner.nonEmpty

  /**
    * Number of rows.
    *
    * @return
    *   Length of the Series, i.e. number of elements in the index.
    * @see
    *   [[length]]
    * @since 0.1.0
    */
  def numRows: Int = inner.numRows

  /**
    * Number of rows of the underlying data vector.
    *
    * @return
    *   Number of elements in the base index.
    * @since 0.1.0
    */
  def numRowsBase: Int = inner.numRowsBase

  /**
    * Replaces undefined (null) values by the values of `series` for the current index. Synonym for [[fill]].
    *
    * @param series
    *   Series with the same base index.
    * @note
    *   Result: Series with data from the first series or if undefined from the second. The original index is not
    *   altered.
    * @throws BaseIndexException
    *   If the base index are different.
    * @throws SeriesCastException
    *   If the Series do not have the same type.
    * @see
    *   [[fill]]
    * @since 0.1.0
    */
  def orElse[T2 <: T](series: Series[T2]): Unit =
    inner = inner.orElse(series)

  /**
    * Fills undefined (null) values of a Series with respect to the current index. Synonym for [[fill]].
    *
    * @param value
    *   Value for filling non-set values.
    * @note
    *   Result: Series with undefined values filled. The original index is preserved.
    * @see
    *   [[fill]]
    * @since 0.1.0
    */
  def orElse(value: T): Unit =
    inner = inner.orElse(value)

  /**
    * Resets the index to a `UniformIndex` with index positions 0 to `numRows - 1` while keeping the order of the
    * elements. If the current index is not a uniform index, the data is copied into a new vector with the order of the
    * current index.
    *
    * @note
    *   Result: Series with a `UniformIndex`.
    * @see
    *   [[sortIndex]] for sorting the index by index positions.
    * @since 0.1.0
    */
  // noinspection UnitMethodIsParameterless
  def resetIndex: Unit =
    inner = inner.resetIndex

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
  ): Unit = inner.show(n, annotateIndex, annotateType, colWidth)

  /**
    * Prints all values of the Series.
    *
    * @param n
    *   The maximal numbers of rows to be printed or -1 (default) for all values.
    * @see
    *   [[https://pan-data.org/scala/basics/working-with-series.html]]
    * @since 0.1.0
    */
  def showValues(n: Int = -1): Unit = inner.showValues(n)

  /**
    * Sorts the Series in ascending order.
    *
    * @param ordering
    *   Implicit ordering that must exist for the type `T`, i.e. classes must extend the trait [[Ordered]].
    * @note
    *   Result: Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted(implicit ordering: Ordering[T]): Unit =
    inner = inner.sorted

  /**
    * Sorts the Series.
    *
    * @param order
    *   Order for sorting. Possible values are [[Order.asc]], [[Order.desc]], [[Order.ascNullsFirst]] and
    *   [[Order.descNullsFirst]].
    * @param ordering
    *   Implicit ordering that must exist for the type `T`, i.e. classes must extend the trait [[Ordered]].
    * @note
    *   Result: Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted(order: Order)(implicit ordering: Ordering[T]): Unit =
    inner.sorted(order)

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
  def str: Series[String] = inner.str

  /**
    * Copies the Series into an array.
    *
    * @return
    *   Array of type `Option[T]` with [[numRows]] elements.
    * @since 0.1.0
    */
  def toArray: Array[Option[T]] = inner.toArray

  /**
    * Copies the Series into an array.
    *
    * @return
    *   Array of type `T`.
    * @since 0.1.0
    */
  def toFlatArray: Array[T] = inner.toFlatArray

  /**
    * Copies the Series into a List.
    *
    * @return
    *   List of type `T`.
    * @since 0.1.0
    */
  def toFlatList: List[T] = inner.toFlatList

  /**
    * Copies the Series into a sequence.
    *
    * @return
    *   Sequence of type `T`.
    * @since 0.1.0
    */
  def toFlatSeq: Seq[T] = inner.toFlatSeq

  /**
    * Copies the Series into a List.
    *
    * @return
    *   List of type `Option[T]` with [[numRows]] elements.
    * @since 0.1.0
    */
  def toList: List[Option[T]] = inner.toList

  /**
    * Copies the Series into a sequence with [[numRows]] elements.
    *
    * @return
    *   Sequence of type `Option[T]`.
    * @since 0.1.0
    */
  def toSeq: Seq[Option[T]] = inner.toSeq

  /**
    * Renders the Series as a table using default parameters.
    *
    * @return
    *   Formatted table.
    * @since 0.1.0
    */
  override def toString: String = inner.toString

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
  ): String = inner.toString(n, colWidth, annotateIndex, annotateType)

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
  def typeString: String = inner.typeString

  /**
    * Updates the series with values from the second series if the values are defined.
    *
    * @param series
    *   Series with the same base index.
    * @note
    *   Result: Series with data from parameter `series` or if a value is undefined in `series` from the original
    *   series. The original index is not altered.
    * @since 0.1.0
    */
  def update[T2 <: T](series: Series[T2]): Unit =
    inner = inner.update(series)

/**
  * @see
  *   [[https://pan-data.org/scala/basics/muteable.html]]
  * @since 0.1.0
  */
object MutableSeries:
  implicit def toMutableSeriesAny(s: MutableSeries[Any]): MutableSeriesAny = MutableSeriesAny(s)
  implicit def toMutableSeriesBoolean(s: MutableSeries[Boolean]): MutableSeriesBoolean = MutableSeriesBoolean(s)
  implicit def toMutableSeriesDouble(s: MutableSeries[Double]): MutableSeriesDouble = MutableSeriesDouble(s)
  implicit def toMutableSeriesInt(s: MutableSeries[Int]): MutableSeriesInt = MutableSeriesInt(s)
  implicit def toMutableSeriesString(s: MutableSeries[String]): MutableSeriesString = MutableSeriesString(s)
