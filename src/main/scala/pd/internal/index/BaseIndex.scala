/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.exception.IndexBoundsException
import pd.internal.series.SeriesData
import pd.{Order, Series, Settings}

/**
  * Index base class used for [[Series]] and [[DataFrame]].
  *
  * @since 0.1.0
  */
private[pd] abstract class BaseIndex:

  /**
    * Index of underlying data vector.
    *
    * @return
    *   Index of underlying data vector. Might reference to itself.
    * @since 0.1.0
    */
  def base: UniformIndex

  /**
    * Determines if position `ix` is contained in the index. The position must be inside the boundaries of the data
    * vector.
    *
    * @param ix
    *   Index position.
    * @return
    *   True, if contained.
    * @throws IndexBoundsException
    *   if out of bounds.
    * @since 0.1.0
    */
  inline def contains(ix: Int): Boolean =
    if ix >= 0 && ix < base.length then isContained(ix)
    else throw IndexBoundsException(ix, base.length)

  /**
    * Short description.
    *
    * @return
    *   Short description.
    * @since 0.1.0
    */
  def describe: String

  /**
    * Checks if indices have the same elements, the same order and the same base index although the index type can be
    * different.
    *
    * @param that
    *   Instance of BaseIndex.
    * @return
    *   True if equivalent as defined above.
    * @since 0.1.0
    */
  def equals(that: BaseIndex): Boolean =
    eq(that) || (hasSameBase(that) && java.util.Arrays.equals(toSeqIndex.indices, that.toSeqIndex.indices))

  /**
    * Checks if base is equivalent.
    *
    * @param that
    *   Instance of [[BaseIndex]].
    * @return
    *   True, if base index is equivalent.
    * @since 0.1.0
    */
  inline def hasSameBase(that: BaseIndex): Boolean = base.equals(that.base)

  /**
    * Checks if index `that` is a sub index (with all indices contained in `this`) without comparing the base index.
    *
    * @param that
    *   Index to be checked.
    * @return
    *   `-1` if `that` is not sub index of `this`; `0` if both indices are equivalent; `1` if `that` is sub index of
    *   `this`
    * @since 0.1.0
    */
  def hasSubIndices(that: BaseIndex): Int

  /**
    * Head of index.
    *
    * @param n
    *   Number of rows.
    * @return
    *   First n rows in index.
    * @since 0.1.0
    */
  def head(n: Int): BaseIndex = SeqIndex.unchecked(base, iterator.take(n).toArray)

  /**
    * Checks if index `that` is a sub index (with all indices contained in `this`). If the base index is not equivalent
    * `-1` is returned.
    *
    * @param that
    *   Index to be checked.
    * @return
    *   `-1` if `that` is not sub index of `this`; `0` if both indices are equivalent; `1` if `that` is sub index of
    *   `this`
    * @since 0.1.0
    */
  def includes(that: BaseIndex): Int =
    if base.equivalent(that.base) then hasSubIndices(that) else -1

  /**
    * Determines if base index.
    *
    * @return
    *   True, if index is the base index.
    * @since 0.1.0
    */
  def isBase: Boolean

  /**
    * Checks if the index covers the complete base index, i.e. a bijective mapping exists between index and base index.
    *
    * @return
    *   True, if bijective.
    * @since 0.1.0
    */
  def isBijective: Boolean

  /**
    * Determines if `ix` is contained in the index without checking the base boundaries.
    *
    * @param ix
    *   Index position.
    * @return
    *   True, if contained.
    * @since 0.1.0
    */
  def isContained(ix: Int): Boolean

  /**
    * Determines if empty.
    *
    * @return
    *   True, if the index has no elements.
    * @since 0.1.0
    */
  def isEmpty: Boolean

  /**
    * Iterator over the index.
    *
    * @return
    *   Iterator over the index.
    * @since 0.1.0
    */
  def iterator: IndexIterator

  /**
    * Number of elements in the index.
    *
    * @return
    *   Number of elements in the index.
    * @since 0.1.0
    */
  def length: Int

  /**
    * Maximal position in index.
    *
    * @return
    *   Maximal position in index or `-1` if empty.
    * @since 0.1.0
    */
  def max: Int

  /**
    * Determines if not empty.
    *
    * @return
    *   True, if non empty.
    * @since 0.1.0
    */
  def nonEmpty: Boolean = !isEmpty

  /**
    * Iterator over values of [[pd.internal.series.SeriesData]].
    *
    * @param seriesData
    *   Data vector.
    * @tparam T
    *   Type of data.
    * @return
    *   Iterator.
    * @since 0.1.0
    */
  def over[T](seriesData: SeriesData[T]): Iterator[Option[Any]] =
    iterator.map(seriesData(_))

  /**
    * Iterator over values of [[pd.internal.series.SeriesData]] with index positions.
    *
    * @param seriesData
    *   Data vector.
    * @tparam T
    *   Type of data.
    * @return
    *   Iterator of tuple with index and Option of value.
    * @since 0.1.0
    */
  def overWithIndex[T](seriesData: SeriesData[T]): Iterator[(Int, Option[Any])] =
    iterator.map(ix => (ix, seriesData(ix)))

  /**
    * Partitioning of interval with regards to current Settings.
    *
    * If the interval is smaller than number of partitions, only one partition is returned. Partitions are equally sized
    * with the exception of the last one which might have more elements.
    *
    * @return
    *   Array of [[IndexIterator]] objects.
    * @since 0.1.0
    */
  def partitions: Array[IndexIterator]

  /**
    * Slices the index using a defined (unchecked) range.
    *
    * @param range
    *   Range.
    * @return
    *   Index instance with intersection with ascending order.
    * @since 0.1.0
    */
  def slice(range: Range): BaseIndex

  /**
    * Slices the index using a defined (unchecked) sequence.
    *
    * @param seq
    *   Sequence of index positions.
    * @return
    *   Index instance with intersection and order of `seq`.
    * @since 0.1.0
    */
  def slice(seq: Seq[Int]): BaseIndex

  /**
    * Slices the index using a defined (unchecked) array.
    *
    * @param array
    *   Array of index positions.
    * @return
    *   Index with intersection and order of `array`.
    * @since 0.1.0
    */
  def slice(array: Array[Int]): BaseIndex

  /**
    * Slices the index using a boolean Series.
    *
    * @param series
    *   Boolean Series as mask.
    * @return
    *   Index with intersection and order of `series`.
    * @since 0.1.0
    */
  def slice(series: Series[Boolean]): BaseIndex

  /**
    * Sorts index with respect to comparison function, e.g. comparing represented values in a Series. The sorting is
    * stable.
    *
    * @param comparison
    *   Lower-than comparison function comparing two index positions. It tests whether its first argument precedes its
    *   second argument in the desired ordering (see [[Seq.sortWith]]).
    * @return
    *   Sorted index.
    * @since 0.1.0
    */
  def sortBy(comparison: (Int, Int) => Boolean): BaseIndex = toSeqIndex.sortBy(comparison)

  /**
    * Sorts the index positions.
    *
    * @return
    *   Index with sorted index positions. Can be the same instance.
    * @since 0.1.0
    */
  def sorted: BaseIndex

  /**
    * Sorts the index position with respect to data entries.
    *
    * @param key
    *   Tuple of [[pd.internal.series.SeriesData]] and [[order]].
    * @param ordering
    *   Implicit [[Ordering]].
    * @tparam T
    *   Type of data.
    * @return
    *   Sorted index.
    * @since 0.1.0
    */
  def sorted[T](key: (SeriesData[T], Order))(implicit ordering: Ordering[T]): BaseIndex =
    toSeqIndex.sorted[T](key)

  /**
    * Tail of index.
    *
    * @param n
    *   Number of rows.
    * @return
    *   Last n rows in index.
    * @since 0.1.0
    */
  def tail(n: Int): BaseIndex =
    SeqIndex.unchecked(base, iterator.drop((length - (n max 0)) max 0).toArray)

  /**
    * Converts index to a [[SeqIndex]] instance.
    *
    * @return
    *   Sequential index.
    * @since 0.1.0
    */
  def toSeqIndex: SeqIndex

/**
  * Base index used for [[Series]] and [[DataFrame]].
  *
  * @since 0.1.0
  */
private[pd] object BaseIndex:

  /**
    * Creates empty index.
    *
    * @return
    *   Empty index.
    * @since 0.1.0
    */
  def empty: BaseIndex = UniformIndex(0)

  /**
    * Partitioning of interval using current Settings.
    *
    * @param start
    *   Start of interval, inclusive.
    * @param end
    *   End of interval, inclusive.
    * @return
    *   Array of tuples (inclusive interval left, inclusive interval right).
    * @see
    *   [[BaseIndex.partitions]]
    * @since 0.1.0
    */
  def partitioning(start: Int, end: Int): Array[(Int, Int)] =
    val length = Settings.partitions
    val size = end - start + 1
    val elements = size / length
    if elements < 1 then Array((start, end))
    else
      val array = new Array[(Int, Int)](length)
      var ix = start
      var partition = 0
      while partition < length do
        if partition < length - 1 then array(partition) = (ix, ix + elements - 1)
        else array(partition) = (ix, end)
        ix += elements
        partition += 1
      array

  /**
    * Partitioning of interval (0, size-1) using current Settings.
    *
    * @param size
    *   Total length.
    * @return
    *   Array of tuples (inclusive interval left, inclusive interval right).
    * @since 0.1.0
    */
  def partitioning(size: Int): Array[(Int, Int)] =
    partitioning(0, size - 1)

  /**
    * Unions indices to a common index containing all index positions of all indices.
    *
    * The index can potentially have a different base index.
    *
    * @param indices
    *   Sequence of indices.
    * @return
    *   Union index.
    * @since 0.1.0
    */
  def union(indices: Seq[BaseIndex]): BaseIndex =
    require(indices.nonEmpty)
    val distinctSeq = indices.distinct
    val allIndices =
      distinctSeq.tail.foldLeft(distinctSeq.head.toSeqIndex.toSeq)((a, b) => a ++ b.toSeqIndex.toSeq)
    val newIndices = allIndices.distinct.sorted
    SeqIndex(UniformIndex(newIndices.max + 1), newIndices)

  /**
    * Unions indices with the same base index to a common index containing all index positions of all indices.
    *
    * The index can potentially have a different base index.
    *
    * @param indices
    *   Sequence of indices.
    * @return
    *   Union index.
    * @since 0.1.0
    */
  def unionSameBase(indices: Seq[BaseIndex]): BaseIndex =
    require(indices.nonEmpty)
    val distinctIndices = indices.distinct
    val allIndices =
      distinctIndices.tail.foldLeft(distinctIndices.head.toSeqIndex.toSeq)((a, b) => a ++ b.toSeqIndex.toSeq)
    SeqIndex(distinctIndices.head.base, allIndices.distinct.sorted)
