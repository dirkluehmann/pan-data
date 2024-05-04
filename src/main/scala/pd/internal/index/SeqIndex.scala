/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.exception.{BaseIndexException, IllegalIndex}
import pd.internal.index.sort.Sorting
import pd.internal.series.SeriesData
import pd.internal.utils.StringUtils.asElements
import pd.{Order, Series}

import scala.collection.immutable.ArraySeq.unsafeWrapArray
import scala.collection.immutable.ListSet
import scala.collection.mutable

/**
  * A sequential index is a subset of a uniform index backed by an array of index positions.
  *
  * @param base
  *   Index of the underlying data vector.
  * @param indices
  *   Array of indices.
  * @since 0.1.0
  */
private[pd] class SeqIndex private (val base: UniformIndex, val indices: Array[Int]) extends BaseIndex:

  /**
    * Internal mapping to a mask for constant-time lookup. The array is built only if required.
    *
    * @since 0.1.0
    */
  private lazy val mask: Array[Boolean] =
    val array = Array.fill[Boolean](base.length)(false)
    // todo foreach might be slow
    indices.foreach(array(_) = true)
    array

  def describe: String = s"Sequential $toString with ${asElements(length)}."

  def isBase: Boolean = false

  def isBijective: Boolean = base.length == length

  def isEmpty: Boolean = indices.isEmpty

  def isContained(ix: Int): Boolean = if ix < 0 || ix >= mask.length then false else mask(ix)

  def iterator: IndexIterator = SeqIndexIterator(indices.length)

  def hasSubIndices(that: BaseIndex): Int =
    val distinctIndices = (indices ++ that.toSeqIndex.indices).distinct.length
    if distinctIndices > length then -1
    else if length == that.length then 0
    else 1

  inline def length: Int = indices.length

  def max: Int = if isEmpty then -1 else indices.max

  def partitions: Array[IndexIterator] = BaseIndex.partitioning(indices.length).map(SeqIndexIterator(_))

  def slice(range: Range): BaseIndex = slice(range.asInstanceOf[Seq[Int]])

  def slice(seq: Seq[Int]): BaseIndex =
    SeqIndex(base, seq.filter(indices.contains))

  def slice(array: Array[Int]): BaseIndex =
    SeqIndex(base, array.filter(indices.contains))

  def slice(series: Series[Boolean]): BaseIndex =
    if (series.index.base equivalent base)
      slice(series.indicesTrue)
    else
      throw BaseIndexException(base, series.index.base)

  override def sortBy(comparison: (Int, Int) => Boolean): BaseIndex =
    SeqIndex.unchecked(base, indices.toSeq.sortWith(comparison).toArray)

  def sorted: BaseIndex = new SeqIndex(base, indices.toSeq.sorted.toArray)

  override def sorted[T](key: (SeriesData[T], Order))(implicit ordering: Ordering[T]): BaseIndex =
    val sorting = Sorting[T](key._1, key._2)(ordering)
    sortBy(sorting.sort)

  /**
    * Returns index positions as a sequence.
    *
    * @return
    *   Sequence.
    * @since 0.1.0
    */
  def toSeq: Seq[Int] = indices.toSeq

  def toSeqIndex: SeqIndex = this

  override def toString: String =
    if isEmpty then "Index (<empty>)"
    else
      s"Index (${if indices.length > 5 then indices.take(5).mkString("", ", ", ", ...") else indices.mkString(", ")})"

  /**
    * Iterator over array of index positions.
    *
    * @param start
    *   Start of interval.
    * @param end
    *   End of interval.
    * @since 0.1.0
    */
  private[pd] class SeqIndexIterator(val start: Int, val end: Int) extends IndexIterator:

    private var ix: Int = start - 1

    /**
      * @return
      *   True, if next elements exists, false otherwise.
      * @since 0.1.0
      */
    def hasNext: Boolean = ix < end

    /**
      * @return
      *   Next element.
      * @since 0.1.0
      */
    def next: Int =
      ix += 1
      indices(ix)

  private[pd] object SeqIndexIterator:

    /**
      * @param start
      *   Start of interval, inclusive.
      * @param end
      *   End of interval, inclusive.
      * @return
      *   Iterator.
      * @since 0.1.0
      */
    def apply(start: Int, end: Int): SeqIndexIterator = new SeqIndexIterator(start, end)

    /**
      * @param interval
      *   Tuple (start of interval, end of interval) with start and end inclusive.
      * @return
      *   Iterator.
      * @since 0.1.0
      */
    def apply(interval: (Int, Int)): SeqIndexIterator = new SeqIndexIterator(interval._1, interval._2)

    /**
      * @param length
      *   Length of interval. Shorthand for (0, length-1).
      * @return
      *   Iterator.
      * @since 0.1.0
      */
    def apply(length: Int): SeqIndexIterator = new SeqIndexIterator(0, length - 1)

/**
  * A sequential index is a subset of a uniform index backed by an array of index positions.
  *
  * @since 0.1.0
  */
private[pd] object SeqIndex:

  /**
    * Creates a [[SeqIndex]] from sequence of index positions which are checked with regards to boundaries and
    * distinctness.
    *
    * @param base
    *   Base index.
    * @param indices
    *   Sequence of index positions.
    * @return
    *   Index.
    * @since 0.1.0
    */
  def apply(base: UniformIndex, indices: Seq[Int]): SeqIndex =
    val array = indices.toArray
    array.foreach(ix => if ix < 0 || ix >= base.length then throw IllegalIndex(s"Index $ix is not within the $base."))
    val distinctLength = array.distinct.length
    if distinctLength != array.length then
      throw IllegalIndex(s"${array.length - distinctLength} duplicate element(s) in index.")
    new SeqIndex(base, array)

  /**
    * Creates a [[SeqIndex]] from a (cloned) array of index positions which are checked with regards to boundaries and
    * distinctness.
    *
    * @param base
    *   Base index.
    * @param indices
    *   Array of index positions.
    * @return
    *   Index.
    * @since 0.1.0
    */
  def apply(base: UniformIndex, indices: Array[Int]): SeqIndex =
    val array = indices.clone
    array.foreach(ix => if ix < 0 || ix >= base.length then throw IllegalIndex(s"Index $ix is not within the $base."))
    val distinctLength = array.distinct.length
    if distinctLength != array.length then
      throw IllegalIndex(s"${array.length - distinctLength} duplicate element(s) in index.")
    new SeqIndex(base, array)

  /**
    * Creates [[SeqIndex]] from an array of index positions without checking and copying.
    *
    * @param base
    *   Base index.
    * @param indices
    *   Array of index positions.
    * @return
    *   Index.
    * @since 0.1.0
    */
  def unchecked(base: UniformIndex, indices: Array[Int]): SeqIndex =
    new SeqIndex(base, indices)
