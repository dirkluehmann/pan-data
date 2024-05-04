/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.Series
import pd.exception.{BaseIndexException, IllegalIndex}
import pd.internal.utils.StringUtils.asElements

/**
  * A sliced index is a subset of a uniform index with a range of index positions between `start` to `end`.
  *
  * @param base
  *   Index of the underlying data vector.
  * @param start
  *   Lowest index position, inclusive.
  * @param end
  *   Highest index position, inclusive.
  * @since 0.1.0
  */
private[pd] class SlicedIndex private (
    override val base: UniformIndex,
    private[pd] val start: Int,
    private[pd] val end: Int,
) extends BaseIndex:

  def describe: String = s"Sliced $toString with ${asElements(length)}."

  def hasSubIndices(that: BaseIndex): Int = that match
    case _: UniformIndex =>
      if start == 0 then
        if end - 1 > length then 1
        else if end - 1 == length then 0
        else -1
      else -1
    case ix: SlicedIndex =>
      if ix.start == start && ix.end == end then 0
      else if ix.start >= start && ix.end <= end then 1
      else -1
    case ix: SeqIndex =>
      if ix.indices.forall(i => i >= start && i <= end) then if ix.length == length then 0 else 1
      else -1

  def isBase: Boolean = false

  def isBijective: Boolean = base.length == length

  def isEmpty: Boolean = false

  def isContained(ix: Int): Boolean = ix >= start && ix <= end

  def iterator: IndexIterator = RangeIterator(start, end)

  inline def length: Int = end - start + 1

  def max: Int = end

  def partitions: Array[IndexIterator] = BaseIndex.partitioning(start, end).map(RangeIterator(_))

  def slice(r: Range): BaseIndex =
    if r.isEmpty then SeqIndex(base, Seq[Int]())
    else if r.step == 1 then SlicedIndex(base, (start max r.start) to (end min r.last))
    else slice(r.asInstanceOf[Seq[Int]])

  def slice(seq: Seq[Int]): BaseIndex = SeqIndex(base, seq.filter(x => x >= start && x <= end))

  def slice(seq: Array[Int]): BaseIndex = SeqIndex(base, seq.filter(x => x >= start && x <= end))

  def slice(series: Series[Boolean]): BaseIndex =
    if (series.index.base equivalent base)
      slice(series.indicesTrue)
    else
      throw BaseIndexException(base, series.index.base)

  def sorted: BaseIndex = this

  def toSeqIndex: SeqIndex = SeqIndex(base, start to end)

  override def toString: String = s"Index ($start to $end)"

/**
  * A sliced index is a subset of a uniform index with a range of index positions between `start` to `end`.
  *
  * @since 0.1.0
  */
private[pd] object SlicedIndex:

  /**
    * Creates a [[SlicedIndex]] from a Range.
    *
    * @param base
    *   Base index.
    * @param range
    *   Range (with positive step size)
    * @return
    *   Index.
    * @since 0.1.0
    */
  def apply(base: UniformIndex, range: Range): BaseIndex =
    if range.step < 1 then throw IllegalIndex(s"Ranges with negative step sizes are not permitted.")

    if range.start < 0 || range.end < 0 || range.start >= base.length ||
      (range.nonEmpty && range.last >= base.length)
    then throw IllegalIndex(s"${range.toString} is not within $base.")

    if range.isEmpty then SeqIndex(base, Seq[Int]())
    else if range.step == 1 then new SlicedIndex(base, range.start, range.last)
    else SeqIndex(base, range)
