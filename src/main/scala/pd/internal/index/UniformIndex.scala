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
  * Uniform index from 0 to `length-1` representing the size of the underlying data vector. It is used as index `base`
  * for all types of indices.
  *
  * @param length
  *   Length of index.
  * @since 0.1.0
  */
private[pd] class UniformIndex(override val length: Int) extends BaseIndex:

  if length < 0 then throw IllegalIndex(s"Index length cannot be negative (found $length).")

  def base: UniformIndex = this

  def describe: String = s"Uniform $toString with ${asElements(length)}."

  override def equals(that: BaseIndex): Boolean = that match
    case x: UniformIndex => equivalent(x)
    case x: SlicedIndex  => equivalent(x)
    case x: BaseIndex    => super.equals(x)

  inline def equivalent(that: UniformIndex): Boolean =
    length == that.length

  inline def equivalent(that: SlicedIndex): Boolean =
    that.start == 0 && length == that.end + 1

  def isBase: Boolean = true

  def isEmpty: Boolean = length < 1

  def hasSubIndices(that: BaseIndex): Int = that match
    case ix: UniformIndex =>
      if ix.length < length then 1
      else if ix.length == length then 0
      else -1
    case ix: SlicedIndex =>
      if ix.end + 1 < length then 1
      else if ix.end + 1 == length then if ix.start == 0 then 0 else 1
      else -1
    case ix: SeqIndex =>
      if ix.indices.forall(_ < length) then if ix.length == length then 0 else 1
      else -1

  def isBijective: Boolean = true

  def isContained(ix: Int): Boolean = ix >= 0 && ix < length

  def iterator: IndexIterator = RangeIterator(length)

  def max: Int = length - 1

  def partitions: Array[IndexIterator] = BaseIndex.partitioning(length).map(RangeIterator(_))

  def slice(range: Range): BaseIndex = SlicedIndex(this, range)

  def slice(seq: Seq[Int]): BaseIndex = SeqIndex(this, seq)

  def slice(array: Array[Int]): BaseIndex = SeqIndex(base, array)

  def slice(series: Series[Boolean]): BaseIndex =
    if (series.index.base equivalent base)
      SeqIndex.unchecked(base, series.indicesTrue)
    else
      throw BaseIndexException(base, series.index.base)

  def sorted: BaseIndex = this

  def toSeqIndex: SeqIndex = SeqIndex(this, 0 until length)

  override def toString: String =
    if isEmpty then "Index (<empty>)"
    else s"Index (0 to ${length - 1})"
