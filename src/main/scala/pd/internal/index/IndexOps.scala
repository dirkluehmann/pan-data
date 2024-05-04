/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.Series

/**
  * Trait with common index operations.
  *
  * @tparam V
  *   Type which is implementing the trait.
  * @since 0.1.0
  */
private[pd] trait IndexOps[V]:

  // *** TO BE IMPLEMENTED ***

  /**
    * Row index.
    *
    * @return
    *   The index.
    * @since 0.1.0
    */
  private[pd] def index: BaseIndex

  /**
    * Creates an instance of type `V` with the index.
    *
    * @param index
    *   New index.
    * @return
    *   New instance of type `V`.
    * @since 0.1.0
    */
  private[pd] def withIndex(index: BaseIndex): V

  // *** PUBLIC INTERFACE ***

  /**
    * Slices the index by intersecting it with a `range`.
    *
    * @param range
    *   Range.
    * @return
    *   Object with sliced index with ascending order.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(range: Range): V = withIndex(index.slice(range))

  /**
    * Slices the index by intersecting it with a sequence of index positions.
    *
    * @param seq
    *   Sequence of index positions.
    * @return
    *   Object with sliced index and order of `seq`.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(seq: Seq[Int]): V = withIndex(index.slice(seq))

  /**
    * Slices the index by intersecting it with an array of index positions.
    *
    * @param array
    *   Array of index positions.
    * @return
    *   Object with sliced index and order of `array`.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(array: Array[Int]): V = withIndex(index.slice(array))

  /**
    * Slices the index by intersecting the current index with a boolean Series.
    *
    * @param series
    *   Boolean Series as mask, where only index positions kept that are `true`.
    * @return
    *   Object with sliced index and order of `series`.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(series: Series[Boolean]): V = withIndex(index.slice(series))

  /**
    * Head of object.
    *
    * @param n
    *   Number of rows.
    * @return
    *   First n rows in index.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def head(n: Int = 1): V = withIndex(index.head(n))

  /**
    * Sorts the index (ascending).
    *
    * @return
    *   Object with sorted index.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def sortIndex: V = withIndex(index.sorted)

  /**
    * Tail of object.
    *
    * @param n
    *   Number of rows.
    * @return
    *   Last n rows in index.
    * @see
    *   [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def tail(n: Int): V = withIndex(index.tail(n))
