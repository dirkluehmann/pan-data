/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

/**
  * Iterates over [start, end] with both `start` and `end` inclusive.
  *
  * @param start
  *   Start of interval.
  * @param end
  *   End of interval.
  * @since 0.1.0
  */
private[pd] class RangeIterator(val start: Int, val end: Int) extends IndexIterator:

  private var ix: Int = start - 1

  /**
    * @return
    *   True, if next elements exists, false otherwise.
    * @since 0.1.0
    */
  inline def hasNext: Boolean = ix < end

  /**
    * @return
    *   Next element.
    * @since 0.1.0
    */
  inline def next: Int =
    ix += 1
    ix

/**
  * Iterates over [start, end] with both `start` and `end` inclusive.
  *
  * @since 0.1.0
  */
private[pd] object RangeIterator:

  /**
    * @param start
    *   Start of interval, inclusive.
    * @param end
    *   End of interval, inclusive.
    * @return
    *   Iterator.
    * @since 0.1.0
    */
  def apply(start: Int, end: Int): RangeIterator = new RangeIterator(start, end)

  /**
    * @param interval
    *   Tuple (start of interval, end of interval) with start and end inclusive.
    * @return
    *   Iterator.
    * @since 0.1.0
    */
  def apply(interval: (Int, Int)): RangeIterator = new RangeIterator(interval._1, interval._2)

  /**
    * @param length
    *   Length of interval. Shorthand for (0, length-1).
    * @return
    *   Iterator.
    * @since 0.1.0
    */
  def apply(length: Int): RangeIterator = new RangeIterator(0, length - 1)
