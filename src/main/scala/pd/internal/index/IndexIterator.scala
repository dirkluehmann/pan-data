/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

/**
  * Iterator over an index with primitive performance.
  *
  * @since 0.1.0
  */
private[pd] trait IndexIterator extends Iterator[Int]:

  /**
    * @return
    *   End of interval, inclusive.
    * @since 0.1.0
    */
  def end: Int

  /**
    * @return
    *   True, if next elements exists, false otherwise.
    * @since 0.1.0
    */
  def hasNext: Boolean

  /**
    * @return
    *   Next element.
    * @since 0.1.0
    */
  def next: Int

  /**
    * @return
    *   Start of interval, inclusive.
    * @since 0.1.0
    */
  def start: Int
