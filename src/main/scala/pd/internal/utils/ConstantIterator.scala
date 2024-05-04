/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.utils

/**
  * Iterator with `length` elements that are all of the value `value`.
  *
  * @param value
  *   Value of all elements.
  * @param length
  *   Length of iterator.
  * @since 0.1.0
  */
class ConstantIterator(value: Int, length: Int) extends Iterator[Int]:

  private var i: Int = 0

  inline def hasNext: Boolean = i < length

  inline def next: Int =
    i += 1
    value
