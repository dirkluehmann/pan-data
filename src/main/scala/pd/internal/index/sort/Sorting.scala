/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index.sort

import pd.Order
import pd.internal.series.SeriesData

/**
  * Sorting interface.
  *
  * @since 0.1.0
  */
private[pd] trait Sorting:
  def sort: (Int, Int) => Boolean

/**
  * Sorting companion object.
  *
  * @since 0.1.0
  */
private[pd] object Sorting:
  def apply[T](data: SeriesData[T], order: Order, successor: Option[Sorting] = None)(implicit
      ordering: Ordering[T]
  ): Sorting =
    if successor.isEmpty then SortingSingle[T](data, order)(ordering)
    else SortingMultiple[T](data, order, successor.get)(ordering)
