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

import scala.math.Ordered.orderingToOrdered

/**
  * Multi-column sorting.
  *
  * @param data
  *   Data to be sorted.
  * @param order
  *   Order.
  * @param successor
  *   Next sorting stage which is applied for equal values.
  * @param ordering
  *   Implicit ordering.
  * @since 0.1.0
  */
class SortingMultiple[T](data: SeriesData[T], order: Order, successor: Sorting)(implicit
    ordering: Ordering[T]
) extends SortingSingle[T](data, order)(ordering):

  private val next = successor.sort

  override def asc(a: Int, b: Int): Boolean =
    if mask == null then if vec(a) == vec(b) then next(a, b) else vec(a) < vec(b)
    else if !mask(b) then if !mask(a) then next(a, b) else true
    else if !mask(a) then false
    else if vec(a) == vec(b) then next(a, b)
    else vec(a) < vec(b)

  override def desc(a: Int, b: Int): Boolean =
    if mask == null then if vec(a) == vec(b) then next(a, b) else vec(a) > vec(b)
    else if !mask(b) then if !mask(a) then next(a, b) else true
    else if !mask(a) then false
    else if vec(a) == vec(b) then next(a, b)
    else vec(a) > vec(b)

  override def ascNullsFirst(a: Int, b: Int): Boolean =
    if mask == null then if vec(a) == vec(b) then next(a, b) else vec(a) < vec(b)
    else if !mask(b) then if !mask(a) then next(a, b) else false
    else if !mask(a) then true
    else if vec(a) == vec(b) then next(a, b)
    else vec(a) < vec(b)

  override def descNullsFirst(a: Int, b: Int): Boolean =
    if mask == null then if vec(a) == vec(b) then next(a, b) else vec(a) > vec(b)
    else if !mask(b) then if !mask(a) then next(a, b) else false
    else if !mask(a) then true
    else if vec(a) == vec(b) then next(a, b)
    else vec(a) > vec(b)
