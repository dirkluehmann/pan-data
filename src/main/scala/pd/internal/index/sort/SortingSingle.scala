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
  * Single column sorting.
  *
  * @param data
  *   Data to be sorted.
  * @param order
  *   Order.
  * @param ordering
  *   Implicit ordering.
  * @since 0.1.0
  */
class SortingSingle[T](data: SeriesData[T], order: Order)(implicit ordering: Ordering[T]) extends Sorting:

  private[pd] val mask = data.mask
  private[pd] val vec = data.vector

  def sort: (Int, Int) => Boolean = order match
    case Order.asc            => asc
    case Order.desc           => desc
    case Order.ascNullsFirst  => ascNullsFirst
    case Order.descNullsFirst => descNullsFirst

  def asc(a: Int, b: Int): Boolean =
    if mask == null then vec(a) < vec(b)
    else if !mask(b) then if !mask(a) then false else true
    else if !mask(a) then false
    else vec(a) < vec(b)

  def desc(a: Int, b: Int): Boolean =
    if mask == null then vec(a) > vec(b)
    else if !mask(b) then if !mask(a) then false else true
    else if !mask(a) then false
    else vec(a) > vec(b)

  def ascNullsFirst(a: Int, b: Int): Boolean =
    if mask == null then vec(a) < vec(b)
    else if !mask(b) then false
    else if !mask(a) then true
    else vec(a) < vec(b)

  def descNullsFirst(a: Int, b: Int): Boolean =
    if mask == null then vec(a) > vec(b)
    else if !mask(b) then false
    else if !mask(a) then true
    else vec(a) > vec(b)
