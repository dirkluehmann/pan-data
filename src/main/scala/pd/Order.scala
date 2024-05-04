/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

enum Order:
  case
  /**
    * Ascending order, nulls last.
    * @since 0.1.0
    */
  asc,
  /**
    * Descending order, nulls last.
    * @since 0.1.0
    */
  desc,
  /**
    * Ascending order, nulls first.
    * @since 0.1.0
    */
  ascNullsFirst,
  /**
    * Descending order, null first.
    * @since 0.1.0
    */
  descNullsFirst
