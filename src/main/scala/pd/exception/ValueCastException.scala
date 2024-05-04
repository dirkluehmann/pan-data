/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.internal.utils.TypeString.typeString

/**
  * Exception raised the value of an element cannot be cast into the required type.
  *
  * @param value
  *   Value found.
  * @param target
  *   Target type.
  * @since 0.1.0
  */
class ValueCastException(value: Any, target: String = "")
    extends PanDataException(
      s"Value cannot be cast from type ${typeString(value)}" +
        (if target.nonEmpty then s" to type $target." else ".")
    )
