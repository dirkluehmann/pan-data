/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

/**
  * Base exception for the pan-data package.
  *
  * @param msg
  *   Message.
  * @param cause
  *   Underlying exception or null.
  * @since 0.1.0
  */
class PanDataException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause: Throwable)
