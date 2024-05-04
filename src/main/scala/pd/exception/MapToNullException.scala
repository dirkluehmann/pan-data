/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

/**
  * Exception raised when the map function returns `null` for an element.
  *
  * @since 0.1.0
  */
class MapToNullException extends PanDataException("Map function cannot return null value.")
