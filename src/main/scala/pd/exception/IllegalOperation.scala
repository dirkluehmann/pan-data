/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.Series

/**
  * Exception raised when the operation is not supported for the object.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class IllegalOperation(msg: String) extends PanDataException(msg)

/**
  * Exception raised when the operation is not supported for the object.
  *
  * @since 0.1.0
  */
private[pd] object IllegalOperation:

  def apply(msg: String): IllegalOperation = new IllegalOperation(msg)

  def apply[T, T2](a: Series[T], operation: String = ""): IllegalOperation =
    new IllegalOperation(
      s"Cannot perform operation ${if operation.isEmpty then "" else operation + " "}on " +
        s"Series[${a.typeString}]."
    )

  def apply[T, T2](a: Series[T], b: Series[T2], operation: String): IllegalOperation =
    new IllegalOperation(
      s"Cannot perform operation ${if operation.isEmpty then "" else operation + " "}on " +
        s"Series[${a.typeString}] and Series[${b.typeString}]."
    )

  def oneColumnGrouping: IllegalOperation = IllegalOperation("Indexing or grouping requires at least one column.")
