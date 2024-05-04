/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.DataFrame

/**
  * Exception raised when a column is not found.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class ColumnNotFoundException(msg: String) extends PanDataException(msg)

/**
  * Exception raised when a column is not found.
  *
  * @since 0.1.0
  */
private[pd] object ColumnNotFoundException:

  def apply(msg: String): ColumnNotFoundException = new ColumnNotFoundException(msg)

  def apply(df: DataFrame, col: String): ColumnNotFoundException = new ColumnNotFoundException(
    s"Column $col not found. Found:\n${df.info}"
  )

  def apply(df: DataFrame, cols: Seq[String]): ColumnNotFoundException = new ColumnNotFoundException(
    s"Column(s) ${cols.mkString(", ")} not found. Found:\n${df.info}"
  )
