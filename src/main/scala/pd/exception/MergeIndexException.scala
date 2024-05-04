/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.{DataFrame, Series}

/**
  * Exception raised if the a column index cannot be merged into a DataFrame.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class MergeIndexException(msg: String)
    extends PanDataException(
      "Cannot insert a column(s) since the index is not a sub index of the DataFrame." +
        s" Use merge or DataFrame.from to force this behavior. Found:\n$msg\n"
    )

/**
  * Exception raised if the a column index cannot be merged into a DataFrame.
  *
  * @since 0.1.0
  */
private[pd] object MergeIndexException:
  def apply(df: DataFrame) = new MergeIndexException(df.info)
  def apply(series: Series[?]) = new MergeIndexException(series.info)
