/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.Series
import pd.internal.index.BaseIndex

/**
  * Exception raised when the BaseIndex does not have the expected length.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class BaseIndexException(msg: String) extends PanDataException(msg):

  // using Java-like constructors to circumvent NoClassDefFoundError in inline code

  def this(series: Seq[Series[?]]) =
    this(
      "The underlying data vectors have different lengths. Use DataFrame.from to convert automatically. Found:\n" +
        series.map(_.info).mkString("\n")
    )

  def this(baseIndex1: BaseIndex, baseIndex2: BaseIndex) =
    this(
      s"The underlying base indices $baseIndex1 and $baseIndex2 are not equal."
    )

  def this(series1: Series[?], series2: Series[?]) =
    this(
      s"Series '${series1.name}' and '${series2.name}' have different base indices: ${series1.index} and ${series2.index}."
    )
