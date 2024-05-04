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
  * Exception raised when a Series cannot be cast into the required type.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class SeriesCastException(msg: String) extends PanDataException(msg)

/**
  * Exception raised when a Series cannot be cast into the required type.
  * @since 0.1.0
  */
object SeriesCastException:
  def apply(series: Series[?], target: String = ""): SeriesCastException =
    new SeriesCastException(
      s"${series.seriesName} cannot be cast from type ${series.typeString}" +
        (if target.nonEmpty then s" to type $target." else ".")
    )

  def apply(target: Series[?], sources: Seq[Series[?]]): SeriesCastException =
    new SeriesCastException(
      s"""Series cannot be cast to type ${target.typeString} of ${target.seriesName}.
          |Found: ${sources.map(s => s"${s.seriesName}: ${s.typeString}").mkString("", ", ", ".")}
      """.stripMargin
    )
