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
  * Exception raised when a Requirement has failed.
  *
  * @param msg
  *   Message.
  * @since 0.1.0
  */
class RequirementException(msg: String = "") extends PanDataException(msg)

/**
  * Exception raised when a Requirement has failed.
  *
  * @since 0.1.0
  */
object RequirementException:

  def apply(msg: String): RequirementException =
    new RequirementException(msg)

  def apply(col: String, msg: String): RequirementException =
    new RequirementException(s"Requirement failed for column $col. $msg")

  def apply(df: DataFrame, expected: DataFrame): RequirementException =
    RequirementException(s"Requirement failed for DataFrame.\nExpected:\n${info(expected)}\nFound:\n${info(df)}\n")

  def apply(series: Series[?], expected: String): RequirementException =
    RequirementException(series.name, s"$expected Found:\n${info(series)}")

  def apply(series: Series[?], expected: Series[?]): RequirementException =
    RequirementException(series.name, s"\nExpected:\n${info(expected)}\nFound:\n${info(series)}\n")

  private def info(df: DataFrame): String =
    s"${df.info}${df.toString()}"

  private def info(series: Series[?]): String =
    s"${series.info}${series.toString()}"
