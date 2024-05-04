/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io

import pd.DataFrame
import pd.io.csv.CsvWriter

import scala.language.implicitConversions

/**
  * Adapter to write a DataFrame via `df.write`.
  *
  * @since 0.1.0
  */
class WriteAdapter(val df: DataFrame)

/**
  * Adapter to write a DataFrame via `df.write`.
  *
  * @since 0.1.0
  */
object WriteAdapter:

  /** @since 0.1.0 */
  implicit def csv(adapter: WriteAdapter): CsvWriter.Adapter = CsvWriter.Adapter(adapter.df)
