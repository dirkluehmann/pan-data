/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io

import pd.io.csv.CsvReader

import scala.language.implicitConversions

/**
  * Adapter to read a DataFrame via `DataFrame.read`.
  *
  * @since 0.1.0
  */
class ReadAdapter

/**
  * Adapter to read a DataFrame via `DataFrame.read`.
  *
  * @since 0.1.0
  */
object ReadAdapter:

  /** @since 0.1.0 */
  implicit def csv(adapter: ReadAdapter): CsvReader.Adapter = CsvReader.Adapter()
