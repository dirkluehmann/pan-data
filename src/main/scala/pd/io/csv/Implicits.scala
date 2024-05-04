/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io.csv

import pd.io.{ReadAdapter, WriteAdapter}

import scala.language.implicitConversions

/**
  * Import `pd.io.csv.implicits` to read a DataFrame via `DataFrame.read`.
  *
  * @since 0.1.0
  */
implicit def implicits(adapter: ReadAdapter): CsvReader.Adapter = CsvReader.Adapter()

/**
  * Import `pd.io.csv.implicits` to write a DataFrame via `df.write`.
  *
  * @since 0.1.0
  */
implicit def implicits(adapter: WriteAdapter): CsvWriter.Adapter = CsvWriter.Adapter(adapter.df)
