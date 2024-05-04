/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot

/**
  * Qualifier for nominal data, also known as categorical data, differentiates between values based only on their names
  * or categories.
  *
  * @since 0.1.0
  */
val Nominal: String = "nominal"

/**
  * Qualifier for ordinal data represents ranked order (1st, 2nd, etc.) by which the data can be sorted.
  *
  * @since 0.1.0
  */
val Ordinal: String = "ordinal"

/**
  * Qualifier for quantitative data expresses some kind of quantity, typically numerical data.
  *
  * @since 0.1.0
  */
val Quantitative: String = "quantitative"

/**
  * Qualifier for temporal data supports date-times and times.
  *
  * @since 0.1.0
  */
val Temporal: String = "temporal"
