/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import java.lang.Runtime

/**
  * Global settings.
  *
  * @since 0.1.0
  */
object Settings:

  // *** MULTI-THREADING VARS ***

  /**
    * Number of available processor cores.
    *
    * @since 0.1.0
    */
  private[pd] val cores: Int = Runtime.getRuntime.availableProcessors

  /**
    * Minimum number of rows to activate multi threading.
    *
    * @since 0.1.0
    */
  private[pd] var minThreadedRows: Int = 1000

  /**
    * If multi threading is activated.
    *
    * @since 0.1.0
    */
  private[pd] var threaded: Boolean = true

  /**
    * Number of partitions.
    *
    * @since 0.1.0
    */
  private[pd] var partitions: Int = cores * 3

  // *** PRINTING VARS ***

  /**
    * Width of a column for a DataFrame (in characters).
    *
    * @since 0.1.0
    */
  private[pd] var printColWidth: Int = 15

  /**
    * Width of a column for Series (in characters).
    *
    * @since 0.1.0
    */
  private[pd] var printColWidthSeries: Int = 15

  /**
    * Width of the index column (in characters).
    *
    * @since 0.1.0
    */
  private[pd] var printIndexWidth: Int = 11

  /**
    * Width of the index column for a DataMap (in characters).
    *
    * @since 0.1.0
    */
  private[pd] var printIndexWidthMapped: Int = 21

  /**
    * Whether to displays the type name using JVM conventions (true) or Scala (false).
    *
    * @since 0.1.0
    */
  private[pd] var printJavaType: Boolean = false

  /**
    * Default number of rows to display.
    *
    * @since 0.1.0
    */
  private[pd] var printRowLength: Int = 20

  /**
    * Maximal width of a displayed table (in characters).
    *
    * @since 0.1.0
    */
  private[pd] var printWidth: Int = 200

  // *** PUBLIC ***

  /**
    * Sets the configuration for printing DataFrames/Series.
    *
    * @param width
    *   Maximal width of a displayed table (in characters).
    * @param colWidth
    *   Width of a column (in characters).
    * @param rowLength
    *   Default number of rows to display.
    * @since 0.1.0
    */
  def printing(width: Int = -1, colWidth: Int = -1, rowLength: Int = 20): Unit =
    if width >= 0 then printWidth = width
    if colWidth > 0 then printColWidth = colWidth
    if rowLength > 0 then printRowLength = rowLength

  /**
    * Sets processing to multi core using default parameter for partitioning.
    *
    * @since 0.1.0
    */
  def setMultiCoreDefaults(): Unit =
    threaded = true
    partitions = cores * 4
    minThreadedRows = 1000

  /**
    * Sets processing to single core.
    *
    * @since 0.1.0
    */
  def setSingleCore(): Unit =
    threaded = false

  // *** PRIVATE ***

  /**
    * Sets the convention for displaying types names.
    *
    * @param value
    *   If true, displays the type name using JVM conventions.
    * @since 0.1.0
    */
  private[pd] def setPrintTypesAsJava(value: Boolean): Unit =
    printJavaType = value

  /**
    * Test default.
    *
    * @since 0.1.0
    */
  private[pd] def setTestDefaults(): Unit =
    setMultiCoreDefaults()

  /**
    * Multi core default for tests suppressing single core execution for a small number of rows.
    *
    * @since 0.1.0
    */
  private[pd] def setTestMultiCore(): Unit =
    threaded = true
    partitions = 3
    minThreadedRows = 0

  /**
    * Sets processing to single core for tests.
    *
    * @since 0.1.0
    */
  private[pd] def setTestSingleCore(): Unit =
    threaded = false
