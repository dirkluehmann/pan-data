/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.internal.series.SeriesData

import scala.collection.immutable.SeqMap
import scala.language.implicitConversions

/**
  * Order-preserving mapping from column names to [[pd.internal.series.SeriesData]].
  *
  * @since 0.1.0
  */
private[pd] type ColIndex = SeqMap[String, SeriesData[?]]

/**
  * Order-preserving mapping from column names to [[pd.internal.series.SeriesData]].
  *
  * @since 0.1.0
  */
private[pd] object ColIndex:

  /**
    * Empty constructor.
    *
    * @return
    *   Empty [[ColIndex]].
    * @since 0.1.0
    */
  def apply(): ColIndex = SeqMap[String, SeriesData[?]]()

  /**
    * Constructor for one element.
    *
    * @param element
    *   Tuple of column names and SeriesData.
    * @return
    *   [[ColIndex]] with entry `element`.
    * @since 0.1.0
    */
  def apply(element: (String, SeriesData[?])): ColIndex = SeqMap(element)

  /**
    * Converts a sequence with column name to [[pd.internal.series.SeriesData]] mappings into a [[ColIndex]].
    *
    * @param seq
    *   Sequence with tuples of column names and [[pd.internal.series.SeriesData]].
    * @return
    *   [[ColIndex]].
    * @since 0.1.0
    */
  implicit def toMap(seq: Seq[(String, SeriesData[?])]): ColIndex = SeqMap.from[String, SeriesData[?]](seq)

  /**
    * Implicit extension class.
    *
    * @param colMap
    *   [[ColIndex]] instance.
    * @since 0.1.0
    */
  implicit class ColMapOps(colMap: ColIndex):

    /**
      * Implements implicit prepending of tuples of column names and [[pd.internal.series.SeriesData]].
      *
      * @param element
      *   Element to be prepended.
      * @since 0.1.0
      */
    def prepend(element: (String, SeriesData[?])): ColIndex =
      ColIndex(element) ++ colMap
