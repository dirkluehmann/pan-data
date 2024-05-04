/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot.internal

import pd.DataFrame
import pd.plot.Struct

import scala.collection.immutable.SeqMap

/**
  * Configuration of Grammar.
  *
  * @param df
  *   DataFrame.
  * @param current
  *   Current method where arguments might be dynamically added.
  * @param mark
  *   Mark definition.
  * @param encoding
  *   Encoding definition.
  * @param objects
  *   Other definitions.
  * @param data
  *   Special data definition.
  * @param layers
  *   Definition of Layers.
  * @param layerMode
  *   Mode of Layer, e.g. "layer", "concat, "hconcat", "vconcat".
  * @since 0.1.0
  */
private[pd] case class Config(
    df: Option[DataFrame],
    current: String = "",
    mark: Struct = Struct.empty,
    encoding: SeqMap[String, Struct] = SeqMap[String, Struct](),
    objects: SeqMap[String, Struct] = SeqMap[String, Struct](),
    data: Seq[String] = Seq.empty,
    layers: Seq[Struct] = Seq.empty,
    layerMode: String = "",
):

  /** @since 0.1.0 */
  def addData(fields: Seq[String]): Config =
    copy(data = (data ++ fields).distinct)

  /** @since 0.1.0 */
  def setDf(df: DataFrame): Config =
    copy(df = Some(df), current = "")

  /** @since 0.1.0 */
  def addEncoding(key: String, field: String, struct: Struct): Config =
    if field.isEmpty then copy(current = "", encoding = encoding + (key -> struct))
    else copy(current = "", encoding = encoding + (key -> struct), data = data :+ field)

  /** @since 0.1.0 */
  def addEncoding(key: String, fields: Seq[String], struct: Struct): Config =
    copy(current = "", encoding = encoding + (key -> struct), data = data ++ fields)

  /** @since 0.1.0 */
  def addLayers(layersAdded: Seq[Struct]): Config =
    copy(layers = layers ++ layersAdded)

  /** @since 0.1.0 */
  def addObj(key: String, struct: Struct): Config =
    if current.isEmpty then UnsupportedOperationException(s"Cannot add $struct to function call.")
    copy(current = "", objects = objects + (key -> struct))

  /** @since 0.1.0 */
  def setCurrent(key: String): Config =
    copy(current = key)

  /** @since 0.1.0 */
  def setLayerMode(mode: String): Config = copy(layerMode = mode)

  /** @since 0.1.0 */
  def setMark(struct: Struct): Config =
    copy(mark = struct)

/**
  * Configuration of Grammar.
  *
  * @since 0.1.0
  */
private[pd] object Config:

  /** @since 0.1.0 */
  def apply(): Config = new Config(None)

  /** @since 0.1.0 */
  def apply(df: DataFrame): Config = new Config(Some(df))
