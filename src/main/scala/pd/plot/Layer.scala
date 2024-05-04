/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot

import pd.plot.internal.{Config, Grammar}

import scala.collection.mutable
import scala.language.dynamics

/**
  * Layer for view compositions such as "layer", "concat, "hconcat" and "vconcat".
  *
  * @param cfg
  *   Plotting configuration.
  * @since 0.1.0
  */
class Layer private[pd] (cfg: Config) extends Grammar[Layer](cfg):

  /** @since 0.1.0 */
  protected def create(config: Config): Layer = new Layer(config)

  /** @since 0.1.0 */
  private[pd] def toStruct: Struct = Struct.create(assemble(cfg.data)*)

/**
  * Layer for view compositions such as "layer", "concat, "hconcat" and "vconcat".
  *
  * @since 0.1.0
  */
object Layer:
  /**
    * Creates a new [[Layer]].
    *
    * @return
    *   Empty [[Layer]].
    * @since 0.1.0
    */
  def apply(): Layer = new Layer(Config())
