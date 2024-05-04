/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.utils

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
  * Use this trait to enforce that the type Nothing is not inferred.
  *
  * @since 0.1.0
  */
@implicitNotFound("Nothing was inferred")
sealed trait RequireType[-T]

/**
  * Ambiguous overload to enforce an compiler error if the type is inferred as Nothing.
  *
  * @since 0.1.0
  */
object RequireType {
  implicit object ` A type parameter is required here since otherwise it is resolved to Nothing `
      extends RequireType[Nothing]
  implicit object RequireTypeAmbiguous extends RequireType[Any]
}
