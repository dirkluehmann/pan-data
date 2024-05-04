/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.internal.index.UniformIndex

/**
  * Exception raised when the index position is not in the allowed bounds.
  *
  * @param ix
  *   Index position.
  * @param length
  *   Length of index.
  * @since 0.1.0
  */
class IndexBoundsException(ix: Int, length: Int)
    extends PanDataException(
      s"Index $ix is out of bounds. It must be smaller than $length and greater or equal 0."
    )

/**
  * Exception raised when the index position is not in the allowed bounds.
  *
  * @since 0.1.0
  */

object IndexBoundsException:

  def apply(ix: Int, length: Int): IndexBoundsException = new IndexBoundsException(ix, length)

  def apply(ix: Int, index: UniformIndex): IndexBoundsException = new IndexBoundsException(ix, index.length)
