/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.BaseTest
import pd.internal.index.ColIndex.ColMapOps

class ColIndexTest extends BaseTest:

  test("prepend") {
    ColIndex("A" -> s1A.data).prepend("B" -> s1B.data).keys.toSeq shouldBe Seq("B", "A")
  }
