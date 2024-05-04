/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.{BaseTest, Settings}

class BaseIndexTest extends BaseTest {

  test("partitioning") {
    Settings.setTestMultiCore()
    BaseIndex.partitioning(5, 10) shouldBe Array((5, 6), (7, 8), (9, 10))
    BaseIndex.partitioning(5, 11) shouldBe Array((5, 6), (7, 8), (9, 11))
    BaseIndex.partitioning(2, 3) shouldBe Array((2, 3))
    BaseIndex.partitioning(2) shouldBe Array((0, 1))
    BaseIndex.partitioning(3) shouldBe Array((0, 0), (1, 1), (2, 2))
    BaseIndex.partitioning(4) shouldBe Array((0, 0), (1, 1), (2, 3))
    BaseIndex.partitioning(12) shouldBe Array((0, 3), (4, 7), (8, 11))
    BaseIndex.partitioning(14) shouldBe Array((0, 3), (4, 7), (8, 13))
    Settings.setTestDefaults()
  }

}
