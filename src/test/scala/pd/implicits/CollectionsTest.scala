/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.{BaseTest, Series}

class CollectionsTest extends BaseTest:

  test("array") {
    import pd.implicits.Collections.array
    Array(1, 2, 3) * Array(1, 2, 3) shouldBe Series(1, 4, 9)
    Array(1, 2, 3) + Array(0.0, 0.0, 0.0) shouldBe Series(1.0, 2.0, 3.0)
    Array(1.1, 1.2, 1.3) + Array(0.0, 0.0, 0.0) shouldBe Series(1.1, 1.2, 1.3)
    Array("a", "A") + Array("b", "B") shouldBe Series("ab", "AB")
    Array(1, 2, 3, 3, 2, 1).last(3) shouldBe Some(3)
    Array(1, 2, 3, 3, 2, 1).apply(1 to 2) shouldBe Array(1, 2, 3, 3, 2, 1).apply(1 to 2)
  }

  test("seq") {
    import pd.implicits.Collections.seq
    Seq(1, 2, 3) * Seq(1, 2, 3) shouldBe Series(1, 4, 9)
    Seq(1, 2, 3) + Seq(0.0, 0.0, 0.0) shouldBe Series(1.0, 2.0, 3.0)
    Seq(1.1, 1.2, 1.3) + Seq(0.0, 0.0, 0.0) shouldBe Series(1.1, 1.2, 1.3)
    Seq("a", "A") + Seq("b", "B") shouldBe Series("ab", "AB")
    Seq(1, 2, 3, 3, 2, 1).last(3) shouldBe Some(3)
    Seq(1, 2, 3, 3, 2, 1).apply(1 to 2) shouldBe Series(1, 2, 3, 3, 2, 1).apply(1 to 2)
  }

  test("<mixed>") {
    import pd.implicits.Collections.*
    Seq(1, 2, 3) * Array(1, 2, 3) shouldBe Series(1, 4, 9)
    Series(1, 2, 3) + Seq(0.0, 0.0, 0.0) shouldBe Series(1.0, 2.0, 3.0)
    Seq(1.1, 1.2, 1.3) + Series(0.0, 0.0, 0.0) shouldBe Series(1.1, 1.2, 1.3)
    Seq("a", "A") + Array("b", "B") shouldBe Series("ab", "AB")
  }
