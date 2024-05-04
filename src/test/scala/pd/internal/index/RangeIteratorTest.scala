/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.BaseTest

class RangeIteratorTest extends BaseTest {

  test("apply(start, end)") {
    val it = RangeIterator(0, 3)
    it.hasNext shouldBe true
    it.next shouldBe 0
    it.hasNext shouldBe true
    it.next shouldBe 1
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe true
    it.next shouldBe 3
    it.hasNext shouldBe false
  }

  test("apply((start, end))") {
    val it = RangeIterator((0, 3))
    it.hasNext shouldBe true
    it.next shouldBe 0
    it.hasNext shouldBe true
    it.next shouldBe 1
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe true
    it.next shouldBe 3
    it.hasNext shouldBe false
  }

  test("apply(length)") {
    val it = RangeIterator(3)
    it.hasNext shouldBe true
    it.next shouldBe 0
    it.hasNext shouldBe true
    it.next shouldBe 1
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe false
  }

}
