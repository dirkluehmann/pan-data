/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023, initial author: Dirk-Soeren Luehmann
 */

package pd

/**
  * @since 0.1.0
  */
class MutableSeriesTest extends BaseTest:

  // *** PUBLIC MUTABLE INTERFACE ***

  test("copy: Series[T]") {
    val mdf = s2A.mutable
    mdf.set(2, 2)
    val df = mdf.copy
    df shouldBe s1A
    mdf shouldBe s1A
    mdf.set(1, -2)
    mdf shouldBe Series("A")(6, -2, 2, 8, 4)
    df shouldBe s1A
  }

  test("put(ix: Int, value: T): Unit") {
    val m1B = s1B.mutable
    m1B.put(2, 2.8)
    m1B shouldBe Series("B")(23.1, 1.4, 2.8, 7.0, 3.1)
    m1B.put(4, -200)
    m1B shouldBe Series("B")(23.1, 1.4, 2.8, 7.0, -200.0)
    s1B shouldBe Series("B")(23.1, 1.4, 1.4, 7.0, 3.1)
    val m2A = s2A.mutable
    m2A.put(1, 100)
    m2A shouldBe Series("A")(6, 100, null, 8, 4)
    m2A.put(2, -200)
    m2A shouldBe Series("A")(6, 100, -200, 8, 4)
    m2A.put(1, 50)
    m2A shouldBe Series("A")(6, 50, -200, 8, 4)
    s2A shouldBe Series("A")(6, 3, null, 8, 4)
    val m2C = s2CSliced.mutable
    m2C.put(0, "abc")
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", null, null).apply(maskC)
    m2C.put(4, "XYZ")
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", null, "XYZ")
    m2C.put(3, "xyz")
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", "xyz", "XYZ")
    s2CSliced shouldBe Series("C")("ghi", "ABC", "XyZ", null, null).apply(maskC)
  }

  test("series: Series[T]") {
    val mdf = s2A.mutable
    mdf.set(2, 2)
    val df = mdf.series
    df shouldBe s1A
    mdf shouldBe Series.empty[Int]
    mdf.length shouldBe 0
  }

  test("set(ix: Int, value: T): Unit") {
    val m1B = s1B.mutable
    m1B.set(2, 2.8)
    m1B shouldBe Series("B")(23.1, 1.4, 2.8, 7.0, 3.1)
    m1B.set(4, -200)
    m1B shouldBe Series("B")(23.1, 1.4, 2.8, 7.0, -200.0)
    s1B shouldBe Series("B")(23.1, 1.4, 1.4, 7.0, 3.1)
    val m2A = s2A.mutable
    m2A.set(1, 100)
    m2A shouldBe Series("A")(6, 100, null, 8, 4)
    m2A.set(2, -200)
    m2A shouldBe Series("A")(6, 100, -200, 8, 4)
    m2A.set(1, 50)
    m2A shouldBe Series("A")(6, 50, -200, 8, 4)
    s2A shouldBe Series("A")(6, 3, null, 8, 4)
    val m2C = s2CSliced.mutable
    m2C.set(0, "abc")
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", null, null).apply(maskC)
    assertThrows[NoSuchElementException](m2C.set(4, "XYZ"))
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", null, null).apply(maskC)
    assertThrows[NoSuchElementException](m2C.set(3, "xyz"))
    m2C shouldBe Series("C")("abc", "ABC", "XyZ", null, null).apply(maskC)
    s2CSliced shouldBe Series("C")("ghi", "ABC", "XyZ", null, null).apply(maskC)
  }

  test("unset(ix: Int): Unit") {
    val m1B = s1B.mutable
    m1B.unset(2)
    m1B shouldBe Series("B")(23.1, 1.4, null, 7.0, 3.1)
    m1B.unset(4)
    m1B shouldBe Series("B")(23.1, 1.4, null, 7.0, null)
    s1B shouldBe Series("B")(23.1, 1.4, 1.4, 7.0, 3.1)
    val m2A = s2A.mutable
    m2A.unset(1)
    m2A shouldBe Series("A")(6, null, null, 8, 4)
    m2A.unset(2)
    m2A shouldBe Series("A")(6, null, null, 8, 4)
    m2A.unset(1)
    m2A shouldBe Series("A")(6, null, null, 8, 4)
    s2A shouldBe Series("A")(6, 3, null, 8, 4)
    val m2C = s2CSliced.mutable
    m2C.unset(0)
    m2C shouldBe Series("C")(null, "ABC", "XyZ", null, null).apply(maskC)
    m2C.unset(4)
    m2C shouldBe Series("C")(null, "ABC", "XyZ", null, null)
    m2C.unset(3)
    m2C shouldBe Series("C")(null, "ABC", "XyZ", null, null)
    s2CSliced shouldBe Series("C")("ghi", "ABC", "XyZ", null, null).apply(maskC)
    val m1A = s1A.mutable
    m1A(Seq(1, 2, 3))
    m1A.unset(0)
    m1A shouldBe Series("A")(null, 3, 2, 8, null)
    m1A.unset(3)
    m1A shouldBe Series("A")(null, 3, 2, null, null)
    m1A.unset(2)
    m1A shouldBe Series("A")(null, 3, null, null, null)
    m1A.unset(1)
    m1A shouldBe Series[Int](null, null, null, null, null).as("A")
    m1A.set(2, 2)
    m1A shouldBe Series("A")(null, null, 2, null, null)
    s1A shouldBe Series("A")(6, 3, 2, 8, 4)
  }

  // *** REIMPLEMENTED MEMBERS FROM SERIES ***

  test("equals(that: Any): Boolean") {
    s1A.mutable shouldBe s1A
    s1A.mutable shouldBe s1A.mutable
    s1A shouldBe s1A.mutable
  }
