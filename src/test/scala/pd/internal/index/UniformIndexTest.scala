/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.index

import pd.Order.*
import pd.exception.*
import pd.{BaseTest, Series, Settings}

class UniformIndexTest extends BaseTest {

  private val i = UniformIndex(6)

  private def assertEq(index1: BaseIndex, index2: BaseIndex) =
    if !index1.equals(index2) then
      println(s"index1: $index1")
      println(s"index2: $index2")
    assert(index1.equals(index2))

  test("base") {
    i.base shouldBe i
  }

  test("contains") {
    i.contains(0) shouldBe true
    i.contains(1) shouldBe true
    i.contains(2) shouldBe true
    i.contains(3) shouldBe true
    i.contains(4) shouldBe true
    i.contains(5) shouldBe true
    assertThrows[IndexBoundsException](i.contains(-1))
    assertThrows[IndexBoundsException](i.contains(6))
  }

  test("describe") {
    i.describe should startWith("Uniform Index")
    i.describe should endWith("with 6 elements.")
  }

  test("equivalent") {
    i.equivalent(i) shouldBe true
    i.equals(SeqIndex(UniformIndex(6), Seq(0, 1, 2, 3, 4, 5))) shouldBe true
    i.equals(SlicedIndex(UniformIndex(6), 0 to 5)) shouldBe true

    i.equals(i.slice(2 to 4)) shouldBe false
    i.equals(i.slice(Seq(1, 2))) shouldBe false
    i.equivalent(UniformIndex(5)) shouldBe false
    i.equivalent(UniformIndex(7)) shouldBe false
  }

  test("hasSameBase") {
    i.hasSameBase(SeqIndex(UniformIndex(6), Seq(4))) shouldBe true
    i.hasSameBase(UniformIndex(6)) shouldBe true
    i.hasSameBase(SeqIndex(UniformIndex(7), Seq(4, 2, 5, 0))) shouldBe false
  }

  test("hasSubIndices") {
    i.hasSubIndices(i) shouldBe 0
    i.hasSubIndices(UniformIndex(5)) shouldBe 1
    i.hasSubIndices(UniformIndex(7)) shouldBe -1

    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(0, 1, 2, 3, 4, 5))) shouldBe 0
    i.hasSubIndices(SlicedIndex(UniformIndex(6), 2 to 3)) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(3, 2))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(2, 3))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(2))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(3))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(3))) shouldBe 1

    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(9), Seq(7, 8))) shouldBe -1
  }

  test("head") {
    assertEq(i.head(0), SeqIndex(i, Seq()))
    assertEq(i.head(1), SeqIndex(i, Seq(0)))
    assertEq(i.head(2), SeqIndex(i, 0 to 1))
    assertEq(i.head(3), SeqIndex(i, 0 to 2))
    assertEq(i.head(4), SeqIndex(i, 0 to 3))
    assertEq(i.head(5), SeqIndex(i, 0 to 4))
    assertEq(i.head(6), i)
    assertEq(i.head(7), i)
  }

  test("includes") {
    i.includes(i) shouldBe 0
    i.includes(UniformIndex(5)) shouldBe -1
    i.includes(UniformIndex(7)) shouldBe -1

    i.includes(SeqIndex(UniformIndex(6), Seq(0, 1, 2, 3, 4, 5))) shouldBe 0
    i.includes(SlicedIndex(UniformIndex(6), 2 to 3)) shouldBe 1
    i.includes(SeqIndex(UniformIndex(6), Seq(3, 2))) shouldBe 1
    i.includes(SeqIndex(UniformIndex(7), Seq(2, 3))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(2))) shouldBe 1
    i.includes(SeqIndex(UniformIndex(7), Seq(3))) shouldBe -1

    i.includes(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.includes(SeqIndex(UniformIndex(9), Seq(7, 8))) shouldBe -1
  }

  test("isBase") {
    i.isBase shouldBe true
  }

  test("isBijective") {
    i.isBijective shouldBe true
  }

  test("isEmpty") {
    i.isEmpty shouldBe false
    UniformIndex(0).isEmpty shouldBe true
  }

  test("isContained") {
    i.isContained(0) shouldBe true
    i.isContained(1) shouldBe true
    i.isContained(2) shouldBe true
    i.isContained(3) shouldBe true
    i.isContained(4) shouldBe true
    i.isContained(5) shouldBe true
    i.isContained(-1) shouldBe false
    i.isContained(6) shouldBe false
  }

  test("iterator") {
    val it = i.iterator
    it.hasNext shouldBe true
    it.next shouldBe 0
    it.hasNext shouldBe true
    it.next shouldBe 1
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe true
    it.next shouldBe 3
    it.hasNext shouldBe true
    it.next shouldBe 4
    it.hasNext shouldBe true
    it.next shouldBe 5
    it.hasNext shouldBe false
  }

  test("length") {
    i.length shouldBe 6
  }

  test("max") {
    i.max shouldBe 5
  }

  test("nonEmpty") {
    i.nonEmpty shouldBe true
    UniformIndex(0).nonEmpty shouldBe false
  }

  test("over") {
    s2A.index.over(s2A.data).toSeq shouldBe Seq(Some(6), Some(3), None, Some(8), Some(4))
  }

  test("overWithIndex") {
    s2A.index.overWithIndex(s2A.data).toSeq shouldBe Seq(
      (0, Some(6)),
      (1, Some(3)),
      (2, None),
      (3, Some(8)),
      (4, Some(4)),
    )
  }

  test("partitions") {
    Settings.setTestSingleCore()
    {
      val p = i.partitions
      p.length shouldBe 1
      (p(0).start, p(0).end) shouldBe (0, 5)
      p(0).toSeq shouldBe Seq(0, 1, 2, 3, 4, 5)
    }
    Settings.setTestMultiCore()
    {
      val p = i.partitions
      p.length shouldBe 3
      (p(0).start, p(0).end) shouldBe (0, 1)
      p(0).toSeq shouldBe Seq(0, 1)
      (p(1).start, p(1).end) shouldBe (2, 3)
      p(1).toSeq shouldBe Seq(2, 3)
      (p(2).start, p(2).end) shouldBe (4, 5)
      p(2).toSeq shouldBe Seq(4, 5)
    }
    Settings.setTestDefaults()
  }

  test("slice(range)") {
    assertEq(i.slice(0 to 5), i)
    assertEq(i.slice(1 to 3), SeqIndex(i, Seq(1, 2, 3)))
    assertEq(i.slice(0 until 3), SeqIndex(i, Seq(0, 1, 2)))
  }

  test("slice(seq: Seq)") {
    assertEq(i.slice(Seq(5, 4, 3, 2, 1, 0)), SeqIndex(i, Seq(5, 4, 3, 2, 1, 0)))
    assertEq(i.slice(Seq(4, 0, 1)), SeqIndex(i, Seq(4, 0, 1)))
    assertEq(i.slice(Seq()), SeqIndex(i, Seq()))
  }

  test("slice(seq: Array)") {
    assertEq(i.slice(Array(5, 4, 3, 2, 1, 0)), SeqIndex(i, Seq(5, 4, 3, 2, 1, 0)))
    assertEq(i.slice(Array(4, 0, 1)), SeqIndex(i, Seq(4, 0, 1)))
    assertEq(i.slice(Array[Int]()), SeqIndex(i, Seq()))
  }

  test("slice(series)") {
    assertEq(i.slice(Series(true, true, true, true, true, true)), i)
    assertEq(i.slice(Series(false, false, true, true, false, false)), SeqIndex(i, Seq(2, 3)))
    assertEq(i.slice(Series(true, true, false, false, true, true)), SeqIndex(i, Seq(0, 1, 4, 5)))
    assertEq(i.slice(Series(false, false, false, false, false, false)), SeqIndex(i, Seq()))
    assertEq(i.slice(Series(false, false, true, false, false, false)), SeqIndex(i, Seq(2)))
    assertEq(i.slice(Series(false, false, false, true, false, false)), SeqIndex(i, Seq(3)))

    val rev = Seq(5, 0, 4, 3, 2, 1)
    assertEq(i.slice(Series(true, true, true, true, true, true).apply(rev)), SeqIndex(i, rev))
    assertEq(i.slice(Series(false, false, true, true, false, false).apply(rev)), SeqIndex(i, Seq(3, 2)))
    assertEq(i.slice(Series(true, true, false, false, true, true).apply(rev)), SeqIndex(i, Seq(5, 0, 4, 1)))
    assertEq(i.slice(Series(false, false, false, false, false, false).apply(rev)), SeqIndex(i, Seq()))
    assertEq(i.slice(Series(false, false, true, false, false, false).apply(rev)), SeqIndex(i, Seq(2)))
    assertEq(i.slice(Series(false, false, false, true, false, false).apply(rev)), SeqIndex(i, Seq(3)))

    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true, true, true)), i)
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false, false, false)), SeqIndex(i, Seq()))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true)), SeqIndex(i, Seq(0, 1, 2, 3, 4)))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false)), SeqIndex(i, Seq()))
    )
  }

  test("sorted") {
    assertEq(i.sorted, i)
  }

  test("tail") {
    assertEq(i.tail(0), SeqIndex(i, Seq()))
    assertEq(i.tail(1), SeqIndex(i, Seq(5)))
    assertEq(i.tail(2), SeqIndex(i, Seq(4, 5)))
    assertEq(i.tail(3), SeqIndex(i, 3 to 5))
    assertEq(i.tail(4), SeqIndex(i, 2 to 5))
    assertEq(i.tail(5), SeqIndex(i, 1 to 5))
    assertEq(i.tail(6), i)
    assertEq(i.tail(7), i)
  }

  test("toSeqIndex") {
    assertEq(i.toSeqIndex, SeqIndex(i, Seq(0, 1, 2, 3, 4, 5)))
  }

  test("toString") {
    i.toString shouldBe "Index (0 to 5)"
    UniformIndex(0).toString shouldBe "Index (<empty>)"
  }

  test("apply") {
    assertEq(UniformIndex(6), i)
  }

}
