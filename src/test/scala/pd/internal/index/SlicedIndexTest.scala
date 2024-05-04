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

class SlicedIndexTest extends BaseTest {

  private val base = UniformIndex(6)
  private val i = SlicedIndex(base, 2 to 3)

  private def assertEq(index1: BaseIndex, index2: BaseIndex) =
    if !index1.equals(index2) then
      println(s"index1: $index1")
      println(s"index2: $index2")
    assert(index1.equals(index2))

  test("base") {
    i.base shouldBe base
  }

  test("contains") {
    i.contains(0) shouldBe false
    i.contains(1) shouldBe false
    i.contains(2) shouldBe true
    i.contains(3) shouldBe true
    i.contains(4) shouldBe false
    i.contains(5) shouldBe false
    assertThrows[IndexBoundsException](i.contains(-1))
    assertThrows[IndexBoundsException](i.contains(6))
  }

  test("describe") {
    i.describe should startWith("Sliced Index")
    i.describe should endWith("with 2 elements.")
  }

  test("equivalent") {
    i.equals(i) shouldBe true
    i.equals(SeqIndex(UniformIndex(6), Seq(2, 3))) shouldBe true
    i.equals(base.slice(2 to 3)) shouldBe true

    i.equals(base.slice(2 to 4)) shouldBe false
    i.equals(base.slice(Seq(1, 2))) shouldBe false
    i.equals(base.slice(Seq(2, 3, 4))) shouldBe false
    i.equals(base) shouldBe false
    i.equals(SlicedIndex(UniformIndex(7), 2 to 3)) shouldBe false

    SlicedIndex(base, 0 to 5).equals(UniformIndex(6)) shouldBe true
    SlicedIndex(base, 1 to 5).equals(UniformIndex(6)) shouldBe false
  }

  test("hasSameBase") {
    i.hasSameBase(SeqIndex(UniformIndex(6), Seq(4))) shouldBe true
    i.hasSameBase(UniformIndex(6)) shouldBe true
    i.hasSameBase(SeqIndex(UniformIndex(7), Seq(4, 2, 5, 0))) shouldBe false
  }

  test("hasSubIndices") {
    i.hasSubIndices(i) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(2, 3))) shouldBe 0
    i.hasSubIndices(SlicedIndex(UniformIndex(6), 2 to 3)) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(3, 2))) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(2, 3))) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(2))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(3))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(3, 1))) shouldBe -1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(5))) shouldBe -1
  }

  test("head") {
    assertEq(i.head(0), SeqIndex(base, Seq()))
    assertEq(i.head(1), SeqIndex(base, Seq(2)))
    assertEq(i.head(2), SeqIndex(base, Seq(2, 3)))
    assertEq(i.head(3), i)
    assertEq(i.head(4), i)
  }

  test("includes") {
    i.includes(i) shouldBe 0
    i.includes(SeqIndex(UniformIndex(6), Seq(2, 3))) shouldBe 0
    i.includes(SlicedIndex(UniformIndex(6), 2 to 3)) shouldBe 0
    i.includes(SeqIndex(UniformIndex(6), Seq(3, 2))) shouldBe 0
    i.includes(SeqIndex(UniformIndex(7), Seq(2, 3))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(2))) shouldBe 1
    i.includes(SeqIndex(UniformIndex(7), Seq(3))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.includes(SeqIndex(UniformIndex(6), Seq(3, 1))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(5))) shouldBe -1
  }

  test("isBase") {
    i.isBase shouldBe false
  }

  test("isBijective") {
    i.isBijective shouldBe false
    SlicedIndex(base, 0 to 5).isBijective shouldBe true
  }

  test("isEmpty") {
    i.isEmpty shouldBe false
    SeqIndex(base, 0 until 0).isEmpty shouldBe true
  }

  test("isContained") {
    i.isContained(0) shouldBe false
    i.isContained(1) shouldBe false
    i.isContained(2) shouldBe true
    i.isContained(3) shouldBe true
    i.isContained(4) shouldBe false
    i.isContained(5) shouldBe false
    i.isContained(-1) shouldBe false
    i.isContained(6) shouldBe false
  }

  test("iterator") {
    val it = i.iterator
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe true
    it.next shouldBe 3
    it.hasNext shouldBe false
  }

  test("length") {
    i.length shouldBe 2
  }

  test("max") {
    i.max shouldBe 3
  }

  test("nonEmpty") {
    i.nonEmpty shouldBe true
    SeqIndex(base, 0 until 0).nonEmpty shouldBe false
  }

  test("over") {
    val index = s2A.index.slice(2 to 4)
    index.over(s2A.data).toSeq shouldBe Seq(None, Some(8), Some(4))
  }

  test("overWithIndex") {
    val index = s2A.index.slice(2 to 4)
    index.overWithIndex(s2A.data).toSeq shouldBe Seq((2, None), (3, Some(8)), (4, Some(4)))
  }

  test("partitions") {
    val index = SlicedIndex(base, 1 to 5)
    Settings.setTestSingleCore()
    {
      val p = index.partitions
      p.length shouldBe 1
      (p(0).start, p(0).end) shouldBe (1, 5)
      p(0).toSeq shouldBe Seq(1, 2, 3, 4, 5)
    }
    Settings.setTestMultiCore()
    {
      val p = index.partitions
      p.length shouldBe 3
      (p(0).start, p(0).end) shouldBe (1, 1)
      p(0).toSeq shouldBe Seq(1)
      (p(1).start, p(1).end) shouldBe (2, 2)
      p(1).toSeq shouldBe Seq(2)
      (p(2).start, p(2).end) shouldBe (3, 5)
      p(2).toSeq shouldBe Seq(3, 4, 5)
    }
    Settings.setTestDefaults()
  }

  test("slice(range)") {
    assertEq(i.slice(0 to 5), SeqIndex(base, Seq(2, 3)))
    assertEq(i.slice(0 to 2), SeqIndex(base, Seq(2)))
    assertEq(i.slice(0 until 3), SeqIndex(base, Seq(2)))
  }

  test("slice(seq: Seq)") {
    assertEq(i.slice(Seq(5, 4, 3, 2, 1, 0)), SeqIndex(base, Seq(3, 2)))
    assertEq(i.slice(Seq(0, 1, 4)), SeqIndex(base, Seq()))
    assertEq(i.slice(Seq(3)), SeqIndex(base, Seq(3)))
  }

  test("slice(seq: Array)") {
    assertEq(i.slice(Array(5, 4, 3, 2, 1, 0)), SeqIndex(base, Seq(3, 2)))
    assertEq(i.slice(Array(0, 1, 4)), SeqIndex(base, Seq()))
    assertEq(i.slice(Array(3)), SeqIndex(base, Seq(3)))
  }

  test("slice(series)") {
    assertEq(i.slice(Series(true, true, true, true, true, true)), SeqIndex(base, Seq(2, 3)))
    assertEq(i.slice(Series(false, false, true, true, false, false)), SeqIndex(base, Seq(2, 3)))
    assertEq(i.slice(Series(true, true, false, false, true, true)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, false, false, false, false)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, true, false, false, false)), SeqIndex(base, Seq(2)))
    assertEq(i.slice(Series(false, false, false, true, false, false)), SeqIndex(base, Seq(3)))

    val rev = Seq(5, 0, 4, 3, 2, 1)
    assertEq(i.slice(Series(true, true, true, true, true, true).apply(rev)), SeqIndex(base, Seq(3, 2)))
    assertEq(i.slice(Series(false, false, true, true, false, false).apply(rev)), SeqIndex(base, Seq(3, 2)))
    assertEq(i.slice(Series(true, true, false, false, true, true).apply(rev)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, false, false, false, false).apply(rev)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, true, false, false, false).apply(rev)), SeqIndex(base, Seq(2)))
    assertEq(i.slice(Series(false, false, false, true, false, false).apply(rev)), SeqIndex(base, Seq(3)))

    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true, true, true)), SeqIndex(base, Seq(2, 3)))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false, false, false)), SeqIndex(base, Seq()))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true)), SeqIndex(base, Seq(2, 3)))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false)), SeqIndex(base, Seq()))
    )
  }

  test("sorted") {
    assertEq(i.sorted, i)
  }

  test("tail") {
    assertEq(i.tail(0), SeqIndex(base, Seq()))
    assertEq(i.tail(1), SeqIndex(base, Seq(3)))
    assertEq(i.tail(2), SeqIndex(base, Seq(2, 3)))
    assertEq(i.tail(3), i)
    assertEq(i.tail(3), i)
  }

  test("toSeqIndex") {
    assertEq(i.toSeqIndex, SeqIndex(base, Seq(2, 3)))
  }

  test("toString") {
    i.toString shouldBe "Index (2 to 3)"
  }

  test("apply") {
    assertEq(SlicedIndex(base, 0 until 6), base)
    assertEq(SlicedIndex(base, 0 to 5), base)
    assertEq(SlicedIndex(base, 0 to 5 by 2), SeqIndex(base, Seq(0, 2, 4)))
    SlicedIndex(base, 0 to 5 by 2) shouldBe a[SeqIndex]
    SlicedIndex(base, 0 until 0) shouldBe a[SeqIndex]

    assertThrows[IllegalIndex](SlicedIndex(base, 5 to 0 by -1))
    assertThrows[IllegalIndex](SlicedIndex(base, -1 to 4))
    assertThrows[IllegalIndex](SlicedIndex(base, 0 to 6))

  }

}
