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

class SeqIndexTest extends BaseTest {

  private val base = UniformIndex(6)
  private val i = SeqIndex(base, Seq(4, 2, 5, 0))

  private def assertEq(index1: BaseIndex, index2: BaseIndex) =
    if !index1.equals(index2) then
      println(s"index1: $index1")
      println(s"index2: $index2")
    assert(index1.equals(index2))

  test("base") {
    i.base shouldBe base
  }

  test("contains") {
    i.contains(0) shouldBe true
    i.contains(1) shouldBe false
    i.contains(2) shouldBe true
    i.contains(3) shouldBe false
    i.contains(4) shouldBe true
    i.contains(5) shouldBe true
    assertThrows[IndexBoundsException](i.contains(-1))
    assertThrows[IndexBoundsException](i.contains(6))
  }

  test("describe") {
    i.describe should startWith("Sequential Index")
    i.describe should endWith("with 4 elements.")
  }

  test("equivalent") {
    i.equals(i) shouldBe true
    i.equals(SeqIndex(UniformIndex(6), Seq(4, 2, 5, 0))) shouldBe true
    i.equals(base.slice(Seq(4, 2, 5, 0))) shouldBe true

    i.equals(base.slice(Seq(4, 2, 5))) shouldBe false
    i.equals(base.slice(Seq(4, 2, 5, 0, 1))) shouldBe false
    i.equals(base.slice(Seq(0, 4, 2, 5))) shouldBe false
    i.equals(base) shouldBe false
    i.equals(SeqIndex(UniformIndex(7), Seq(4, 2, 5, 0))) shouldBe false

    SeqIndex(base, Seq(0, 1, 2, 3, 4, 5)).equals(UniformIndex(6)) shouldBe true
    SeqIndex(base, Seq(1, 2, 3)).equals(SlicedIndex(base, 1 to 3)) shouldBe true
    SeqIndex(base, Seq(0, 1, 2, 3, 4)).equals(UniformIndex(6)) shouldBe false
    SeqIndex(base, Seq(1, 2, 3)).equals(SlicedIndex(base, 1 to 4)) shouldBe false
  }

  test("hasSameBase") {
    i.hasSameBase(SeqIndex(UniformIndex(6), Seq(4))) shouldBe true
    i.hasSameBase(UniformIndex(6)) shouldBe true
    i.hasSameBase(SeqIndex(UniformIndex(7), Seq(4, 2, 5, 0))) shouldBe false
  }

  test("hasSubIndices") {
    i.hasSubIndices(i) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(4, 2, 5, 0))) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(4, 2, 0, 5))) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(4, 2, 0, 5))) shouldBe 0
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(4, 2, 5))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(7), Seq(4, 2, 5))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(5))) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(4, 1))) shouldBe -1
    i.hasSubIndices(SeqIndex(UniformIndex(6), Seq(1))) shouldBe -1
  }

  test("head") {
    assertEq(i.head(0), SeqIndex(base, Seq()))
    assertEq(i.head(1), SeqIndex(base, Seq(4)))
    assertEq(i.head(2), SeqIndex(base, Seq(4, 2)))
    assertEq(i.head(3), SeqIndex(base, Seq(4, 2, 5)))
    assertEq(i.head(4), i)
    assertEq(i.head(5), i)
  }

  test("includes") {
    i.includes(i) shouldBe 0
    i.includes(SeqIndex(UniformIndex(6), Seq(4, 2, 5, 0))) shouldBe 0
    i.includes(SeqIndex(UniformIndex(6), Seq(4, 2, 0, 5))) shouldBe 0
    i.includes(SeqIndex(UniformIndex(7), Seq(4, 2, 0, 5))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(4, 2, 5))) shouldBe 1
    i.includes(SeqIndex(UniformIndex(7), Seq(4, 2, 5))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(5))) shouldBe 1
    i.includes(SeqIndex(UniformIndex(6), Seq())) shouldBe 1
    i.includes(SeqIndex(UniformIndex(6), Seq(4, 1))) shouldBe -1
    i.includes(SeqIndex(UniformIndex(6), Seq(1))) shouldBe -1
  }

  test("isBase") {
    i.isBase shouldBe false
  }

  test("isBijective") {
    i.isBijective shouldBe false
    SeqIndex(base, Seq(4, 2, 5, 0, 1, 3)).isBijective shouldBe true
  }

  test("isEmpty") {
    i.isEmpty shouldBe false
    SeqIndex(base, Seq()).isEmpty shouldBe true
  }

  test("isContained") {
    i.isContained(0) shouldBe true
    i.isContained(1) shouldBe false
    i.isContained(2) shouldBe true
    i.isContained(3) shouldBe false
    i.isContained(4) shouldBe true
    i.isContained(5) shouldBe true
    i.isContained(-1) shouldBe false
    i.isContained(6) shouldBe false
  }

  test("iterator") {
    val it = i.iterator
    it.hasNext shouldBe true
    it.next shouldBe 4
    it.hasNext shouldBe true
    it.next shouldBe 2
    it.hasNext shouldBe true
    it.next shouldBe 5
    it.hasNext shouldBe true
    it.next shouldBe 0
    it.hasNext shouldBe false
  }

  test("length") {
    i.length shouldBe 4
  }

  test("max") {
    i.max shouldBe 5
  }

  test("nonEmpty") {
    i.nonEmpty shouldBe true
    SeqIndex(base, Seq()).nonEmpty shouldBe false
  }

  test("over") {
    val index = s2A.index.slice(Seq(2, 1))
    index.over(s2A.data).toSeq shouldBe Seq(None, Some(3))
  }

  test("overWithIndex") {
    val index = s2A.index.slice(Seq(2, 1))
    index.overWithIndex(s2A.data).toSeq shouldBe Seq((2, None), (1, Some(3)))
  }

  test("partitions") {
    Settings.setTestSingleCore()
    {
      val p = i.partitions
      p.length shouldBe 1
      (p(0).start, p(0).end) shouldBe (0, 3)
      p(0).toSeq shouldBe Seq(4, 2, 5, 0)
    }
    Settings.setTestMultiCore()
    {
      val p = i.partitions
      p.length shouldBe 3
      (p(0).start, p(0).end) shouldBe (0, 0)
      p(0).toSeq shouldBe Seq(4)
      (p(1).start, p(1).end) shouldBe (1, 1)
      p(1).toSeq shouldBe Seq(2)
      (p(2).start, p(2).end) shouldBe (2, 3)
      p(2).toSeq shouldBe Seq(5, 0)
    }
    Settings.setTestDefaults()
  }

  test("slice(range)") {
    assertEq(i.slice(0 to 5), SeqIndex(base, Seq(0, 2, 4, 5)))
    assertEq(i.slice(0 to 3), SeqIndex(base, Seq(0, 2)))
    assertEq(i.slice(3 until 5), SeqIndex(base, Seq(4)))
  }

  test("slice(seq: Seq)") {
    assertEq(i.slice(Seq(5, 4, 3, 2, 1, 0)), SeqIndex(base, Seq(5, 4, 2, 0)))
    assertEq(i.slice(Seq(0, 1, 4)), SeqIndex(base, Seq(0, 4)))
    assertEq(i.slice(Seq(3)), SeqIndex(base, Seq()))
  }

  test("slice(seq: Array)") {
    assertEq(i.slice(Array(5, 4, 3, 2, 1, 0)), SeqIndex(base, Seq(5, 4, 2, 0)))
    assertEq(i.slice(Array(0, 1, 4)), SeqIndex(base, Seq(0, 4)))
    assertEq(i.slice(Array(3)), SeqIndex(base, Seq()))
  }

  test("slice(series)") {
    assertEq(i.slice(Series(true, true, true, true, true, true)), SeqIndex(base, Seq(0, 2, 4, 5)))
    assertEq(i.slice(Series(true, false, true, false, true, true)), SeqIndex(base, Seq(0, 2, 4, 5)))
    assertEq(i.slice(Series(false, true, false, true, false, false)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, false, false, false, false)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(true, false, false, false, false, false)), SeqIndex(base, Seq(0)))
    assertEq(i.slice(Series(false, true, true, true, true, false)), SeqIndex(base, Seq(2, 4)))

    val rev = Seq(5, 0, 4, 3, 2, 1)
    assertEq(i.slice(Series(true, true, true, true, true, true).apply(rev)), SeqIndex(base, Seq(5, 0, 4, 2)))
    assertEq(i.slice(Series(true, false, true, false, true, true).apply(rev)), SeqIndex(base, Seq(5, 0, 4, 2)))
    assertEq(i.slice(Series(false, true, false, true, false, false).apply(rev)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, false, false, false, false).apply(rev)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(true, false, false, false, false, false).apply(rev)), SeqIndex(base, Seq(0)))
    assertEq(i.slice(Series(false, true, true, true, true, false).apply(rev)), SeqIndex(base, Seq(4, 2)))

    assertEq(i.slice(Series(true, true, true, true, true, true)), SeqIndex(base, Seq(0, 2, 4, 5)))
    assertEq(i.slice(Series(true, null, true, null, true, true)), SeqIndex(base, Seq(0, 2, 4, 5)))
    assertEq(i.slice(Series(false, true, null, true, false, false)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(null, null, null, null, false, false)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(true, null, null, null, false, false)), SeqIndex(base, Seq(0)))
    assertEq(i.slice(Series(null, true, true, true, true, false)), SeqIndex(base, Seq(2, 4)))

    val sl = Seq(5, 0, 4, 3)
    assertEq(i.slice(Series(true, true, true, true, true, true).apply(sl)), SeqIndex(base, Seq(5, 0, 4)))
    assertEq(i.slice(Series(true, false, true, false, true, true).apply(sl)), SeqIndex(base, Seq(5, 0, 4)))
    assertEq(i.slice(Series(false, true, false, true, false, false).apply(sl)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(false, false, false, false, false, false).apply(sl)), SeqIndex(base, Seq()))
    assertEq(i.slice(Series(true, false, false, false, false, false).apply(sl)), SeqIndex(base, Seq(0)))
    assertEq(i.slice(Series(false, true, true, true, true, false).apply(sl)), SeqIndex(base, Seq(4)))

    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true, true, true)), SeqIndex(base, Seq(4, 2, 5, 0)))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false, false, false)), SeqIndex(base, Seq()))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(true, true, true, true, true)), SeqIndex(base, Seq(4, 2, 5, 0)))
    )
    assertThrows[BaseIndexException](
      assertEq(i.slice(Series(false, false, false, false, false)), SeqIndex(base, Seq()))
    )
  }

  test("sorted") {
    assertEq(i.sorted, SeqIndex(base, Seq(0, 2, 4, 5)))
  }

  test("sorted(key) / sortBy") {
    assertEq(s1A.index.sorted(s1A.data -> asc), SeqIndex(s1A.index.base, Seq(2, 1, 4, 0, 3)))
    assertEq(s1A.index.sorted(s1A.data -> ascNullsFirst), SeqIndex(s1A.index.base, Seq(2, 1, 4, 0, 3)))
    assertEq(s1A.index.sorted(s1A.data -> desc), SeqIndex(s1A.index.base, Seq(3, 0, 4, 1, 2)))
    assertEq(s1A.index.sorted(s1A.data -> descNullsFirst), SeqIndex(s1A.index.base, Seq(3, 0, 4, 1, 2)))

    assertEq(s2B.index.sorted(s2B.data -> asc), SeqIndex(s2B.index.base, Seq(1, 2, 4, 3, 0)))
    assertEq(s2B.index.sorted(s2B.data -> ascNullsFirst), SeqIndex(s2B.index.base, Seq(0, 1, 2, 4, 3)))
    assertEq(s2B.index.sorted(s2B.data -> desc), SeqIndex(s2B.index.base, Seq(3, 4, 1, 2, 0)))
    assertEq(s2B.index.sorted(s2B.data -> descNullsFirst), SeqIndex(s2B.index.base, Seq(0, 3, 4, 1, 2)))
  }

  test("tail") {
    assertEq(i.tail(0), SeqIndex(base, Seq()))
    assertEq(i.tail(1), SeqIndex(base, Seq(0)))
    assertEq(i.tail(2), SeqIndex(base, Seq(5, 0)))
    assertEq(i.tail(3), SeqIndex(base, Seq(2, 5, 0)))
    assertEq(i.tail(4), i)
    assertEq(i.tail(5), i)
  }

  test("toSeq") {
    i.toSeq shouldBe Seq(4, 2, 5, 0)
  }

  test("toSeqIndex") {
    assertEq(i.toSeqIndex, i)
  }

  test("toString") {
    i.toString shouldBe "Index (4, 2, 5, 0)"
  }

  test("SeqIndexIterator") {
    {
      val it = i.SeqIndexIterator(4)
      it.start shouldBe 0
      it.end shouldBe 3
      it.hasNext shouldBe true
      it.next shouldBe 4
      it.hasNext shouldBe true
      it.next shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 5
      it.hasNext shouldBe true
      it.next shouldBe 0
      it.hasNext shouldBe false
    }
    {
      val it = i.SeqIndexIterator((1, 2))
      it.start shouldBe 1
      it.end shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 5
      it.hasNext shouldBe false
    }
    {
      val it = i.SeqIndexIterator(1, 2)
      it.start shouldBe 1
      it.end shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 5
      it.hasNext shouldBe false
    }
  }

  test("apply(base, indices: Seq)") {
    val index = SeqIndex(base, Seq(0, 1, 2, 3, 4, 5))
    assertEq(index, base)
    assertThrows[IllegalIndex](SeqIndex(base, Seq(0, 7)))
    assertThrows[IllegalIndex](SeqIndex(base, Seq(-1)))
    assertThrows[IllegalIndex](SeqIndex(base, Seq(0, 1, 2, 0)))
  }

  test("apply(base, indices: Array)") {
    val index = SeqIndex(base, Array(0, 1, 2, 3, 4, 5))
    assertEq(index, base)
    assertThrows[IllegalIndex](SeqIndex(base, Array(0, 7)))
    assertThrows[IllegalIndex](SeqIndex(base, Array(-1)))
    assertThrows[IllegalIndex](SeqIndex(base, Array(0, 1, 2, 0)))
  }

  test("unchecked") {
    assertEq(SeqIndex.unchecked(base, i.toSeq.toArray), i)
  }

}
