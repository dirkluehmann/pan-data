/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.*
import pd.internal.index.{SeqIndex, SlicedIndex}
import pd.Order.*

import System.identityHashCode

/**
  * @since 0.1.0
  */
class SeriesTest extends BaseTest:

  test("|(df: DataFrame): DataFrame") {
    val df = df1(Seq("B", "C", "D"))
    (s1A | df) shouldBe df1
    (s1B | df) shouldBe df
    (s1BVar | df) shouldBe df
    (s1A | df(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C(0 to 2), s1D(0 to 2))
    (s1ASorted | df(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C(0 to 2), s1D(0 to 2)).sortValues("A")
    (s1A | df(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A, s1B(Seq(0, 1, 3)), s1C(Seq(0, 1, 3)), s1D(Seq(0, 1, 3)))
    (s1A | df.sortValues("B")) shouldBe df1
    assertThrows[MergeIndexException](s1ASub | df)
    assertThrows[MergeIndexException](s1BSub | df)
  }

  test("|(other: Series[?]): DataFrame") {
    (s1A | s1B) shouldBe df1(Seq("A", "B"))
    (s1D | s1C) shouldBe df1(Seq("D", "C"))
    (s1AVar | s1A) shouldBe df1(Seq("A"))
    (s1A | s1AVar) shouldBe DataFrame.from(s1AVar)
    (s1A | s1BSorted) shouldBe df1(Seq("A", "B"))
    (s1ASorted | s1B) shouldBe df1(Seq("A", "B")).sortValues("A")
    (s1A | s1BSub) shouldBe DataFrame.from(s1A, s1BSub)
    (s1ASorted | s1BSub) shouldBe DataFrame.from(s1A, s1BSub).sortValues("A")
    assertThrows[MergeIndexException](s1ASub | s1B)
  }

  test("::(df: DataFrame): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    (df :: s1D) shouldBe df1
    (df :: s1B) shouldBe df
    (df :: s1BVar) shouldBe DataFrame.from(s1A, s1BVar, s1C)
    (df(0 to 2) :: s1D) shouldBe DataFrame.from(s1A(0 to 2), s1B(0 to 2), s1C(0 to 2), s1D)
    (df :: s1D.sorted) shouldBe df1.sorted[C1]("D")
    (df(Seq(3, 0, 1)) :: s1D) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B(Seq(0, 1, 3)), s1C(Seq(0, 1, 3)), s1D)
    (df.sortValues("B") :: s1A) shouldBe df
    assertThrows[MergeIndexException](df :: s1A(0 to 2))
    assertThrows[MergeIndexException](df :: s1BSub(0 to 2))
  }

  test("::[T2](other: Series[T2]): DataFrame") {
    (s1A :: s1B) shouldBe df1(Seq("A", "B"))
    (s1D :: s1C) shouldBe df1(Seq("D", "C"))
    (s1AVar :: s1A) shouldBe df1(Seq("A"))
    (s1A :: s1AVar) shouldBe DataFrame.from(s1AVar)
    (s1A :: s1BSorted) shouldBe df1(Seq("A", "B")).sortValues("B")
    (s1ASorted :: s1B) shouldBe df1(Seq("A", "B"))
    (s1ASub :: s1B) shouldBe DataFrame.from(s1ASub, s1B)
    (s1ASub :: s1BSorted) shouldBe DataFrame.from(s1ASub, s1B).sortValues("B")
    assertThrows[MergeIndexException](s1A :: s1BSub)
  }

  test("+:(s: String): Series[String]") {
    "x=" +: Series.from(1 to 3) shouldBe (Series("x=1", "x=2", "x=3"))
    "a" +: Series("x", "y") shouldBe (Series("ax", "ay"))
  }

  test("==[T2: ClassTag](series: Series[T2]): Series[Boolean] ") {
    val allTrue = Series.fill(seriesRowLength)(true)
    series1.foreach(s => s == s shouldBe allTrue.as(s.name))
    s2A == s2A shouldBe allTrue(maskA).dense.as("A")
    s2B == s2B shouldBe allTrue(maskB).dense.as("B")
    s2C == s2C shouldBe allTrue(maskC).dense.as("C")
    s2D == s2D shouldBe allTrue(maskD).dense.as("D")
    s1A == s2A shouldBe allTrue(maskA).dense.as("A")
    s1B == s2B shouldBe allTrue(maskB).dense.as("B")
    s1C == s2C shouldBe allTrue(maskC).dense.as("C")
    s1D == s2D shouldBe allTrue(maskD).dense.as("D")

    series1.map(s =>
      series1.filter(identityHashCode(_) != identityHashCode(s)).map(t => assert(!((t == s) equalsByValue allTrue)))
    )

    // Variation in index
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) == s shouldBe allTrue.as(s.name))
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) == s(0 to 4) shouldBe allTrue.as(s.name))
    series1.foreach(s => s(0 to 4) == s shouldBe allTrue.as(s.name))
    series1.foreach(s => s(Seq(0, 1, 3, 2, 4)) == s(Seq(0, 1, 3, 2, 4)) shouldBe allTrue(Seq(0, 1, 3, 2, 4)).as(s.name))
    series1.foreach(s => s(Seq(0, 1, 3, 2, 4)) == s shouldBe allTrue(Seq(0, 1, 3, 2, 4)).as(s.name))
  }

  test("==[T2: ClassTag](v: T2): Series[Boolean]") {
    s1D == C1("C", 2) shouldBe Series(false, false, true, false, false).as("D")
    s2D == C1("C", 2) shouldBe Series(false, null, true, false, false).as("D")
    s2DSliced == C1("C", 2) shouldBe Series(false, null, true, false, false).apply(maskD).as("D")
    s1D == 4 shouldBe Series(false, false, false, false, false).as("D")
  }

  test("==(v: Boolean): Series[Boolean]") {
    val s1 = Series(false, true, true, false, false)
    val s2 = Series(false, true, true, false, null)
    val mask = Seq(true, true, true, true, false)
    val s2Sliced = s2(mask)
    s1 == true shouldBe Series(false, true, true, false, false)
    s2 == true shouldBe Series(false, true, true, false, null)
    s2Sliced == true shouldBe Series(false, true, true, false, null).apply(mask)
  }

  test("==(v: Double): Series[Boolean]") {
    s1B == 1.4 shouldBe Series(false, true, true, false, false).as("B")
    s2B == 1.4 shouldBe Series(null, true, true, false, false).as("B")
    s2BSliced == 1.4 shouldBe Series(null, true, true, false, false).apply(maskB).as("B")
  }

  test("==(v: Int): Series[Boolean]") {
    s1A == 8 shouldBe Series(false, false, false, true, false).as("A")
    s2A == 8 shouldBe Series(false, false, null, true, false).as("A")
    s2ASliced == 8 shouldBe Series(false, false, null, true, false).apply(maskA).as("A")
  }

  test("!=[T2: ClassTag](series: Series[T2]): Series[Boolean]") {
    val allFalse = Series.fill(seriesRowLength)(false)
    series1.foreach(s => s != s shouldBe allFalse.as(s.name))
    s2A != s2A shouldBe allFalse(maskA).dense.as("A")
    s2B != s2B shouldBe allFalse(maskB).dense.as("B")
    s2C != s2C shouldBe allFalse(maskC).dense.as("C")
    s2D != s2D shouldBe allFalse(maskD).dense.as("D")
    s1A != s2A shouldBe allFalse(maskA).dense.as("A")
    s1B != s2B shouldBe allFalse(maskB).dense.as("B")
    s1C != s2C shouldBe allFalse(maskC).dense.as("C")
    s1D != s2D shouldBe allFalse(maskD).dense.as("D")

    series1.map(s =>
      series1.filter(identityHashCode(_) != identityHashCode(s)).map(t => assert(!((t != s) equalsByValue allFalse)))
    )

    // Variation in index
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) != s shouldBe allFalse.as(s.name))
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) != s(0 to 4) shouldBe allFalse.as(s.name))
    series1.foreach(s => s(0 to 4) != s shouldBe allFalse.as(s.name))
    series1.foreach(s =>
      s(Seq(0, 1, 3, 2, 4)) != s(Seq(0, 1, 3, 2, 4)) shouldBe allFalse(Seq(0, 1, 3, 2, 4)).as(s.name)
    )
    series1.foreach(s => s(Seq(0, 1, 3, 2, 4)) != s shouldBe allFalse(Seq(0, 1, 3, 2, 4)).as(s.name))
  }

  test("!=[T2: ClassTag](v: T2): Series[Boolean]") {
    s1D != C1("C", 2) shouldBe !Series(false, false, true, false, false).as("D")
    s2D != C1("C", 2) shouldBe !Series(false, null, true, false, false).as("D")
    s2DSliced != C1("C", 2) shouldBe !Series(false, null, true, false, false).apply(maskD).as("D")
    s1D != 4 shouldBe !Series(false, false, false, false, false).as("D")
  }

  test("!=(v: Boolean): Series[Boolean]") {
    val s1 = Series(false, true, true, false, false)
    val s2 = Series(false, true, true, false, null)
    val mask = Seq(true, true, true, true, false)
    val s2Sliced = s2(mask)
    s1 != true shouldBe !Series(false, true, true, false, false)
    s2 != true shouldBe !Series(false, true, true, false, null)
    s2Sliced != true shouldBe !Series(false, true, true, false, null).apply(mask)
  }

  test("!=(v: Double): Series[Boolean]") {
    s1B != 1.4 shouldBe !Series(false, true, true, false, false).as("B")
    s2B != 1.4 shouldBe !Series(null, true, true, false, false).as("B")
    s2BSliced != 1.4 shouldBe !Series(null, true, true, false, false).apply(maskB).as("B")
  }

  test("!=(v: Int): Series[Boolean]") {
    s1A != 8 shouldBe !Series(false, false, false, true, false).as("A")
    s2A != 8 shouldBe !Series(false, false, null, true, false).as("A")
    s2ASliced != 8 shouldBe !Series(false, false, null, true, false).apply(maskA).as("A")
  }

  test("agg[R](start: R, f: (R, T) => R): R") {
    s1A.agg(-10, _ + _) shouldBe 13
    s1B.agg(1.0, _ * _) shouldBe 1 * 23.1 * 1.4 * 1.4 * 7.0 * 3.1
    s1C.agg("A", _ + _.toLowerCase) shouldBe "Aghiabcxyzqqquuu"
    s1D.agg(0, _ + _.i) shouldBe 10

    s2A.agg(-10, _ + _) shouldBe 11
    s2B.agg(1.0, _ * _) shouldBe 1 * 1.4 * 1.4 * 7.0 * 3.1
    s2C.agg("A", _ + _.toLowerCase) shouldBe "Aghiabcxyz"
    s2D.agg(0, _ + _.i) shouldBe 9

    s1ASliced.agg(-10, _ + _) shouldBe 11
    s1BSliced.agg(1.0, _ * _) shouldBe 1 * 1.4 * 1.4 * 7.0 * 3.1
    s1CSliced.agg("A", _ + _.toLowerCase) shouldBe "Aghiabcxyz"
    s1DSliced.agg(0, _ + _.i) shouldBe 9

    s2ASliced.agg(-10, _ + _) shouldBe 11
    s2BSliced.agg(1.0, _ * _) shouldBe 1 * 1.4 * 1.4 * 7.0 * 3.1
    s2CSliced.agg("A", _ + _.toLowerCase) shouldBe "Aghiabcxyz"
    s2DSliced.agg(0, _ + _.i) shouldBe 9

    df3("A").asInt.agg(0, _ + _) shouldBe 20
    df3("B").asDouble.agg(0.0, _ + _) shouldBe 23.1 + 1.4 + 3.1
    df3("C").as[String].agg("", _ + _) shouldBe "ghiqqqUuu"
  }

  test("apply(ix: Int): Option[T]") {
    val s = s2A.sorted

    assertThrows[IndexBoundsException](s2A(-1))
    s2A(0) shouldBe Some(6)
    s2A(1) shouldBe Some(3)
    s2A(2) shouldBe None
    s2A(3) shouldBe Some(8)
    s2A(4) shouldBe Some(4)
    assertThrows[IndexBoundsException](s2A(-5))

    assertThrows[IndexBoundsException](s(-1))
    s(0) shouldBe Some(6)
    s(1) shouldBe Some(3)
    s(2) shouldBe None
    s(3) shouldBe Some(8)
    s(4) shouldBe Some(4)
    assertThrows[IndexBoundsException](s(-5))

    assertThrows[IndexBoundsException](s1CSliced(-1))
    s1CSliced(0) shouldBe Some("ghi")
    s1CSliced(1) shouldBe Some("ABC")
    s1CSliced(2) shouldBe Some("XyZ")
    s1CSliced(3) shouldBe None
    s1CSliced(4) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced(5))
  }

  test("apply(ix: Option[Int]): Option[T]") {
    val s = s2A.sorted

    s2A(None) shouldBe None
    assertThrows[IndexBoundsException](s2A(Some(-1)))
    s2A(Some(0)) shouldBe Some(6)
    s2A(Some(1)) shouldBe Some(3)
    s2A(Some(2)) shouldBe None
    s2A(Some(3)) shouldBe Some(8)
    s2A(Some(4)) shouldBe Some(4)
    assertThrows[IndexBoundsException](s2A(Some(-5)))

    s(None) shouldBe None
    assertThrows[IndexBoundsException](s(Some(-1)))
    s(Some(0)) shouldBe Some(6)
    s(Some(1)) shouldBe Some(3)
    s(Some(2)) shouldBe None
    s(Some(3)) shouldBe Some(8)
    s(Some(4)) shouldBe Some(4)
    assertThrows[IndexBoundsException](s(Some(-5)))

    s1CSliced(None) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced(Some(-1)))
    s1CSliced(Some(0)) shouldBe Some("ghi")
    s1CSliced(Some(1)) shouldBe Some("ABC")
    s1CSliced(Some(2)) shouldBe Some("XyZ")
    s1CSliced(Some(3)) shouldBe None
    s1CSliced(Some(4)) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced(Some(5)))
  }

  test("apply(ix: Int, default: => T): T") {
    val s = s2A.sorted

    assertThrows[IndexBoundsException](s2A(-1, 10))
    s2A(0, 10) shouldBe 6
    s2A(1, 10) shouldBe 3
    s2A(2, 10) shouldBe 10
    s2A(3, 10) shouldBe 8
    s2A(4, 10) shouldBe 4
    assertThrows[IndexBoundsException](s2A(-5, 10))

    assertThrows[IndexBoundsException](s(-1, 10))
    s(0, 10) shouldBe 6
    s(1, 10) shouldBe 3
    s(2, 10) shouldBe 10
    s(3, 10) shouldBe 8
    s(4, 10) shouldBe 4
    assertThrows[IndexBoundsException](s(-5, 10))

    assertThrows[IndexBoundsException](s1CSliced(-1, "default"))
    s1CSliced(0, "default") shouldBe "ghi"
    s1CSliced(1, "default") shouldBe "ABC"
    s1CSliced(2, "default") shouldBe "XyZ"
    s1CSliced(3, "default") shouldBe "default"
    s1CSliced(4, "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced(5, "default"))
  }

  test("apply(ix: Option[Int], default: => T): T ") {
    val s = s2A.sorted

    s2A(None, 10) shouldBe 10
    assertThrows[IndexBoundsException](s2A(Some(-1), 10))
    s2A(Some(0), 10) shouldBe 6
    s2A(Some(1), 10) shouldBe 3
    s2A(Some(2), 10) shouldBe 10
    s2A(Some(3), 10) shouldBe 8
    s2A(Some(4), 10) shouldBe 4
    assertThrows[IndexBoundsException](s2A(Some(-5), 10))

    s(None, 10) shouldBe 10
    assertThrows[IndexBoundsException](s(Some(-1), 10))
    s(Some(0), 10) shouldBe 6
    s(Some(1), 10) shouldBe 3
    s(Some(2), 10) shouldBe 10
    s(Some(3), 10) shouldBe 8
    s(Some(4), 10) shouldBe 4
    assertThrows[IndexBoundsException](s(Some(-5), 10))

    s1CSliced(None, "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced(Some(-1), "default"))
    s1CSliced(Some(0), "default") shouldBe "ghi"
    s1CSliced(Some(1), "default") shouldBe "ABC"
    s1CSliced(Some(2), "default") shouldBe "XyZ"
    s1CSliced(Some(3), "default") shouldBe "default"
    s1CSliced(Some(4), "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced(Some(5), "default"))
  }

  test("as[T2: Typeable: ClassTag]: Series[T2]") {
    val s1 = s1A.as[Int]
    s1A.get(0) shouldBe 6
    val s2 = s1A.toAny.as[Int]
    s2.get(0) shouldBe 6
    assertThrows[SeriesCastException](s1A.as[Double])
    assertThrows[SeriesCastException](s1A.as[String])
    assertThrows[SeriesCastException](s1B.as[Int])
    assertThrows[SeriesCastException](s1B.toAny.as[Int])

  }

  test("as(newName: String): Series[T]") {
    s1A.as("new Name").name shouldBe "new Name"
    (s1A as "numbers").name shouldBe "numbers"
  }

  test("as[T2: Typeable: ClassTag](ix: Int): Option[T2]") {
    val s = s2A.sorted

    s2A.as[Int](0).get shouldBe 6
    s2A.toAny.as[Int](0).get shouldBe 6
    assertThrows[ClassCastException](s2A.as[Double](0).get)
    assertThrows[ClassCastException](s2A.as[Boolean](0).get)
    s1CSliced.as[String](1).get shouldBe "ABC"
    s1CSliced.toAny.as[String](1).get shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as[Int](1).get)

    assertThrows[IndexBoundsException](s2A.as[Int](-1))
    s2A.as[Int](0) shouldBe Some(6)
    s2A.as[Int](1) shouldBe Some(3)
    s2A.as[Int](2) shouldBe None
    s2A.as[Int](3) shouldBe Some(8)
    s2A.as[Int](4) shouldBe Some(4)
    assertThrows[IndexBoundsException](s2A.as[Int](-5))

    assertThrows[IndexBoundsException](s.as[Int](-1))
    s.as[Int](0) shouldBe Some(6)
    s.as[Int](1) shouldBe Some(3)
    s.as[Int](2) shouldBe None
    s.as[Int](3) shouldBe Some(8)
    s.as[Int](4) shouldBe Some(4)
    assertThrows[IndexBoundsException](s.as[Int](-5))

    assertThrows[IndexBoundsException](s1CSliced.as[String](-1))
    s1CSliced.as[String](0) shouldBe Some("ghi")
    s1CSliced.as[String](1) shouldBe Some("ABC")
    s1CSliced.as[String](2) shouldBe Some("XyZ")
    s1CSliced.as[String](3) shouldBe None
    s1CSliced.as[String](4) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced.as[String](5))
  }

  test("as[T2: Typeable: ClassTag](ix: Option[Int]): Option[T2]") {
    val s = s2A.sorted

    s2A.as[Int](Some(0)).get shouldBe 6
    s2A.toAny.as[Int](Some(0)).get shouldBe 6
    assertThrows[ClassCastException](s2A.as[Double](Some(0)).get)
    assertThrows[ClassCastException](s2A.as[Boolean](Some(0)).get)
    s1CSliced.as[String](Some(1)).get shouldBe "ABC"
    s1CSliced.toAny.as[String](Some(1)).get shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as[Int](Some(1)).get)

    s2A.as[Int](None) shouldBe None
    assertThrows[IndexBoundsException](s2A.as[Int](Some(-1)))
    s2A.as[Int](Some(0)) shouldBe Some(6)
    s2A.as[Int](Some(1)) shouldBe Some(3)
    s2A.as[Int](Some(2)) shouldBe None
    s2A.as[Int](Some(3)) shouldBe Some(8)
    s2A.as[Int](Some(4)) shouldBe Some(4)
    assertThrows[IndexBoundsException](s2A.as[Int](Some(-5)))

    s.as[Int](None) shouldBe None
    assertThrows[IndexBoundsException](s.as[Int](Some(-1)))
    s.as[Int](Some(0)) shouldBe Some(6)
    s.as[Int](Some(1)) shouldBe Some(3)
    s.as[Int](Some(2)) shouldBe None
    s.as[Int](Some(3)) shouldBe Some(8)
    s.as[Int](Some(4)) shouldBe Some(4)
    assertThrows[IndexBoundsException](s.as[Int](Some(-5)))

    s1CSliced.as[String](None) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced.as[String](Some(-1)))
    s1CSliced.as[String](Some(0)) shouldBe Some("ghi")
    s1CSliced.as[String](Some(1)) shouldBe Some("ABC")
    s1CSliced.as[String](Some(2)) shouldBe Some("XyZ")
    s1CSliced.as[String](Some(3)) shouldBe None
    s1CSliced.as[String](Some(4)) shouldBe None
    assertThrows[IndexBoundsException](s1CSliced.as[String](Some(5)))
  }

  test("as[T2: Typeable: ClassTag](ix: Int, default: => T2): T2") {
    val s = s2A.sorted

    s2A.as[Int](0, -1) shouldBe 6
    s2A.toAny.as[Int](0, -1) shouldBe 6
    assertThrows[ClassCastException](s2A.as[Double](0, -1))
    assertThrows[ClassCastException](s2A.as[Boolean](0, true))
    s1CSliced.as[String](1, "default") shouldBe "ABC"
    s1CSliced.toAny.as[String](1, "default") shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as[Int](1, -1))

    s2A.as(0, -1) shouldBe 6
    s2A.toAny.as(0, -1) shouldBe 6
    assertThrows[ClassCastException](s2A.as(0, -1.0))
    assertThrows[ClassCastException](s2A.as(0, true))
    s1CSliced.as(1, "default") shouldBe "ABC"
    s1CSliced.toAny.as(1, "default") shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as(1, -1))

    assertThrows[IndexBoundsException](s2A.as[Int](-1, 10))
    s2A.as[Int](0, 10) shouldBe 6
    s2A.as[Int](1, 10) shouldBe 3
    s2A.as[Int](2, 10) shouldBe 10
    s2A.as[Int](3, 10) shouldBe 8
    s2A.as[Int](4, 10) shouldBe 4
    assertThrows[IndexBoundsException](s2A.as[Int](-5, 10))

    assertThrows[IndexBoundsException](s.as[Int](-1, 10))
    s.as[Int](0, 10) shouldBe 6
    s.as[Int](1, 10) shouldBe 3
    s.as[Int](2, 10) shouldBe 10
    s.as[Int](3, 10) shouldBe 8
    s.as[Int](4, 10) shouldBe 4
    assertThrows[IndexBoundsException](s.as[Int](-5, 10))

    assertThrows[IndexBoundsException](s1CSliced.as[String](-1, "default"))
    s1CSliced.as[String](0, "default") shouldBe "ghi"
    s1CSliced.as[String](1, "default") shouldBe "ABC"
    s1CSliced.as[String](2, "default") shouldBe "XyZ"
    s1CSliced.as[String](3, "default") shouldBe "default"
    s1CSliced.as[String](4, "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced.as[String](5, "default"))
  }

  test("as[T2: Typeable: ClassTag](ix: Option[Int], default: => T2): T2") {
    val s = s2A.sorted

    s2A.as[Int](Some(0), -1) shouldBe 6
    s2A.toAny.as[Int](Some(0), -1) shouldBe 6
    assertThrows[ClassCastException](s2A.as[Double](Some(0), -1.0))
    assertThrows[ClassCastException](s2A.as[Boolean](Some(0), false))
    s1CSliced.as[String](Some(1), "default") shouldBe "ABC"
    s1CSliced.toAny.as[String](Some(1), "default") shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as[Int](Some(1), -1))

    s2A.as(Some(0), -1) shouldBe 6
    s2A.toAny.as(Some(0), -1) shouldBe 6
    assertThrows[ClassCastException](s2A.as(Some(0), -1.0))
    assertThrows[ClassCastException](s2A.as(Some(0), false))
    s1CSliced.as(Some(1), "default") shouldBe "ABC"
    s1CSliced.toAny.as(Some(1), "default") shouldBe "ABC"
    assertThrows[ClassCastException](s1CSliced.as(Some(1), -1))

    s2A.as[Int](None, 10) shouldBe 10
    assertThrows[IndexBoundsException](s2A.as[Int](Some(-1), 10))
    s2A.as[Int](Some(0), 10) shouldBe 6
    s2A.as[Int](Some(1), 10) shouldBe 3
    s2A.as[Int](Some(2), 10) shouldBe 10
    s2A.as[Int](Some(3), 10) shouldBe 8
    s2A.as[Int](Some(4), 10) shouldBe 4
    assertThrows[IndexBoundsException](s2A.as[Int](Some(-5), 10))

    s.as[Int](None, 10) shouldBe 10
    assertThrows[IndexBoundsException](s.as[Int](Some(-1), 10))
    s.as[Int](Some(0), 10) shouldBe 6
    s.as[Int](Some(1), 10) shouldBe 3
    s.as[Int](Some(2), 10) shouldBe 10
    s.as[Int](Some(3), 10) shouldBe 8
    s.as[Int](Some(4), 10) shouldBe 4
    assertThrows[IndexBoundsException](s.as[Int](Some(-5), 10))

    s1CSliced.as[String](None, "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced.as[String](Some(-1), "default"))
    s1CSliced.as[String](Some(0), "default") shouldBe "ghi"
    s1CSliced.as[String](Some(1), "default") shouldBe "ABC"
    s1CSliced.as[String](Some(2), "default") shouldBe "XyZ"
    s1CSliced.as[String](Some(3), "default") shouldBe "default"
    s1CSliced.as[String](Some(4), "default") shouldBe "default"
    assertThrows[IndexBoundsException](s1CSliced.as[String](Some(5), "default"))
  }

  test("canEqual(a: Any): Boolean") {
    series1.foreach(s => s canEqual s shouldBe true)
    series2.foreach(s => s canEqual s shouldBe true)
    series1.zip(series2).map(_ canEqual _ shouldBe true)
    s1A canEqual 1 shouldBe false
    s1B canEqual 1.1 shouldBe false
    s1C canEqual "1.1" shouldBe false
    s1D canEqual C1("1.1", 32) shouldBe false
  }

  test("count: Int") {
    series1.foreach(_.count shouldBe seriesRowLength)
    s2A.count shouldBe 4
    s2B.count shouldBe 4
    s2C.count shouldBe 3
    s2D.count shouldBe 4
    s1ASliced.count shouldBe 4
    s1BSliced.count shouldBe 4
    s1CSliced.count shouldBe 3
    s1DSliced.count shouldBe 4
    Series(1.0, 2.0, Double.NaN, 4.0).count shouldBe 3
    Series(null, 2.0, Double.NaN, Double.NaN).count shouldBe 1
  }

  test("defined: Series[T]") {
    s1A.defined shouldBe s1A
    s1B.defined shouldBe s1B
    s1C.defined shouldBe s1C
    s1D.defined shouldBe s1D
    s1ASliced.defined shouldBe s1ASliced
    s1BSliced.defined shouldBe s1BSliced
    s1CSliced.defined shouldBe s1CSliced
    s1DSliced.defined shouldBe s1DSliced
    s2A.defined shouldBe s2ASliced
    s2B.defined shouldBe s2BSliced
    s2C.defined shouldBe s2CSliced
    s2D.defined shouldBe s2DSliced
    s2ASliced.defined shouldBe s2ASliced
    s2BSliced.defined shouldBe s2BSliced
    s2CSliced.defined shouldBe s2CSliced
    s2DSliced.defined shouldBe s2DSliced
  }

  test("dense: Series[T]") {
    s1A.dense shouldBe s1A
    s1B.dense shouldBe s1B
    s1C.dense shouldBe s1C
    s1D.dense shouldBe s1D
    s1ASliced.dense shouldBe s2A
    s1BSliced.dense shouldBe s2B
    s1CSliced.dense shouldBe s2C
    s1DSliced.dense shouldBe s2D
    s2A.dense shouldBe s2A
    s2B.dense shouldBe s2B
    s2C.dense shouldBe s2C
    s2D.dense shouldBe s2D
    s2ASliced.dense shouldBe s2A
    s2BSliced.dense shouldBe s2B
    s2CSliced.dense shouldBe s2C
    s2DSliced.dense shouldBe s2D

    val s3A: Series[Int] = Series("A")(6, null, 2, 8, 4).apply(Seq(0, 2, 3, 4))
    val s3B: Series[Double] = Series("B")(23.1, null, 1.4, null, 3.1).apply(Seq(0, 2, 3, 4))
    val s3C: Series[String] = Series("C")("ghi", null, null, "qqq", "Uuu").apply(Seq(0, 2, 3, 4))
    val s3D: Series[C1] = Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).apply(Seq(0, 2, 3, 4))

    df3("A").dense shouldBe Series("A")(6, null, 2, 8, 4)
    df3("B").dense shouldBe Series("B")(23.1, null, 1.4, null, 3.1)
    df3("C").dense shouldBe Series("C")("ghi", null, null, "qqq", "Uuu")
    df3("D").dense shouldBe Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("display(...): Unit") {
    s1A.display(0)
    s1B.display(5)
    s2C.display(2, 100)
    s2D.display()
    // see toString(...) for asserts
  }

  test("distinct: Series[T]") {
    s1A.distinct shouldBe s1A
    s1B.distinct shouldBe Series("B")(23.1, 1.4, 7.0, 3.1)
    s1C.distinct shouldBe s1C
    s1D.distinct shouldBe s1D
    s2A.distinct shouldBe s1ASliced.resetIndex
    s2B.distinct shouldBe Series("B")(1.4, 7.0, 3.1)
    s2C.distinct shouldBe s1CSliced.resetIndex
    s2D.distinct shouldBe s1DSliced.resetIndex
    s2ASliced.distinct shouldBe s1ASliced.resetIndex
    s2BSliced.distinct shouldBe Series("B")(1.4, 7.0, 3.1)
    s2CSliced.distinct shouldBe s1CSliced.resetIndex
    s2DSliced.distinct shouldBe s1DSliced.resetIndex
  }

  test("equals(that: Any): Boolean") {
    s1A equals Series(0, 1, 2) shouldBe false

    series1.foreach(s => s equals s shouldBe true)
    series1.zip(series1Sliced).map(_ equals _ shouldBe false)
    series1Sliced.zip(series1).map(_ equals _ shouldBe false)
    series1.map(s => series1.filter(identityHashCode(_) != identityHashCode(s)).map(_ equals s shouldBe false))

    series2.foreach(s => s equals s shouldBe true)
    series2.zip(series2Sliced).map(_ equals _ shouldBe false)
    series2Sliced.zip(series2).map(_ equals _ shouldBe false)

    series1.zip(series2).map(_ equals _ shouldBe false)
    series2.zip(series1).map(_ equals _ shouldBe false)
    series1Sliced.zip(series2Sliced).map(_ equals _ shouldBe true)
    series2Sliced.zip(series1Sliced).map(_ equals _ shouldBe true)
    series1.zip(series2Sliced).map(_ equals _ shouldBe false)
    series2Sliced.zip(series1).map(_ equals _ shouldBe false)

    // Variation in index
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) equals s shouldBe true)
    series1.foreach(s => s(Seq(0, 1, 2, 3, 4)) equals s(0 to 4) shouldBe true)
    series1.foreach(s => s(0 to 4) equals s shouldBe true)
    series1.foreach(s => s(Seq(0, 1, 3, 2, 4)) equals s(Seq(0, 1, 3, 2, 4)) shouldBe true)
    series1.foreach(s => s(Seq(0, 1, 3, 2, 4)) equals s shouldBe false)

    // Variation in value (and index)
    series1.zip(series1Var).map(_ equals _ shouldBe false)
    series1.zip(series1Var).map(_(0 to 3) equals _ shouldBe false)
    series1.zip(series1Var).map(_(0 to 3) equals _(1 to 4) shouldBe false)
    series1.zip(series1Var).map(_(0 to 3) equals _(0 to 3) shouldBe true)
    series1.zip(series1Var).map(_(1 to 4) equals _(1 to 4) shouldBe false)
    series1.zip(series1Sorted).map(_ equals _ shouldBe false)

    // Double.NaN
    Series(1.0, Double.NaN) equals Series(1.0, Double.NaN) shouldBe true
    Series(1.0, Double.NaN) equals Series(1.0, 1.0) shouldBe false
    Series(1.0, Double.NaN) equals Series(1.0, null) shouldBe false
    Series(1.0, Double.NaN) equals Series(1.0, Double.PositiveInfinity) shouldBe false
    val sDouble = Series(1.0, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)
    sDouble equals sDouble shouldBe true
  }

  test("equalsByValue[T2: ClassTag](series: Series[T2]): Boolean") {
    s1A equalsByValue Series(0, 1, 2) shouldBe false
    s1A equalsByValue Series(0, 1, 2, 3, 4) shouldBe false
    s1ASliced equalsByValue Series(0, 1, null, 3, 4) shouldBe false
    s1ASorted equalsByValue s1A shouldBe true
    s1A.as("B") equalsByValue s1A shouldBe true

    series1.foreach(s => s equalsByValue s shouldBe true)
    series2.foreach(s => s equalsByValue s shouldBe true)
    series1.filter(_.name != "D").foreach(s => s.sortValues equalsByValue s shouldBe true)
    series1.foreach(s => s.as("Series") equalsByValue s shouldBe true)
    series2.zip(series2Sliced).map(_ equalsByValue _ shouldBe true)
    series2Sliced.zip(series2).map(_ equalsByValue _ shouldBe true)
    series1.zip(series2).map(_ equalsByValue _ shouldBe false)
    series2.zip(series1).map(_ equalsByValue _ shouldBe false)
    series1.zip(series2Sliced).map(_ equalsByValue _ shouldBe false)
    series2Sliced.zip(series1).map(_ equalsByValue _ shouldBe false)
    series1.map(s => series1.filter(identityHashCode(_) != identityHashCode(s)).map(_ equalsByValue s shouldBe false))

    Series(1.0, Double.NaN) equalsByValue Series(1.0, Double.NaN) shouldBe true
    Series(1.0, Double.NaN) equalsByValue Series(1.0, 1.0) shouldBe false
    Series(1.0, Double.NaN) equalsByValue Series(1.0, null) shouldBe false
    Series(1.0, Double.NaN) equalsByValue Series(1.0, Double.PositiveInfinity) shouldBe false
    val sDouble = Series(1.0, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)
    sDouble equalsByValue sDouble.as("X") shouldBe true
  }

  test("exists(value: T): Boolean ") {
    s1A.exists(1) shouldBe false
    s1A.exists(2) shouldBe true
    s1A.exists(6) shouldBe true
    s1B.exists(23.0) shouldBe false
    s1B.exists(23.1) shouldBe true
    s1B.exists(1.4) shouldBe true
    s1C.exists("QQQ") shouldBe false
    s1C.exists("qqq") shouldBe true
    s1C.exists("XyZ") shouldBe true
    s1D.exists(C1("A", 4)) shouldBe false
    s1D.exists(C1("A", 5)) shouldBe true
    s1D.exists(C1("C", 2)) shouldBe true
  }

  test("fill[T2 <: T](series: Series[T2]): Series[T]") {
    s2A.fill(s1A) shouldBe s1A
    s2B.fill(s1B) shouldBe s1B
    s2C.fill(s1C) shouldBe s1C
    s2D.fill(s1D) shouldBe s1D
    s2ASliced.fill(s1A) shouldBe s2ASliced
    s2BSliced.fill(s1B) shouldBe s2BSliced
    s2CSliced.fill(s1C) shouldBe s2CSliced
    s2DSliced.fill(s1D) shouldBe s2DSliced
    s2ASliced.fill(s2ASliced) shouldBe s2ASliced
    s2BSliced.fill(s2BSliced) shouldBe s2BSliced
    s2CSliced.fill(s2CSliced) shouldBe s2CSliced
    s2DSliced.fill(s2DSliced) shouldBe s2DSliced

    s2A.toAny.fill(s1A) shouldBe s1A
    s2B.toAny.fill(s1B) shouldBe s1B
    s2A.toAny.fill(s1A.toAny) shouldBe s1A
    s2B.toAny.fill(s1B.toAny) shouldBe s1B

    assertThrows[SeriesCastException](s2A.toAny.fill(s1B))
    assertThrows[SeriesCastException](s2B.toAny.fill(s1C))
    s2D.toAny.fill(s1DOfC2) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("fill(value: T): Series[T]") {
    s2A.fill(2) shouldBe s1A
    s2B.fill(23.1) shouldBe s1B
    s2C.fill("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2D.fill(C1("B", 1)) shouldBe s1D
    s2ASliced.fill(2) shouldBe s2ASliced
    s2BSliced.fill(23.1) shouldBe s2BSliced
    s2CSliced.fill("value") shouldBe s2CSliced
    s2DSliced.fill(C1("B", 1)) shouldBe s2DSliced

    s2A.toAny.fill(2) shouldBe s1A
    s2B.toAny.fill(23.1) shouldBe s1B
    s2C.toAny.fill("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2D.toAny.fill(C1("B", 1)) shouldBe s1D

    s2B.fill(23) shouldBe Series("B")(23.0, 1.4, 1.4, 7.0, 3.1)
    assertThrows[ValueCastException](s2A.toAny.fill(2.0))
    assertThrows[ValueCastException](s2B.toAny.fill("23"))
    s2D.toAny.fill(C2()) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("fillAll[T2 <: T](series: Series[T2]): Series[T]") {
    s2A.fillAll(s1A) shouldBe s1A
    s2B.fillAll(s1B) shouldBe s1B
    s2C.fillAll(s1C) shouldBe s1C
    s2D.fillAll(s1D) shouldBe s1D
    s2ASliced.fillAll(s1A) shouldBe s1A
    s2BSliced.fillAll(s1B) shouldBe s1B
    s2CSliced.fillAll(s1C) shouldBe s1C
    s2DSliced.fillAll(s1D) shouldBe s1D
    s2ASliced.fillAll(s2ASliced) shouldBe s2A
    s2BSliced.fillAll(s2BSliced) shouldBe s2B
    s2CSliced.fillAll(s2CSliced) shouldBe s2C
    s2DSliced.fillAll(s2DSliced) shouldBe s2D

    s2A.toAny.fillAll(s1A) shouldBe s1A
    s2B.toAny.fillAll(s1B) shouldBe s1B
    s2A.toAny.fillAll(s1A.toAny) shouldBe s1A
    s2B.toAny.fillAll(s1B.toAny) shouldBe s1B

    assertThrows[SeriesCastException](s2A.toAny.fillAll(s1B))
    assertThrows[SeriesCastException](s2B.toAny.fillAll(s1C))
    s2D.toAny.fillAll(s1DOfC2) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("fillAll(value: T): Series[T]") {
    s2A.fillAll(2) shouldBe s1A
    s2B.fillAll(23.1) shouldBe s1B
    s2C.fillAll("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2D.fillAll(C1("B", 1)) shouldBe s1D
    s2ASliced.fillAll(2) shouldBe s1A
    s2BSliced.fillAll(23.1) shouldBe s1B
    s2CSliced.fillAll("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2DSliced.fillAll(C1("B", 1)) shouldBe s1D

    s2A.toAny.fillAll(2) shouldBe s1A
    s2B.toAny.fillAll(23.1) shouldBe s1B
    s2CSliced.toAny.fillAll("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2DSliced.toAny.fillAll(C1("B", 1)) shouldBe s1D

    s2B.fillAll(23) shouldBe Series("B")(23.0, 1.4, 1.4, 7.0, 3.1)
    assertThrows[ValueCastException](s2A.toAny.fillAll(2.0))
    assertThrows[ValueCastException](s2B.toAny.fillAll("23"))
    s2DSliced.toAny.fillAll(C2()) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("first: Option[Int]") {
    Series.empty[Int].first shouldBe None
    Series(1, 2, 3).first shouldBe Some(0)
    Series(null, 2, 3).first shouldBe Some(1)
    Series(1, null, 3).first shouldBe Some(0)
    Series(null, null, 3).first shouldBe Some(2)
    s1A(Seq[Int]()).first shouldBe None

    Series.empty[Boolean].first shouldBe None
    Series(true, true, true).first shouldBe Some(0)
    Series(false, true, false).first shouldBe Some(1)
    Series(false, true, true).first shouldBe Some(1)
    Series(false, false, true).first shouldBe Some(2)
    Series(false, null, true).first shouldBe Some(2)
    Series(false, null, true).first shouldBe Some(2)
    Series(false, null, null).first shouldBe None
    Series(null, true, null).first shouldBe Some(1)
    Series(true, true, true).apply(Seq(1, 2)).first shouldBe Some(1)
    Series(true, true, true).apply(Seq()).first shouldBe None

    s1A.first shouldBe Some(0)
    s1B.first shouldBe Some(0)
    s1C.first shouldBe Some(0)
    s1D.first shouldBe Some(0)
    s2A.first shouldBe Some(0)
    s2B.first shouldBe Some(1)
    s2C.first shouldBe Some(0)
    s2D.first shouldBe Some(0)
    s1ASliced.first shouldBe Some(0)
    s1BSliced.first shouldBe Some(1)
    s1CSliced.first shouldBe Some(0)
    s1DSliced.first shouldBe Some(0)

    s1A.sorted.first shouldBe Some(2)
    s1B.sorted.first shouldBe Some(1)
    s1C.sorted.first shouldBe Some(1)
    s1D.sorted.first shouldBe Some(4)

  }

  test("first(f: T => Boolean): Option[Int]") {
    s1A.first(_ == 6) shouldBe Some(0)
    s1A.first(_ < 4) shouldBe Some(1)
    s1A.first(_ < 3) shouldBe Some(2)
    s1A.first(_ >= 8) shouldBe Some(3)
    s1A.first(_ == 4) shouldBe Some(4)
    s1A.first(_ == 1) shouldBe None
    s1B.first(_ == 1.4) shouldBe Some(1)
    s1B.first(_ < 2.0) shouldBe Some(1)
    s1B.first(_ < 1.0) shouldBe None
    s1C.first(_ == "ABC") shouldBe Some(1)
    s1C.first(_ == "ABc") shouldBe None
    s1D.first(_ == C1("D", 2)) shouldBe Some(3)
    s1D.first(_ == C1("D", 7)) shouldBe None

    s1A.sorted.first(_ == 4) shouldBe Some(4)
    s1A.sorted.first(_ == 1) shouldBe None
    s1B.sorted.first(_ == 1.4) shouldBe Some(1)
    s1B((0 to 4).reverse.toArray).first(_ == 1.4) shouldBe Some(2)
    s1B.sorted.first(_ == 7.0) shouldBe Some(3)
    s1B.sorted.first(_ == 1.0) shouldBe None
    s1C.sorted.first(_ == "ABC") shouldBe Some(1)
    s1C.sorted.first(_ == "ABc") shouldBe None
    s1D.sorted.first(_ == C1("D", 2)) shouldBe Some(3)
    s1D.sorted.first(_ == C1("D", 7)) shouldBe None

    s1ASliced.first(_ == 4) shouldBe Some(4)
    s1ASliced.first(_ == 2) shouldBe None
    s1BSliced.first(_ == 1.4) shouldBe Some(1)
    s1BSliced.first(_ == 23.1) shouldBe None
    s1CSliced.first(_ == "ABC") shouldBe Some(1)
    s1CSliced.first(_ == "qqq") shouldBe None
    s1DSliced.first(_ == C1("D", 2)) shouldBe Some(3)
    s1DSliced.first(_ == C1("B", 1)) shouldBe None

    Series.empty[Int].first(_ == 1) shouldBe None
  }

  test("first(value: T): Option[Int]") {
    s1A.first(6) shouldBe Some(0)
    s1A.first(3) shouldBe Some(1)
    s1A.first(2) shouldBe Some(2)
    s1A.first(8) shouldBe Some(3)
    s1A.first(4) shouldBe Some(4)
    s1A.first(1) shouldBe None
    s1B.first(1.4) shouldBe Some(1)
    s1B.first(7.0) shouldBe Some(3)
    s1B.first(1.0) shouldBe None
    s1C.first("ABC") shouldBe Some(1)
    s1C.first("ABc") shouldBe None
    s1D.first(C1("D", 2)) shouldBe Some(3)
    s1D.first(C1("D", 7)) shouldBe None

    s1A.sorted.first(4) shouldBe Some(4)
    s1A.sorted.first(1) shouldBe None
    s1B.sorted.first(1.4) shouldBe Some(1)
    s1B((0 to 4).reverse.toArray).first(1.4) shouldBe Some(2)
    s1B.sorted.first(7.0) shouldBe Some(3)
    s1B.sorted.first(1.0) shouldBe None
    s1C.sorted.first("ABC") shouldBe Some(1)
    s1C.sorted.first("ABc") shouldBe None
    s1D.sorted.first(C1("D", 2)) shouldBe Some(3)
    s1D.sorted.first(C1("D", 7)) shouldBe None

    s1ASliced.first(4) shouldBe Some(4)
    s1ASliced.first(2) shouldBe None
    s1BSliced.first(1.4) shouldBe Some(1)
    s1BSliced.first(23.1) shouldBe None
    s1CSliced.first("ABC") shouldBe Some(1)
    s1CSliced.first("qqq") shouldBe None
    s1DSliced.first(C1("D", 2)) shouldBe Some(3)
    s1DSliced.first(C1("B", 1)) shouldBe None

    Series.empty[Int].first(1) shouldBe None
  }

  test("first[T2](series: Series[T2], value: T2): Option[T]") {
    val s = Series(0.0, 1.0, 2.0, 3.0, 4.0)

    s.first(s1A, 6) shouldBe Some(0.0)
    s.first(s1A, 3) shouldBe Some(1.0)
    s.first(s1A, 2) shouldBe Some(2.0)
    s.first(s1A, 8) shouldBe Some(3.0)
    s.first(s1A, 4) shouldBe Some(4.0)
    s.first(s1A, 1) shouldBe None
    s.first(s1B, 1.4) shouldBe Some(1.0)
    s.first(s1B, 7.0) shouldBe Some(3.0)
    s.first(s1B, 1.0) shouldBe None
    s.first(s1C, "ABC") shouldBe Some(1.0)
    s.first(s1C, "ABc") shouldBe None
    s.first(s1D, C1("D", 2)) shouldBe Some(3.0)
    s.first(s1D, C1("D", 7)) shouldBe None

    s.first(s1A.sorted, 4) shouldBe Some(4.0)
    s.first(s1A.sorted, 1) shouldBe None
    s.first(s1B.sorted, 1.4) shouldBe Some(1.0)
    s.first(s1B((0 to 4).reverse.toArray), 1.4) shouldBe Some(2.0)
    s.first(s1B.sorted, 7.0) shouldBe Some(3.0)
    s.first(s1B.sorted, 1.0) shouldBe None
    s.first(s1C.sorted, "ABC") shouldBe Some(1.0)
    s.first(s1C.sorted, "ABc") shouldBe None
    s.first(s1D.sorted, C1("D", 2)) shouldBe Some(3.0)
    s.first(s1D.sorted, C1("D", 7)) shouldBe None

    s.first(s1ASliced, 4) shouldBe Some(4.0)
    s.first(s1ASliced, 2) shouldBe None
    s.first(s1BSliced, 1.4) shouldBe Some(1.0)
    s.first(s1BSliced, 23.1) shouldBe None
    s.first(s1CSliced, "ABC") shouldBe Some(1.0)
    s.first(s1CSliced, "qqq") shouldBe None
    s.first(s1DSliced, C1("D", 2)) shouldBe Some(3.0)
    s.first(s1DSliced, C1("B", 1)) shouldBe None

    Series.empty[Double].first(Series.empty[Int], 1) shouldBe None
    assertThrows[BaseIndexException](s.first(Series(1, 2), 2))

    val s2 = Series(0.0, null, 2.0, 3.0, 4.0).apply(Seq(0, 1, 2, 4))
    s2.first(s1A, 6) shouldBe Some(0.0)
    s2.first(s1A, 3) shouldBe None
    s2.first(s1A, 2) shouldBe Some(2.0)
    s2.first(s1A, 8) shouldBe None
    s2.first(s1A, 4) shouldBe Some(4.0)
    s2.first(s1A, 1) shouldBe None
  }

  test("first[T2](series: Series[T2], value: T2, default: => T): T") {
    val s = Series(0.0, 1.0, 2.0, 3.0, 4.0)

    s.first(s1A, 6, -1) shouldBe 0.0
    s.first(s1A, 3, -1) shouldBe 1.0
    s.first(s1A, 2, -1) shouldBe 2.0
    s.first(s1A, 8, -1) shouldBe 3.0
    s.first(s1A, 4, -1) shouldBe 4.0
    s.first(s1A, 1, -1) shouldBe -1.0
    s.first(s1B, 1.4, 2.0) shouldBe 1.0
    s.first(s1B, 7.0, 2.0) shouldBe 3.0
    s.first(s1B, 1.0, 2.0) shouldBe 2.0
    s.first(s1C, "ABC", -1.0) shouldBe 1.0
    s.first(s1C, "ABc", -1.0) shouldBe -1.0
    s.first(s1D, C1("D", 2), -1.0) shouldBe 3.0
    s.first(s1D, C1("D", 7), -1.0) shouldBe -1.0

    s.first(s1A.sorted, 4, -1) shouldBe 4.0
    s.first(s1A.sorted, 1, -1) shouldBe -1.0
    s.first(s1B.sorted, 1.4, 2.0) shouldBe 1.0
    s.first(s1B((0 to 4).reverse.toArray), 1.4, 2.0) shouldBe 2.0
    s.first(s1B.sorted, 7.0, 2.0) shouldBe 3.0
    s.first(s1B.sorted, 1.0, 2.0) shouldBe 2.0
    s.first(s1C.sorted, "ABC", -1.0) shouldBe 1.0
    s.first(s1C.sorted, "ABc", -1.0) shouldBe -1.0
    s.first(s1D.sorted, C1("D", 2), -1.0) shouldBe 3.0
    s.first(s1D.sorted, C1("D", 7), -1.0) shouldBe -1.0

    s.first(s1ASliced, 4, -1) shouldBe 4.0
    s.first(s1ASliced, 2, -1) shouldBe -1.0
    s.first(s1BSliced, 1.4, -2.0) shouldBe 1.0
    s.first(s1BSliced, 23.1, -2.0) shouldBe -2.0
    s.first(s1CSliced, "ABC", -1.0) shouldBe 1.0
    s.first(s1CSliced, "qqq", -1.0) shouldBe -1.0
    s.first(s1DSliced, C1("D", 2), -1.0) shouldBe 3.0
    s.first(s1DSliced, C1("B", 1), -1.0) shouldBe -1.0

    Series.empty[Double].first(Series.empty[Int], 1, -1.0) shouldBe -1.0
    assertThrows[BaseIndexException](s.first(Series(1, 2), 2, -1.0))

    val s2 = Series(0.0, null, 2.0, 3.0, 4.0).apply(Seq(0, 1, 2, 4))
    s2.first(s1A, 6, -1) shouldBe 0.0
    s2.first(s1A, 3, -1) shouldBe -1.0
    s2.first(s1A, 2, -1) shouldBe 2.0
    s2.first(s1A, 8, -1) shouldBe -1.0
    s2.first(s1A, 4, -1) shouldBe 4.0
    s2.first(s1A, 1, -1) shouldBe -1.0
  }

  test("forall(f: T => Boolean): Boolean") {
    s1A.forall(_ > 1) shouldBe true
    s1A.forall(_ > 2) shouldBe false
    s1B.forall(_ >= 1.4) shouldBe true
    s1B.forall(_ < 23.1) shouldBe false
    s1C.forall(_.length == 3) shouldBe true
    s1C.forall(_.contains("A")) shouldBe false
    s1D.forall(_.i >= 0) shouldBe true
    s1D.forall(_.i > 0) shouldBe false

    s2A.forall(_ > 1) shouldBe true
    s2A.forall(_ > 2) shouldBe true
    s2B.forall(_ >= 1.4) shouldBe true
    s2B.forall(_ < 23.1) shouldBe true
    s2C.forall(_.length == 3) shouldBe true
    s2C.forall(_.contains("A")) shouldBe false
    s2D.forall(_.i >= 0) shouldBe true
    s2D.forall(_.i > 0) shouldBe false

    s1ASliced.forall(_ > 1) shouldBe true
    s1ASliced.forall(_ > 2) shouldBe true
    s1BSliced.forall(_ >= 1.4) shouldBe true
    s1BSliced.forall(_ < 23.1) shouldBe true
    s1CSliced.forall(_.length == 3) shouldBe true
    s1CSliced.forall(_.contains("A")) shouldBe false
    s1DSliced.forall(_.i >= 0) shouldBe true
    s1DSliced.forall(_.i > 0) shouldBe false
  }

  test("forallStrict(f: T => Boolean): Boolean") {
    s1A.forallStrictly(_ > 1) shouldBe true
    s1A.forallStrictly(_ > 2) shouldBe false
    s1B.forallStrictly(_ >= 1.4) shouldBe true
    s1B.forallStrictly(_ < 23.1) shouldBe false
    s1C.forallStrictly(_.length == 3) shouldBe true
    s1C.forallStrictly(_.contains("A")) shouldBe false
    s1D.forallStrictly(_.i >= 0) shouldBe true
    s1D.forallStrictly(_.i > 0) shouldBe false

    s2A.forallStrictly(_ > 1) shouldBe false
    s2A.forallStrictly(_ > 2) shouldBe false
    s2B.forallStrictly(_ >= 1.4) shouldBe false
    s2B.forallStrictly(_ < 23.1) shouldBe false
    s2C.forallStrictly(_.length == 3) shouldBe false
    s2C.forallStrictly(_.contains("A")) shouldBe false
    s2D.forallStrictly(_.i >= 0) shouldBe false
    s2D.forallStrictly(_.i > 0) shouldBe false

    s1ASliced.forallStrictly(_ > 1) shouldBe false
    s1ASliced.forallStrictly(_ > 2) shouldBe false
    s1BSliced.forallStrictly(_ >= 1.4) shouldBe false
    s1BSliced.forallStrictly(_ < 23.1) shouldBe false
    s1CSliced.forallStrictly(_.length == 3) shouldBe false
    s1CSliced.forallStrictly(_.contains("A")) shouldBe false
    s1DSliced.forallStrictly(_.i >= 0) shouldBe false
    s1DSliced.forallStrictly(_.i > 0) shouldBe false
  }

  test("get(ix: Int): T") {
    val s = s2A.sorted

    assertThrows[NoSuchElementException](s2A.get(-1))
    s2A.get(0) shouldBe 6
    s2A.get(1) shouldBe 3
    assertThrows[NoSuchElementException](s2A.get(2))
    s2A.get(3) shouldBe 8
    s2A.get(4) shouldBe 4
    assertThrows[NoSuchElementException](s2A.get(-5))

    assertThrows[NoSuchElementException](s.get(-1))
    s.get(0) shouldBe 6
    s.get(1) shouldBe 3
    assertThrows[NoSuchElementException](s.get(2))
    s.get(3) shouldBe 8
    s.get(4) shouldBe 4
    assertThrows[NoSuchElementException](s.get(-5))

    assertThrows[NoSuchElementException](s1CSliced.get(-1))
    s1CSliced.get(0) shouldBe "ghi"
    s1CSliced.get(1) shouldBe "ABC"
    s1CSliced.get(2) shouldBe "XyZ"
    assertThrows[NoSuchElementException](s1CSliced.get(3))
    assertThrows[NoSuchElementException](s1CSliced.get(4))
    assertThrows[NoSuchElementException](s1CSliced.get(5))
  }

  test("get(ix: Option[Int]): T") {
    val s = s2A.sorted

    assertThrows[NoSuchElementException](s2A.get(None))
    assertThrows[NoSuchElementException](s2A.get(Some(-1)))
    s2A.get(Some(0)) shouldBe 6
    s2A.get(Some(1)) shouldBe 3
    assertThrows[NoSuchElementException](s2A.get(Some(2)))
    s2A.get(Some(3)) shouldBe 8
    s2A.get(Some(4)) shouldBe 4
    assertThrows[NoSuchElementException](s2A.get(Some(-5)))

    assertThrows[NoSuchElementException](s.get(None))
    assertThrows[NoSuchElementException](s.get(Some(-1)))
    s.get(Some(0)) shouldBe 6
    s.get(Some(1)) shouldBe 3
    assertThrows[NoSuchElementException](s.get(Some(2)))
    s.get(Some(3)) shouldBe 8
    s.get(Some(4)) shouldBe 4
    assertThrows[NoSuchElementException](s.get(Some(-5)))

    assertThrows[NoSuchElementException](s1CSliced.get(None))
    assertThrows[NoSuchElementException](s1CSliced.get(Some(-1)))
    s1CSliced.get(Some(0)) shouldBe "ghi"
    s1CSliced.get(Some(1)) shouldBe "ABC"
    s1CSliced.get(Some(2)) shouldBe "XyZ"
    assertThrows[NoSuchElementException](s1CSliced.get(Some(3)))
    assertThrows[NoSuchElementException](s1CSliced.get(Some(4)))
    assertThrows[NoSuchElementException](s1CSliced.get(Some(5)))
  }

  test("hashCode: Int") {
    Set(
      s1A.hashCode,
      s1B.hashCode,
      s1C.hashCode,
      s1D.hashCode,
      s2A.hashCode,
      s2B.hashCode,
      s2C.hashCode,
      s2D.hashCode,
      s1ASliced.hashCode,
      s1BSliced.hashCode,
      s1CSliced.hashCode,
      s1DSliced.hashCode,
    ).size shouldBe 8
  }

  test("hasSameDefined[T2: ClassTag](series: Series[T2]): Boolean") {
    s1A hasSameDefined Series(0, 1, 2) shouldBe false
    s1A hasSameDefined Series(0, 1, 2, 3, 4) shouldBe true
    s1ASliced hasSameDefined Series(0, 1, null, 3, 4) shouldBe true

    s1A hasSameDefined s1B shouldBe true
    s2A hasSameDefined s2B shouldBe false
    s1ASliced hasSameDefined s2ASliced shouldBe true
    s1BSliced hasSameDefined s1A(1 to 4) shouldBe true
    s1BSliced hasSameDefined s2A(1 to 4) shouldBe false
    s1ASliced hasSameDefined s2BSliced shouldBe false
    s2A hasSameDefined s2ASliced shouldBe true
    s2ASliced hasSameDefined s2A shouldBe true
  }

  test("hasSameIndex[T2: ClassTag](series: Series[T2]): Boolean") {
    s1A hasSameIndex Series(0, 1, 2) shouldBe false

    s1A hasSameIndex s1B shouldBe true
    s2A hasSameIndex s2B shouldBe true
    s1ASliced hasSameIndex s2ASliced shouldBe true
    s1BSliced hasSameIndex s2A(1 to 4) shouldBe true
    s1ASliced hasSameIndex s2BSliced shouldBe false
    s2A hasSameIndex s2ASliced shouldBe false
    s2ASliced hasSameIndex s2A shouldBe false

  }

  test("hasUndefined: Boolean") {
    s1A.hasUndefined shouldBe false
    s1B.hasUndefined shouldBe false
    s1C.hasUndefined shouldBe false
    s1D.hasUndefined shouldBe false

    s2A.hasUndefined shouldBe true
    s2B.hasUndefined shouldBe true
    s2C.hasUndefined shouldBe true
    s2D.hasUndefined shouldBe true

    s1ASliced.hasUndefined shouldBe true
    s1BSliced.hasUndefined shouldBe true
    s1CSliced.hasUndefined shouldBe true
    s1DSliced.hasUndefined shouldBe true

    s2ASliced.hasUndefined shouldBe true
    s2BSliced.hasUndefined shouldBe true
    s2CSliced.hasUndefined shouldBe true
    s2DSliced.hasUndefined shouldBe true
  }

  test("headOption: Option[T]") {
    s1A.headOption shouldBe Some(6)
    s1B.headOption shouldBe Some(23.1)
    s1BSliced.headOption shouldBe Some(1.4)
    s1CSliced.headOption shouldBe Some("ghi")
    s1D.headOption shouldBe Some(C1("A", 5))
    s2B.headOption shouldBe None
    s2C.headOption shouldBe Some("ghi")
    s1B.sorted.headOption shouldBe Some(1.4)
    s1C.sorted.headOption shouldBe Some("ABC")
    s2B.sorted.headOption shouldBe Some(1.4)
    s2C.sorted(ascNullsFirst).headOption shouldBe None
    Series[Int](null, null).headOption shouldBe None
  }

  test("headValue: Option[T]") {
    s1A.headValue shouldBe Some(6)
    s1B.headValue shouldBe Some(23.1)
    s1BSliced.headValue shouldBe Some(1.4)
    s1CSliced.headValue shouldBe Some("ghi")
    s1D.headValue shouldBe Some(C1("A", 5))
    s2B.headValue shouldBe Some(1.4)
    s2C.headValue shouldBe Some("ghi")
    s1B.sorted.headValue shouldBe Some(1.4)
    s1C.sorted.headValue shouldBe Some("ABC")
    s2B.sorted.headValue shouldBe Some(1.4)
    s2C.sorted(ascNullsFirst).headValue shouldBe Some("ABC")
    Series[Int](null, null).headValue shouldBe None
  }

  test("indexEmpty: Boolean") {
    s1A.indexEmpty shouldBe false
    s1ASliced.indexEmpty shouldBe false
    s2A.indexEmpty shouldBe false
    s2ASliced.indexEmpty shouldBe false
    s2A(Seq(1)).indexEmpty shouldBe false
    s2A(Seq(2)).indexEmpty shouldBe false
    s2C(3 to 4).indexEmpty shouldBe false
    s2A(Seq()).indexEmpty shouldBe true
    s1ASliced(Seq()).indexEmpty shouldBe true
    s1C(Seq()).indexEmpty shouldBe true
    Series.empty[Int].indexEmpty shouldBe true
    Series[Int](null, null).indexEmpty shouldBe false
  }

  test("indexIterator: Iterator[Int]") {
    {
      val it = s1A.indexIterator
      var ix = 0
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
      it.hasNext shouldBe false
    }
    {
      val it = s1A.indexIterator
      var ix = 0
      it.hasNext shouldBe true
      it.next shouldBe 0
    }
    {
      val it = s1ASliced.indexIterator
      var ix = 0
      it.hasNext shouldBe true
      it.next shouldBe 0
      it.hasNext shouldBe true
      it.next shouldBe 1
      it.hasNext shouldBe true
      it.next shouldBe 3
      it.hasNext shouldBe true
      it.next shouldBe 4
      it.hasNext shouldBe false
    }
    {
      val it = s1ASorted.indexIterator
      var ix = 0
      it.hasNext shouldBe true
      it.next shouldBe 2
      it.hasNext shouldBe true
      it.next shouldBe 1
      it.hasNext shouldBe true
      it.next shouldBe 4
      it.hasNext shouldBe true
      it.next shouldBe 0
      it.hasNext shouldBe true
      it.next shouldBe 3
      it.hasNext shouldBe false
    }
    {
      val it = s2C.indexIterator
      var ix = 0
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
      it.hasNext shouldBe false
    }
  }

  test("indexNonEmpty: Boolean") {
    s1A.indexNonEmpty shouldBe true
    s1ASliced.indexNonEmpty shouldBe true
    s2A.indexNonEmpty shouldBe true
    s2ASliced.indexNonEmpty shouldBe true
    s2A(Seq(1)).indexNonEmpty shouldBe true
    s2A(Seq(2)).indexNonEmpty shouldBe true
    s2C(3 to 4).indexNonEmpty shouldBe true
    s2A(Seq()).indexNonEmpty shouldBe false
    s1ASliced(Seq()).indexNonEmpty shouldBe false
    s1C(Seq()).indexNonEmpty shouldBe false
    Series.empty[Int].indexNonEmpty shouldBe false
    Series[Int](null, null).indexNonEmpty shouldBe true
  }

  test("info: String") {
    s1A.info should include("'A'")
    s1A.info should include("[Int]")
    s1A.info should include("no undefined entries")
    s1A.info should include("Uniform Index (0 to 4)")
    s2B.info should include("'B'")
    s2B.info should include("[Double?]")
    s2B.info should include("with undefined entries")
    s2B.info should include("Uniform Index (0 to 4)")
    s2DSliced.info should include("'D'")
    s2DSliced.info should include("[C1?]")
    s2DSliced.info should include("with undefined entries")
    s2DSliced.info should include("Sequential Index (0, 2, 3, 4)")
  }

  test("isDefined: Boolean") {
    s1A.isDefined shouldBe true
    s1B.isDefined shouldBe true
    s1C.isDefined shouldBe true
    s1D.isDefined shouldBe true

    s2A.isDefined shouldBe false
    s2B.isDefined shouldBe false
    s2C.isDefined shouldBe false
    s2D.isDefined shouldBe false

    s1ASliced.isDefined shouldBe false
    s1BSliced.isDefined shouldBe false
    s1CSliced.isDefined shouldBe false
    s1DSliced.isDefined shouldBe false

    s2ASliced.isDefined shouldBe false
    s2BSliced.isDefined shouldBe false
    s2CSliced.isDefined shouldBe false
    s2DSliced.isDefined shouldBe false
  }

  test("isEmpty: Boolean") {
    s1A.isEmpty shouldBe false
    s1ASliced.isEmpty shouldBe false
    s1A(Seq()).isEmpty shouldBe true
    s2A.isEmpty shouldBe false
    s2ASliced.isEmpty shouldBe false
    s2A(Seq(1)).isEmpty shouldBe false
    s2A(Seq(2)).isEmpty shouldBe true
    s2B(Seq(0)).isEmpty shouldBe true
    s2C(3 to 4).isEmpty shouldBe true
    s2A(Seq()).isEmpty shouldBe true
    s1ASliced(Seq()).isEmpty shouldBe true
    s1C(Seq()).isEmpty shouldBe true
    Series.empty[Int].isEmpty shouldBe true
    Series[Int](null, null).isEmpty shouldBe true
  }

  test("isType[T2: Typeable]: Boolean") {
    s1A.isType[Int] shouldBe true
    s1A.isType[Double] shouldBe false
    s1A.isType[String] shouldBe false
    s1A.isType[C1] shouldBe false
    s1B.isType[Int] shouldBe false
    s1B.isType[Double] shouldBe true
    s1B.isType[String] shouldBe false
    s1B.isType[C1] shouldBe false
    s1C.isType[Int] shouldBe false
    s1C.isType[Double] shouldBe false
    s1C.isType[String] shouldBe true
    s1C.isType[C1] shouldBe false
    s1D.isType[Int] shouldBe false
    s1D.isType[Double] shouldBe false
    s1D.isType[String] shouldBe false
    s1D.isType[C1] shouldBe true

    s2A.isType[Int] shouldBe true
    s2A.isType[Double] shouldBe false
    s2A.isType[String] shouldBe false
    s2A.isType[C1] shouldBe false
    s2B.isType[Int] shouldBe false
    s2B.isType[Double] shouldBe true
    s2B.isType[String] shouldBe false
    s2B.isType[C1] shouldBe false
    s2C.isType[Int] shouldBe false
    s2C.isType[Double] shouldBe false
    s2C.isType[String] shouldBe true
    s2C.isType[C1] shouldBe false
    s2D.isType[Int] shouldBe false
    s2D.isType[Double] shouldBe false
    s2D.isType[String] shouldBe false
    s2D.isType[C1] shouldBe true

    s1DOfC2.isType[C2] shouldBe true
    s1DOfC2.isType[C1] shouldBe true
    s1A.isType[Any] shouldBe true
    s1C.isType[Any] shouldBe true
    s1B.isType[Any] shouldBe true
    s1D.isType[Any] shouldBe true

    Series(C2(), 1, 1.1).isType[C2] shouldBe true
    Series(C2(), 1, 1.1).isType[C1] shouldBe true
    Series(C2(), 1, 1.1).isType[Any] shouldBe true
    Series(C2(), 1, 1.1).isType[String] shouldBe false
    Series(true, 1, 1.1).isType[Boolean] shouldBe false
    Series(1.0, 1, "A").isType[Double] shouldBe false
    Series(1.0, 1, 1.1).isType[Double] shouldBe true
    Series(1, "1", 1.1).isType[Int] shouldBe false
    Series("1", 1, 1.1).isType[String] shouldBe false

    s1ASliced.isType[Int] shouldBe true
    Series(C2(), null, 1.1).isType[C2] shouldBe true
    Series(C2(), null, 1.1).isType[C1] shouldBe true
    Series(C2(), null, 1.1).isType[Any] shouldBe true
    Series(C2(), null, 1.1).isType[String] shouldBe false
    Series(true, null, 1.1).isType[Boolean] shouldBe false
    Series(1.0, null, 1.1).isType[Double] shouldBe true
    Series(1, null, 1.1).isType[Int] shouldBe false
    Series("1", null, 1.1).isType[String] shouldBe false
  }

  test("isTypeAll[T2: Typeable]: Boolean") {
    s1A.isTypeAll[Int] shouldBe true
    s1A.isTypeAll[Double] shouldBe false
    s1A.isTypeAll[String] shouldBe false
    s1A.isTypeAll[C1] shouldBe false
    s1B.isTypeAll[Int] shouldBe false
    s1B.isTypeAll[Double] shouldBe true
    s1B.isTypeAll[String] shouldBe false
    s1B.isTypeAll[C1] shouldBe false
    s1C.isTypeAll[Int] shouldBe false
    s1C.isTypeAll[Double] shouldBe false
    s1C.isTypeAll[String] shouldBe true
    s1C.isTypeAll[C1] shouldBe false
    s1D.isTypeAll[Int] shouldBe false
    s1D.isTypeAll[Double] shouldBe false
    s1D.isTypeAll[String] shouldBe false
    s1D.isTypeAll[C1] shouldBe true

    s2A.isTypeAll[Int] shouldBe false
    s2A.isTypeAll[Double] shouldBe false
    s2A.isTypeAll[String] shouldBe false
    s2A.isTypeAll[C1] shouldBe false
    s2B.isTypeAll[Int] shouldBe false
    s2B.isTypeAll[Double] shouldBe false
    s2B.isTypeAll[String] shouldBe false
    s2B.isTypeAll[C1] shouldBe false
    s2C.isTypeAll[Int] shouldBe false
    s2C.isTypeAll[Double] shouldBe false
    s2C.isTypeAll[String] shouldBe false
    s2C.isTypeAll[C1] shouldBe false
    s2D.isTypeAll[Int] shouldBe false
    s2D.isTypeAll[Double] shouldBe false
    s2D.isTypeAll[String] shouldBe false
    s2D.isTypeAll[C1] shouldBe false

    s2ASliced.isTypeAll[Int] shouldBe false
    s2ASliced.isTypeAll[Double] shouldBe false
    s2ASliced.isTypeAll[String] shouldBe false
    s2ASliced.isTypeAll[C1] shouldBe false
    s2BSliced.isTypeAll[Int] shouldBe false
    s2BSliced.isTypeAll[Double] shouldBe false
    s2BSliced.isTypeAll[String] shouldBe false
    s2BSliced.isTypeAll[C1] shouldBe false
    s2CSliced.isTypeAll[Int] shouldBe false
    s2CSliced.isTypeAll[Double] shouldBe false
    s2CSliced.isTypeAll[String] shouldBe false
    s2CSliced.isTypeAll[C1] shouldBe false
    s2DSliced.isTypeAll[Int] shouldBe false
    s2DSliced.isTypeAll[Double] shouldBe false
    s2DSliced.isTypeAll[String] shouldBe false
    s2DSliced.isTypeAll[C1] shouldBe false

    s1DOfC2.isTypeAll[C2] shouldBe true
    s1DOfC2.isTypeAll[C1] shouldBe true
    s1A.isTypeAll[Any] shouldBe true
    s1C.isTypeAll[Any] shouldBe true
    s1B.isTypeAll[Any] shouldBe true
    s1D.isTypeAll[Any] shouldBe true

    Series(C2(), 1, 1.1).isTypeAll[C2] shouldBe true
    Series(C2(), 1, 1.1).isTypeAll[C1] shouldBe true
    Series(C2(), 1, 1.1).isTypeAll[Any] shouldBe true
    Series(C2(), 1, 1.1).isTypeAll[String] shouldBe false
    Series(true, 1, 1.1).isTypeAll[Boolean] shouldBe false
    Series(1.0, 1, "A").isTypeAll[Double] shouldBe false
    Series(1.0, 1, 1.1).isTypeAll[Double] shouldBe true
    Series(1, 1, 1.1).isTypeAll[Int] shouldBe false
    Series("1", 1, 1.1).isTypeAll[String] shouldBe false

    s1ASliced.isTypeAll[Int] shouldBe false
    Series(C2(), null, 1.1).isTypeAll[C2] shouldBe false
    Series(C2(), null, 1.1).isTypeAll[C1] shouldBe false
    Series(C2(), null, 1.1).isTypeAll[Any] shouldBe false
    Series(C2(), null, 1.1).isTypeAll[String] shouldBe false
    Series(true, null, 1.1).isTypeAll[Boolean] shouldBe false
    Series(1.0, null, 1.1).isTypeAll[Double] shouldBe false
    Series(1, null, 1.1).isTypeAll[Int] shouldBe false
    Series("1", null, 1.1).isTypeAll[String] shouldBe false
  }

  test("isTypeAllStrictly[T2: Typeable]: Boolean") {
    s1A.isTypeAllStrictly[Int] shouldBe true
    s1A.isTypeAllStrictly[Double] shouldBe false
    s1A.isTypeAllStrictly[String] shouldBe false
    s1A.isTypeAllStrictly[C1] shouldBe false
    s1B.isTypeAllStrictly[Int] shouldBe false
    s1B.isTypeAllStrictly[Double] shouldBe true
    s1B.isTypeAllStrictly[String] shouldBe false
    s1B.isTypeAllStrictly[C1] shouldBe false
    s1C.isTypeAllStrictly[Int] shouldBe false
    s1C.isTypeAllStrictly[Double] shouldBe false
    s1C.isTypeAllStrictly[String] shouldBe true
    s1C.isTypeAllStrictly[C1] shouldBe false
    s1D.isTypeAllStrictly[Int] shouldBe false
    s1D.isTypeAllStrictly[Double] shouldBe false
    s1D.isTypeAllStrictly[String] shouldBe false
    s1D.isTypeAllStrictly[C1] shouldBe true

    s2A.isTypeAllStrictly[Int] shouldBe false
    s2A.isTypeAllStrictly[Double] shouldBe false
    s2A.isTypeAllStrictly[String] shouldBe false
    s2A.isTypeAllStrictly[C1] shouldBe false
    s2B.isTypeAllStrictly[Int] shouldBe false
    s2B.isTypeAllStrictly[Double] shouldBe false
    s2B.isTypeAllStrictly[String] shouldBe false
    s2B.isTypeAllStrictly[C1] shouldBe false
    s2C.isTypeAllStrictly[Int] shouldBe false
    s2C.isTypeAllStrictly[Double] shouldBe false
    s2C.isTypeAllStrictly[String] shouldBe false
    s2C.isTypeAllStrictly[C1] shouldBe false
    s2D.isTypeAllStrictly[Int] shouldBe false
    s2D.isTypeAllStrictly[Double] shouldBe false
    s2D.isTypeAllStrictly[String] shouldBe false
    s2D.isTypeAllStrictly[C1] shouldBe false

    s2ASliced.isTypeAllStrictly[Int] shouldBe false
    s2ASliced.isTypeAllStrictly[Double] shouldBe false
    s2ASliced.isTypeAllStrictly[String] shouldBe false
    s2ASliced.isTypeAllStrictly[C1] shouldBe false
    s2BSliced.isTypeAllStrictly[Int] shouldBe false
    s2BSliced.isTypeAllStrictly[Double] shouldBe false
    s2BSliced.isTypeAllStrictly[String] shouldBe false
    s2BSliced.isTypeAllStrictly[C1] shouldBe false
    s2CSliced.isTypeAllStrictly[Int] shouldBe false
    s2CSliced.isTypeAllStrictly[Double] shouldBe false
    s2CSliced.isTypeAllStrictly[String] shouldBe false
    s2CSliced.isTypeAllStrictly[C1] shouldBe false
    s2DSliced.isTypeAllStrictly[Int] shouldBe false
    s2DSliced.isTypeAllStrictly[Double] shouldBe false
    s2DSliced.isTypeAllStrictly[String] shouldBe false
    s2DSliced.isTypeAllStrictly[C1] shouldBe false

    s1DOfC2.isTypeAllStrictly[C2] shouldBe true
    s1DOfC2.isTypeAllStrictly[C1] shouldBe true
    s1A.isTypeAllStrictly[Any] shouldBe true
    s1C.isTypeAllStrictly[Any] shouldBe true
    s1B.isTypeAllStrictly[Any] shouldBe true
    s1D.isTypeAllStrictly[Any] shouldBe true

    Series(C2(), 1, 1.1).isTypeAllStrictly[C2] shouldBe false
    Series(C2(), 1, 1.1).isTypeAllStrictly[C1] shouldBe false
    Series(C2(), 1, 1.1).isTypeAllStrictly[Any] shouldBe true
    Series(C2(), 1, 1.1).isTypeAllStrictly[String] shouldBe false
    Series(true, 1, 1.1).isTypeAllStrictly[Boolean] shouldBe false
    Series(1.0, 1, "A").isTypeAllStrictly[Double] shouldBe false
    Series(1.0, 1, 1.1).isTypeAllStrictly[Double] shouldBe true
    Series(1, "1", 1.1).isTypeAllStrictly[Int] shouldBe false
    Series("1", 1, 1.1).isTypeAllStrictly[String] shouldBe false

    s1ASliced.isTypeAllStrictly[Int] shouldBe false
    Series(C2(), null, 1.1).isTypeAllStrictly[C2] shouldBe false
    Series(C2(), null, 1.1).isTypeAllStrictly[C1] shouldBe false
    Series(C2(), null, 1.1).isTypeAllStrictly[Any] shouldBe false
    Series(C2(), null, 1.1).isTypeAllStrictly[String] shouldBe false
    Series(true, null, 1.1).isTypeAllStrictly[Boolean] shouldBe false
    Series(1.0, null, 1.1).isTypeAllStrictly[Double] shouldBe false
    Series(1, null, 1.1).isTypeAllStrictly[Int] shouldBe false
    Series("1", null, 1.1).isTypeAllStrictly[String] shouldBe false
  }

  test("isTypeStrictly[T2: Typeable]: Boolean") {
    s1A.isTypeStrictly[Int] shouldBe true
    s1A.isTypeStrictly[Double] shouldBe false
    s1A.isTypeStrictly[String] shouldBe false
    s1A.isTypeStrictly[C1] shouldBe false
    s1B.isTypeStrictly[Int] shouldBe false
    s1B.isTypeStrictly[Double] shouldBe true
    s1B.isTypeStrictly[String] shouldBe false
    s1B.isTypeStrictly[C1] shouldBe false
    s1C.isTypeStrictly[Int] shouldBe false
    s1C.isTypeStrictly[Double] shouldBe false
    s1C.isTypeStrictly[String] shouldBe true
    s1C.isTypeStrictly[C1] shouldBe false
    s1D.isTypeStrictly[Int] shouldBe false
    s1D.isTypeStrictly[Double] shouldBe false
    s1D.isTypeStrictly[String] shouldBe false
    s1D.isTypeStrictly[C1] shouldBe true

    s2A.isTypeStrictly[Int] shouldBe true
    s2A.isTypeStrictly[Double] shouldBe false
    s2A.isTypeStrictly[String] shouldBe false
    s2A.isTypeStrictly[C1] shouldBe false
    s2B.isTypeStrictly[Int] shouldBe false
    s2B.isTypeStrictly[Double] shouldBe true
    s2B.isTypeStrictly[String] shouldBe false
    s2B.isTypeStrictly[C1] shouldBe false
    s2C.isTypeStrictly[Int] shouldBe false
    s2C.isTypeStrictly[Double] shouldBe false
    s2C.isTypeStrictly[String] shouldBe true
    s2C.isTypeStrictly[C1] shouldBe false
    s2D.isTypeStrictly[Int] shouldBe false
    s2D.isTypeStrictly[Double] shouldBe false
    s2D.isTypeStrictly[String] shouldBe false
    s2D.isTypeStrictly[C1] shouldBe true

    s1DOfC2.isTypeStrictly[C2] shouldBe true
    s1DOfC2.isTypeStrictly[C1] shouldBe true
    s1A.isTypeStrictly[Any] shouldBe true
    s1C.isTypeStrictly[Any] shouldBe true
    s1B.isTypeStrictly[Any] shouldBe true
    s1D.isTypeStrictly[Any] shouldBe true

    Series(C2(), 1, 1.1).isTypeStrictly[C2] shouldBe false
    Series(C2(), 1, 1.1).isTypeStrictly[C1] shouldBe false
    Series(C2(), 1, 1.1).isTypeStrictly[Any] shouldBe true
    Series(C2(), 1, 1.1).isTypeStrictly[String] shouldBe false
    Series(true, 1, 1.1).isTypeStrictly[Boolean] shouldBe false
    Series(1.0, 1, "A").isTypeStrictly[Double] shouldBe false
    Series(1.0, 1, 1.1).isTypeStrictly[Double] shouldBe true
    Series(1, "1", 1.1).isTypeStrictly[Int] shouldBe false
    Series("1", 1, 1.1).isTypeStrictly[String] shouldBe false

    s1ASliced.isTypeStrictly[Int] shouldBe true
    Series(C2(), null, 1.1).isTypeStrictly[C2] shouldBe false
    Series(C2(), null, 1.1).isTypeStrictly[C1] shouldBe false
    Series(C2(), null, 1.1).isTypeStrictly[Any] shouldBe true
    Series(C2(), null, 1.1).isTypeStrictly[String] shouldBe false
    Series(true, null, 1.1).isTypeStrictly[Boolean] shouldBe false
    Series(1.0, null, 1.1).isTypeStrictly[Double] shouldBe true
    Series(1, null, 1.1).isTypeStrictly[Int] shouldBe false
    Series("1", null, 1.1).isTypeStrictly[String] shouldBe false
  }

  test("iterator: Iterator[Option[T]]") {
    {
      val it = s1A.iterator
      var ix = 0
      it.hasNext shouldBe true
      it.next.get shouldBe 6
      it.hasNext shouldBe true
      it.next.get shouldBe 3
      it.hasNext shouldBe true
      it.next.get shouldBe 2
      it.hasNext shouldBe true
      it.next.get shouldBe 8
      it.hasNext shouldBe true
      it.next.get shouldBe 4
      it.hasNext shouldBe false
    }
    {
      val it = s1A.iterator
      var ix = 0
      it.hasNext shouldBe true
      it.next.get shouldBe 6
    }
    {
      val it = s1ASliced.iterator
      var ix = 0
      it.hasNext shouldBe true
      it.next.get shouldBe 6
      it.hasNext shouldBe true
      it.next.get shouldBe 3
      it.hasNext shouldBe true
      it.next.get shouldBe 8
      it.hasNext shouldBe true
      it.next.get shouldBe 4
      it.hasNext shouldBe false
    }
    {
      val it = s1ASorted.iterator
      var ix = 0
      it.hasNext shouldBe true
      it.next.get shouldBe 2
      it.hasNext shouldBe true
      it.next.get shouldBe 3
      it.hasNext shouldBe true
      it.next.get shouldBe 4
      it.hasNext shouldBe true
      it.next.get shouldBe 6
      it.hasNext shouldBe true
      it.next.get shouldBe 8
      it.hasNext shouldBe false
    }
    {
      val it = s2C.iterator
      var ix = 0
      it.hasNext shouldBe true
      it.next.get shouldBe "ghi"
      it.hasNext shouldBe true
      it.next.get shouldBe "ABC"
      it.hasNext shouldBe true
      it.next.get shouldBe "XyZ"
      it.hasNext shouldBe true
      it.next shouldBe None
      it.hasNext shouldBe true
      it.next shouldBe None
      it.hasNext shouldBe false
    }
  }

  test("last: Option[Int]") {
    Series.empty[Int].last shouldBe None
    Series(1, 2, 3).last shouldBe Some(2)
    Series(null, 2, 3).last shouldBe Some(2)
    Series(1, null, null).last shouldBe Some(0)
    Series(null, 2, null).last shouldBe Some(1)
    s1A(Seq[Int]()).last shouldBe None

    Series.empty[Boolean].last shouldBe None
    Series(true, true, true).last shouldBe Some(2)
    Series(false, true, false).last shouldBe Some(1)
    Series(false, true, true).last shouldBe Some(2)
    Series(false, false, true).last shouldBe Some(2)
    Series(true, null, false).last shouldBe Some(0)
    Series(false, null, true).last shouldBe Some(2)
    Series(false, null, null).last shouldBe None
    Series(null, true, null).last shouldBe Some(1)
    Series(true, true, true).apply(Seq(0, 1)).last shouldBe Some(1)
    Series(true, true, true).apply(Seq()).last shouldBe None

    s1A.last shouldBe Some(4)
    s1B.last shouldBe Some(4)
    s1C.last shouldBe Some(4)
    s1D.last shouldBe Some(4)
    s2A.last shouldBe Some(4)
    s2B.last shouldBe Some(4)
    s2C.last shouldBe Some(2)
    s2D.last shouldBe Some(4)
    s1ASliced.last shouldBe Some(4)
    s1BSliced.last shouldBe Some(4)
    s1CSliced.last shouldBe Some(2)
    s1DSliced.last shouldBe Some(4)

    s1A.sorted.last shouldBe Some(3)
    s1B.sorted.last shouldBe Some(0)
    s1C.sorted.last shouldBe Some(2)
    s1D.sorted.last shouldBe Some(0)
  }

  test("last(value: T): Option[Int]") {
    s1A.last(6) shouldBe Some(0)
    s1A.last(3) shouldBe Some(1)
    s1A.last(2) shouldBe Some(2)
    s1A.last(8) shouldBe Some(3)
    s1A.last(4) shouldBe Some(4)
    s1A.last(1) shouldBe None
    s1B.last(1.4) shouldBe Some(2)
    s1B.last(7.0) shouldBe Some(3)
    s1B.last(1.0) shouldBe None
    s1C.last("ABC") shouldBe Some(1)
    s1C.last("ABc") shouldBe None
    s1D.last(C1("D", 2)) shouldBe Some(3)
    s1D.last(C1("D", 7)) shouldBe None

    s1A.sorted.last(4) shouldBe Some(4)
    s1A.sorted.last(1) shouldBe None
    s1B.sorted.last(1.4) shouldBe Some(2)
    s1B((0 to 4).reverse.toArray).last(1.4) shouldBe Some(1)
    s1B.sorted.last(7.0) shouldBe Some(3)
    s1B.sorted.last(1.0) shouldBe None
    s1C.sorted.last("ABC") shouldBe Some(1)
    s1C.sorted.last("ABc") shouldBe None
    s1D.sorted.last(C1("D", 2)) shouldBe Some(3)
    s1D.sorted.last(C1("D", 7)) shouldBe None

    s1ASliced.last(4) shouldBe Some(4)
    s1ASliced.last(2) shouldBe None
    s1BSliced.last(1.4) shouldBe Some(2)
    s1BSliced.last(23.1) shouldBe None
    s1CSliced.last("ABC") shouldBe Some(1)
    s1CSliced.last("qqq") shouldBe None
    s1DSliced.last(C1("D", 2)) shouldBe Some(3)
    s1DSliced.last(C1("B", 1)) shouldBe None

    Series.empty[Int].last(1) shouldBe None
  }

  test("last[T2](series: Series[T2], value: T2): Option[T]") {
    val s = Series(0.0, 1.0, 2.0, 3.0, 4.0)

    s.last(s1A, 6) shouldBe Some(0.0)
    s.last(s1A, 3) shouldBe Some(1.0)
    s.last(s1A, 2) shouldBe Some(2.0)
    s.last(s1A, 8) shouldBe Some(3.0)
    s.last(s1A, 4) shouldBe Some(4.0)
    s.last(s1A, 1) shouldBe None
    s.last(s1B, 1.4) shouldBe Some(2.0)
    s.last(s1B, 7.0) shouldBe Some(3.0)
    s.last(s1B, 1.0) shouldBe None
    s.last(s1C, "ABC") shouldBe Some(1.0)
    s.last(s1C, "ABc") shouldBe None
    s.last(s1D, C1("D", 2)) shouldBe Some(3.0)
    s.last(s1D, C1("D", 7)) shouldBe None

    s.last(s1A.sorted, 4) shouldBe Some(4.0)
    s.last(s1A.sorted, 1) shouldBe None
    s.last(s1B.sorted, 1.4) shouldBe Some(2.0)
    s.last(s1B((0 to 4).reverse.toArray), 1.4) shouldBe Some(1.0)
    s.last(s1B.sorted, 7.0) shouldBe Some(3.0)
    s.last(s1B.sorted, 1.0) shouldBe None
    s.last(s1C.sorted, "ABC") shouldBe Some(1.0)
    s.last(s1C.sorted, "ABc") shouldBe None
    s.last(s1D.sorted, C1("D", 2)) shouldBe Some(3.0)
    s.last(s1D.sorted, C1("D", 7)) shouldBe None

    s.last(s1ASliced, 4) shouldBe Some(4.0)
    s.last(s1ASliced, 2) shouldBe None
    s.last(s1BSliced, 1.4) shouldBe Some(2.0)
    s.last(s1BSliced, 23.1) shouldBe None
    s.last(s1CSliced, "ABC") shouldBe Some(1.0)
    s.last(s1CSliced, "qqq") shouldBe None
    s.last(s1DSliced, C1("D", 2)) shouldBe Some(3.0)
    s.last(s1DSliced, C1("B", 1)) shouldBe None

    Series.empty[Double].last(Series.empty[Int], 1) shouldBe None
    assertThrows[BaseIndexException](s.last(Series(1, 2), 2))

    val s2 = Series(0.0, null, 2.0, 3.0, 4.0).apply(Seq(0, 1, 2, 4))
    s2.last(s1A, 6) shouldBe Some(0.0)
    s2.last(s1A, 3) shouldBe None
    s2.last(s1A, 2) shouldBe Some(2.0)
    s2.last(s1A, 8) shouldBe None
    s2.last(s1A, 4) shouldBe Some(4.0)
    s2.last(s1A, 1) shouldBe None
  }

  test("last[T2](series: Series[T2], value: T2, default: => T): T") {
    val s = Series(0.0, 1.0, 2.0, 3.0, 4.0)

    s.last(s1A, 6, -1) shouldBe 0.0
    s.last(s1A, 3, -1) shouldBe 1.0
    s.last(s1A, 2, -1) shouldBe 2.0
    s.last(s1A, 8, -1) shouldBe 3.0
    s.last(s1A, 4, -1) shouldBe 4.0
    s.last(s1A, 1, -1) shouldBe -1.0
    s.last(s1B, 1.4, 2.0) shouldBe 2.0
    s.last(s1B, 7.0, 2.0) shouldBe 3.0
    s.last(s1B, 1.0, 2.0) shouldBe 2.0
    s.last(s1C, "ABC", -1.0) shouldBe 1.0
    s.last(s1C, "ABc", -1.0) shouldBe -1.0
    s.last(s1D, C1("D", 2), -1.0) shouldBe 3.0
    s.last(s1D, C1("D", 7), -1.0) shouldBe -1.0

    s.last(s1A.sorted, 4, -1) shouldBe 4.0
    s.last(s1A.sorted, 1, -1) shouldBe -1.0
    s.last(s1B.sorted, 1.4, 2.0) shouldBe 2.0
    s.last(s1B((0 to 4).reverse.toArray), 1.4, 2.0) shouldBe 1.0
    s.last(s1B.sorted, 7.0, 2.0) shouldBe 3.0
    s.last(s1B.sorted, 1.0, 2.0) shouldBe 2.0
    s.last(s1C.sorted, "ABC", -1.0) shouldBe 1.0
    s.last(s1C.sorted, "ABc", -1.0) shouldBe -1.0
    s.last(s1D.sorted, C1("D", 2), -1.0) shouldBe 3.0
    s.last(s1D.sorted, C1("D", 7), -1.0) shouldBe -1.0

    s.last(s1ASliced, 4, -1) shouldBe 4.0
    s.last(s1ASliced, 2, -1) shouldBe -1.0
    s.last(s1BSliced, 1.4, -2.0) shouldBe 2.0
    s.last(s1BSliced, 23.1, -2.0) shouldBe -2.0
    s.last(s1CSliced, "ABC", -1.0) shouldBe 1.0
    s.last(s1CSliced, "qqq", -1.0) shouldBe -1.0
    s.last(s1DSliced, C1("D", 2), -1.0) shouldBe 3.0
    s.last(s1DSliced, C1("B", 1), -1.0) shouldBe -1.0

    Series.empty[Double].last(Series.empty[Int], 1, -1.0) shouldBe -1.0
    assertThrows[BaseIndexException](s.last(Series(1, 2), 2, -1.0))

    val s2 = Series(0.0, null, 2.0, 3.0, 4.0).apply(Seq(0, 1, 2, 4))
    s2.last(s1A, 6, -1) shouldBe 0.0
    s2.last(s1A, 3, -1) shouldBe -1.0
    s2.last(s1A, 2, -1) shouldBe 2.0
    s2.last(s1A, 8, -1) shouldBe -1.0
    s2.last(s1A, 4, -1) shouldBe 4.0
    s2.last(s1A, 1, -1) shouldBe -1.0
  }

  test("length: Int") {
    // dopy from rows
    s1A.length shouldBe seriesRowLength
    s1B.length shouldBe seriesRowLength
    s1C.length shouldBe seriesRowLength
    s1D.length shouldBe seriesRowLength

    s2A.length shouldBe seriesRowLength
    s2B.length shouldBe seriesRowLength
    s2C.length shouldBe seriesRowLength
    s2D.length shouldBe seriesRowLength

    s1ASliced.length shouldBe 4
    s1BSliced.length shouldBe 4
    s1CSliced.length shouldBe 3
    s1DSliced.length shouldBe 4
  }

  test("map[R: ClassTag](f: T => R): Series[R]") {
    s1A.map(-_) shouldBe Series("A")(-6, -3, -2, -8, -4)
    s1A.map(x => (x + 1).toString) shouldBe Series("A")("7", "4", "3", "9", "5")
    s1B.map(_ + 0.1) shouldBe Series("B")(23.1 + 0.1, 1.4 + 0.1, 1.4 + 0.1, 7.0 + 0.1, 3.1 + 0.1)
    s1C.map(_.toLowerCase) shouldBe Series("C")("ghi", "abc", "xyz", "qqq", "uuu")
    s1D.map(x => x._2) shouldBe Series("D")(5, 1, 2, 2, 0)

    s1ASliced.map(-_) shouldBe Series("A")(-6, -3, -2, -8, -4).apply(maskA)
    s1ASliced.map(x => (x + 1).toString) shouldBe Series("A")("7", "4", "3", "9", "5").apply(maskA)
    s1BSliced.map(_ + 0.1) shouldBe Series("B")(23.1 + 0.1, 1.4 + 0.1, 1.4 + 0.1, 7.0 + 0.1, 3.1 + 0.1).apply(maskB)
    s1CSliced.map(_.toLowerCase) shouldBe Series("C")("ghi", "abc", "xyz", "qqq", "uuu").apply(maskC)
    s1DSliced.map(x => x._2) shouldBe Series("D")(5, 1, 2, 2, 0).apply(maskD)

    s2A.map(-_) shouldBe Series("A")(-6, -3, null, -8, -4)
    s2A.map(x => (x + 1).toString) shouldBe Series("A")("7", "4", null, "9", "5")
    s2B.map(_ + 0.1) shouldBe Series("B")(null, 1.4 + 0.1, 1.4 + 0.1, 7.0 + 0.1, 3.1 + 0.1)
    s2C.map(_.toLowerCase) shouldBe Series("C")("ghi", "abc", "xyz", null, null)
    s2D.map(x => x._2) shouldBe Series("D")(5, null, 2, 2, 0)

    s1ASorted.map(-_) shouldBe Series("A")(-6, -3, -2, -8, -4).apply(Seq(2, 1, 4, 0, 3))
  }

  test("map[T2: ClassTag, R: ClassTag](series: Series[T2], f: (T, T2) => R): Series[R]") {
    s1A.map(s1A, _ - _) shouldBe Series("A")(0, 0, 0, 0, 0)
    s1A.map(s1B, _.toDouble min _) shouldBe Series("A")(6.0, 1.4, 1.4, 7.0, 3.1)
    s1B.map(s1B, _ + _) shouldBe Series("B")(23.1 + 23.1, 1.4 + 1.4, 1.4 + 1.4, 7.0 + 7.0, 3.1 + 3.1)
    s1C.map(s1C, _.toLowerCase + _) shouldBe Series("C")("ghighi", "abcABC", "xyzXyZ", "qqqqqq", "uuuUuu")
    s1D.map(s1A, _._2 + _) shouldBe Series("D")(11, 4, 4, 10, 4)

    s1ASliced.map(s1A, _ - _) shouldBe Series("A")(0, 0, 0, 0, 0).apply(maskA)
    s1ASliced.map(s1B, _.toDouble min _) shouldBe Series("A")(6.0, 1.4, 1.4, 7.0, 3.1).apply(maskA)
    s1BSliced.map(s1B, _ + _) shouldBe Series("B")(23.1 + 23.1, 1.4 + 1.4, 1.4 + 1.4, 7.0 + 7.0, 3.1 + 3.1).apply(maskB)
    s1CSliced.map(s1C, _.toLowerCase + _) shouldBe Series("C")("ghighi", "abcABC", "xyzXyZ", "qqqqqq", "uuuUuu").apply(
      maskC
    )
    s1DSliced.map(s1A, _._2 + _) shouldBe Series("D")(11, 4, 4, 10, 4).apply(maskD)

    s1A.map(s1ASliced, _ - _) shouldBe Series("A")(0, 0, null, 0, 0)
    s1A.map(s1BSliced, _.toDouble min _) shouldBe Series("A")(null, 1.4, 1.4, 7.0, 3.1)
    s1B.map(s1BSliced, _ + _) shouldBe Series("B")(null, 1.4 + 1.4, 1.4 + 1.4, 7.0 + 7.0, 3.1 + 3.1)
    s1C.map(s1CSliced, _.toLowerCase + _) shouldBe Series("C")("ghighi", "abcABC", "xyzXyZ", null, null)
    s1D.map(s1ASliced, _._2 + _) shouldBe Series("D")(11, 4, null, 10, 4)

    s2A.map(s1A, _ - _) shouldBe Series("A")(0, 0, null, 0, 0)
    s2A.map(s1B, _.toDouble min _) shouldBe Series("A")(6.0, 1.4, null, 7.0, 3.1)
    s2B.map(s1B, _ + _) shouldBe Series("B")(null, 1.4 + 1.4, 1.4 + 1.4, 7.0 + 7.0, 3.1 + 3.1)
    s2C.map(s1C, _.toLowerCase + _) shouldBe Series("C")("ghighi", "abcABC", "xyzXyZ", null, null)
    s2D.map(s1A, _._2 + _) shouldBe Series("D")(11, null, 4, 10, 4)

    s1A.map(s2A, _ - _) shouldBe Series("A")(0, 0, null, 0, 0)
    s1A.map(s2B, _.toDouble min _) shouldBe Series("A")(null, 1.4, 1.4, 7.0, 3.1)
    s1B.map(s2B, _ + _) shouldBe Series("B")(null, 1.4 + 1.4, 1.4 + 1.4, 7.0 + 7.0, 3.1 + 3.1)
    s1C.map(s2C, _.toLowerCase + _) shouldBe Series("C")("ghighi", "abcABC", "xyzXyZ", null, null)
    s1D.map(s2A, _._2 + _) shouldBe Series("D")(11, 4, null, 10, 4)
  }

  test("mutable: MutableSeries[T]") {
    val mA = s2A.mutable
    mA shouldBe s2A
    mA.set(0, -1)
    mA equals s2A shouldBe false
    s2A shouldBe Series("A")(6, 3, null, 8, 4)

    val mB = s1B.mutable
    mB shouldBe s1B
    mB.set(0, -1.0)
    mB equals s1B shouldBe false
    s1B shouldBe Series("B")(23.1, 1.4, 1.4, 7.0, 3.1)

    val mC = s3C.mutable
    mC shouldBe s3C
    mC.index shouldBe s3C.index
    mC.put(1, "X")
    mC equals s3C shouldBe false
    mC.index equals s3C.index shouldBe false
    s3C shouldBe Series("C")("ghi", null, null, "qqq", "Uuu").apply(Seq(0, 2, 3, 4))

    val mD = s2D.mutable
    mD shouldBe s2D
    mD.set(1, C2())
    mD equals s2D shouldBe false
    s2D shouldBe Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("nonEmpty: Boolean") {
    s1A.nonEmpty shouldBe true
    s1ASliced.nonEmpty shouldBe true
    s1A(Seq()).nonEmpty shouldBe false
    s2A.nonEmpty shouldBe true
    s2ASliced.nonEmpty shouldBe true
    s2A(Seq(1)).nonEmpty shouldBe true

    s2A(Seq(2)).isEmpty shouldBe true
    s2A(Seq(2)).nonEmpty shouldBe false

    s2B(Seq(0)).nonEmpty shouldBe false
    s2C(3 to 4).nonEmpty shouldBe false
    s2A(Seq()).nonEmpty shouldBe false
    s1ASliced(Seq()).nonEmpty shouldBe false
    s1C(Seq()).nonEmpty shouldBe false
    Series.empty[Int].nonEmpty shouldBe false
    Series[Int](null, null).nonEmpty shouldBe false
  }

  test("numRows: Int") {
    s1A.numRows shouldBe seriesRowLength
    s1B.numRows shouldBe seriesRowLength
    s1C.numRows shouldBe seriesRowLength
    s1D.numRows shouldBe seriesRowLength

    s2A.numRows shouldBe seriesRowLength
    s2B.numRows shouldBe seriesRowLength
    s2C.numRows shouldBe seriesRowLength
    s2D.numRows shouldBe seriesRowLength

    s1ASliced.numRows shouldBe 4
    s1BSliced.numRows shouldBe 4
    s1CSliced.numRows shouldBe 3
    s1DSliced.numRows shouldBe 4
  }

  test("numRowsBase: Int") {
    s1A.numRowsBase shouldBe seriesRowLength
    s1B.numRowsBase shouldBe seriesRowLength
    s1C.numRowsBase shouldBe seriesRowLength
    s1D.numRowsBase shouldBe seriesRowLength

    s2A.numRowsBase shouldBe seriesRowLength
    s2B.numRowsBase shouldBe seriesRowLength
    s2C.numRowsBase shouldBe seriesRowLength
    s2D.numRowsBase shouldBe seriesRowLength

    s1ASliced.numRowsBase shouldBe seriesRowLength
    s1BSliced.numRowsBase shouldBe seriesRowLength
    s1CSliced.numRowsBase shouldBe seriesRowLength
    s1DSliced.numRowsBase shouldBe seriesRowLength
  }

  test("orElse[T2 <: T](series: Series[T2]): Series[T]") {
    // copy from fill(series: Series[T]): Series[T]
    s2A.orElse(s1A) shouldBe s1A
    s2B.orElse(s1B) shouldBe s1B
    s2C.orElse(s1C) shouldBe s1C
    s2D.orElse(s1D) shouldBe s1D
    s2ASliced.orElse(s1A) shouldBe s2ASliced
    s2BSliced.orElse(s1B) shouldBe s2BSliced
    s2CSliced.orElse(s1C) shouldBe s2CSliced
    s2DSliced.orElse(s1D) shouldBe s2DSliced
    s2ASliced.orElse(s2ASliced) shouldBe s2ASliced
    s2BSliced.orElse(s2BSliced) shouldBe s2BSliced
    s2CSliced.orElse(s2CSliced) shouldBe s2CSliced
    s2DSliced.orElse(s2DSliced) shouldBe s2DSliced

    s2A.toAny.orElse(s1A) shouldBe s1A
    s2B.toAny.orElse(s1B) shouldBe s1B
    s2A.toAny.orElse(s1A.toAny) shouldBe s1A
    s2B.toAny.orElse(s1B.toAny) shouldBe s1B

    assertThrows[SeriesCastException](s2A.toAny.orElse(s1B))
    assertThrows[SeriesCastException](s2B.toAny.orElse(s1C))
    s2D.toAny.orElse(s1DOfC2) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("orElse(value: T): Series[T]") {
    // copy from fill(value: T): Series[T]
    s2A.orElse(2) shouldBe s1A
    s2B.orElse(23.1) shouldBe s1B
    s2C.orElse("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2D.orElse(C1("B", 1)) shouldBe s1D
    s2ASliced.orElse(2) shouldBe s2ASliced
    s2BSliced.orElse(23.1) shouldBe s2BSliced
    s2CSliced.orElse("value") shouldBe s2CSliced
    s2DSliced.orElse(C1("B", 1)) shouldBe s2DSliced

    s2A.toAny.orElse(2) shouldBe s1A
    s2B.toAny.orElse(23.1) shouldBe s1B
    s2C.toAny.orElse("value") shouldBe Series("C")("ghi", "ABC", "XyZ", "value", "value")
    s2D.toAny.orElse(C1("B", 1)) shouldBe s1D

    s2B.orElse(23) shouldBe Series("B")(23.0, 1.4, 1.4, 7.0, 3.1)
    assertThrows[ValueCastException](s2A.toAny.orElse(2.0))
    assertThrows[ValueCastException](s2B.toAny.orElse("23"))
    s2D.toAny.orElse(C2()) shouldBe Series("D")(C1("A", 5), C2(), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("resetIndex: Series[T]") {
    s1A.sorted.resetIndex shouldBe Series("A")(2, 3, 4, 6, 8)
    s1B.sorted.resetIndex shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, 23.1)
    s1C.sorted.resetIndex shouldBe Series("C")("ABC", "ghi", "qqq", "Uuu", "XyZ")
    s1D.sorted.resetIndex shouldBe Series("D")(C1("E", 0), C1("B", 1), C1("C", 2), C1("D", 2), C1("A", 5))
    s2A.sorted.resetIndex shouldBe Series("A")(3, 4, 6, 8, null)
    s2B.sorted.resetIndex shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, null)
    s2C.sorted(Order.ascNullsFirst).resetIndex shouldBe Series("C")(null, null, "ABC", "ghi", "XyZ")
    s2D.sorted(Order.ascNullsFirst).resetIndex shouldBe Series("D")(
      null,
      C1("E", 0),
      C1("C", 2),
      C1("D", 2),
      C1("A", 5),
    )
    s2ASliced.sorted.resetIndex shouldBe Series("A")(3, 4, 6, 8)
    s2BSliced.sorted.resetIndex shouldBe Series("B")(1.4, 1.4, 3.1, 7.0)
    s2CSliced.sorted(Order.ascNullsFirst).resetIndex shouldBe Series("C")("ABC", "ghi", "XyZ")
    s2DSliced.sorted(Order.ascNullsFirst).resetIndex shouldBe Series("D")(
      C1("E", 0),
      C1("C", 2),
      C1("D", 2),
      C1("A", 5),
    )
  }

  test("show(n: Int, annotateIndex: Boolean, annotateType: Boolean, colWidth: Int): Unit") {
    s1A.show(0)
    s1B.show(5, annotateIndex = true)
    s2C.show(2, annotateType = true, annotateIndex = true, colWidth = 100)
    s2D.show()
    // see toString(...) for asserts
  }

  test("showValues(n: Int = 0): Unit") {
    s1A.showValues(-1)
    s1B.showValues(5)
    s2C.showValues(3)
    s1CSliced.showValues(0)
    s2CSliced.showValues(100)
    s2D.showValues()
  }

  test("sorted(implicit ordering: Ordering[T]): Series[T]") {
    s1A.sorted(asc).name shouldBe "A"

    s1A.sorted.as("A") shouldBe s1A(Seq(2, 1, 4, 0, 3))
    s1B.sorted.as("B") shouldBe s1B(Seq(1, 2, 4, 3, 0))
    s1C.sorted.as("C") shouldBe s1C(Seq(1, 0, 3, 4, 2))
    s1D.sorted.as("D") shouldBe s1D(Seq(4, 1, 2, 3, 0))
    s1ASliced.sorted.as("A") shouldBe s1ASliced(Seq(1, 4, 0, 3))
    s1BSliced.sorted.as("B") shouldBe s1BSliced(Seq(1, 2, 4, 3))
    s1CSliced.sorted.as("C") shouldBe s1CSliced(Seq(1, 0, 2))
    s1DSliced.sorted.as("D") shouldBe s1DSliced(Seq(4, 2, 3, 0))
    s2A.sorted.as("A") shouldBe s2A(Seq(1, 4, 0, 3, 2))
    s2B.sorted.as("B") shouldBe s2B(Seq(1, 2, 4, 3, 0))
    s2C.sorted.as("C") shouldBe s2C(Seq(1, 0, 2, 3, 4))
    s2D.sorted.as("D") shouldBe s2D(Seq(4, 2, 3, 0, 1))

  }

  test("sorted(order: Order)(implicit ordering: Ordering[T]): Series[T]") {
    s1A.sorted(asc).name shouldBe "A"

    s1A.sorted(asc).as("A") shouldBe s1A(Seq(2, 1, 4, 0, 3))
    s1B.sorted(asc).as("B") shouldBe s1B(Seq(1, 2, 4, 3, 0))
    s1C.sorted(asc).as("C") shouldBe s1C(Seq(1, 0, 3, 4, 2))
    s1D.sorted(asc).as("D") shouldBe s1D(Seq(4, 1, 2, 3, 0))
    s1ASliced.sorted(asc).as("A") shouldBe s1ASliced(Seq(1, 4, 0, 3))
    s1BSliced.sorted(asc).as("B") shouldBe s1BSliced(Seq(1, 2, 4, 3))
    s1CSliced.sorted(asc).as("C") shouldBe s1CSliced(Seq(1, 0, 2))
    s1DSliced.sorted(asc).as("D") shouldBe s1DSliced(Seq(4, 2, 3, 0))
    s2A.sorted(asc).as("A") shouldBe s2A(Seq(1, 4, 0, 3, 2))
    s2B.sorted(asc).as("B") shouldBe s2B(Seq(1, 2, 4, 3, 0))
    s2C.sorted(asc).as("C") shouldBe s2C(Seq(1, 0, 2, 3, 4))
    s2D.sorted(asc).as("D") shouldBe s2D(Seq(4, 2, 3, 0, 1))

    s1A.sorted(desc).as("A") shouldBe s1A(Seq(2, 1, 4, 0, 3).reverse)
    s1B.sorted(desc).as("B") shouldBe s1B(Seq(0, 3, 4, 1, 2))
    s1C.sorted(desc).as("C") shouldBe s1C(Seq(1, 0, 3, 4, 2).reverse)
    s1D.sorted(desc).as("D") shouldBe s1D(Seq(4, 1, 2, 3, 0).reverse)
    s1ASliced.sorted(desc).as("A") shouldBe s1ASliced(Seq(1, 4, 0, 3).reverse)
    s1BSliced.sorted(desc).as("B") shouldBe s1BSliced(Seq(3, 4, 1, 2))
    s1CSliced.sorted(desc).as("C") shouldBe s1CSliced(Seq(1, 0, 2).reverse)
    s1DSliced.sorted(desc).as("D") shouldBe s1DSliced(Seq(4, 2, 3, 0).reverse)
    s2A.sorted(desc).as("A") shouldBe s2A(Seq(1, 4, 0, 3).reverse :+ 2)
    s2B.sorted(desc).as("B") shouldBe s2B(Seq(3, 4, 1, 2, 0))
    s2C.sorted(desc).as("C") shouldBe s2C(Seq(2, 0, 1, 3, 4))
    s2D.sorted(desc).as("D") shouldBe s2D(Seq(4, 2, 3, 0).reverse :+ 1)

    s1A.sorted(ascNullsFirst).as("A") shouldBe s1A(Seq(2, 1, 4, 0, 3))
    s1B.sorted(ascNullsFirst).as("B") shouldBe s1B(Seq(1, 2, 4, 3, 0))
    s1C.sorted(ascNullsFirst).as("C") shouldBe s1C(Seq(1, 0, 3, 4, 2))
    s1D.sorted(ascNullsFirst).as("D") shouldBe s1D(Seq(4, 1, 2, 3, 0))
    s1ASliced.sorted(ascNullsFirst).as("A") shouldBe s1ASliced(Seq(1, 4, 0, 3))
    s1BSliced.sorted(ascNullsFirst).as("B") shouldBe s1BSliced(Seq(1, 2, 4, 3))
    s1CSliced.sorted(ascNullsFirst).as("C") shouldBe s1CSliced(Seq(1, 0, 2))
    s1DSliced.sorted(ascNullsFirst).as("D") shouldBe s1DSliced(Seq(4, 2, 3, 0))
    s2A.sorted(ascNullsFirst).as("A") shouldBe s2A(Seq(2, 1, 4, 0, 3))
    s2B.sorted(ascNullsFirst).as("B") shouldBe s2B(Seq(0, 1, 2, 4, 3))
    s2C.sorted(ascNullsFirst).as("C") shouldBe s2C(Seq(3, 4, 1, 0, 2))
    s2D.sorted(ascNullsFirst).as("D") shouldBe s2D(Seq(1, 4, 2, 3, 0))

    s1A.sorted(descNullsFirst).as("A") shouldBe s1A(Seq(2, 1, 4, 0, 3).reverse)
    s1B.sorted(descNullsFirst).as("B") shouldBe s1B(Seq(0, 3, 4, 1, 2))
    s1C.sorted(descNullsFirst).as("C") shouldBe s1C(Seq(1, 0, 3, 4, 2).reverse)
    s1D.sorted(descNullsFirst).as("D") shouldBe s1D(Seq(4, 1, 2, 3, 0).reverse)
    s1ASliced.sorted(descNullsFirst).as("A") shouldBe s1ASliced(Seq(1, 4, 0, 3).reverse)
    s1BSliced.sorted(descNullsFirst).as("B") shouldBe s1BSliced(Seq(3, 4, 1, 2))
    s1CSliced.sorted(descNullsFirst).as("C") shouldBe s1CSliced(Seq(1, 0, 2).reverse)
    s1DSliced.sorted(descNullsFirst).as("D") shouldBe s1DSliced(Seq(4, 2, 3, 0).reverse)
    s2A.sorted(descNullsFirst).as("A") shouldBe s2A(2 +: Seq(1, 4, 0, 3).reverse)
    s2B.sorted(descNullsFirst).as("B") shouldBe s2B(Seq(0, 3, 4, 1, 2))
    s2C.sorted(descNullsFirst).as("C") shouldBe s2C(Seq(3, 4, 2, 0, 1))
    s2D.sorted(descNullsFirst).as("D") shouldBe s2D(1 +: Seq(4, 2, 3, 0).reverse)
  }

  test("str: Series[String]") {
    s1A.str shouldBe Series("A")("6", "3", "2", "8", "4")
    s1B.str shouldBe Series("B")("23.1", "1.4", "1.4", "7.0", "3.1")
    s1C.str shouldBe s1C
    s1D.str shouldBe Series("D")("C1(A,5)", "C1(B,1)", "C1(C,2)", "C1(D,2)", "C1(E,0)")

    s1ASliced.str shouldBe Series("A")("6", "3", "2", "8", "4").apply(maskA)
    s1BSliced.str shouldBe Series("B")("23.1", "1.4", "1.4", "7.0", "3.1").apply(maskB)
    s1CSliced.str shouldBe s1CSliced
    s1DSliced.str shouldBe Series("D")("C1(A,5)", "C1(B,1)", "C1(C,2)", "C1(D,2)", "C1(E,0)").apply(maskD)

    s2A.str shouldBe Series("A")("6", "3", null, "8", "4")
    s2B.str shouldBe Series("B")(null, "1.4", "1.4", "7.0", "3.1")
    s2C.str shouldBe s2C
    s2D.str shouldBe Series("D")("C1(A,5)", null, "C1(C,2)", "C1(D,2)", "C1(E,0)")
  }

  test("toAny: Series[Any]") {
    s1A.toAny shouldBe s1A
    s1B.toAny shouldBe s1B
    s2C.toAny shouldBe s2C
    s2D.toAny shouldBe s2D
    s1ASliced.toAny shouldBe s1ASliced
    s1BSliced.toAny shouldBe s1BSliced
    s2CSliced.toAny shouldBe s2CSliced
    s2DSliced.toAny shouldBe s2DSliced
  }

  test("toArray: Array[Option[T]]") {
    s1A.toArray shouldBe Array(6, 3, 2, 8, 4).map(Some(_))
    s1B.toArray shouldBe Array(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1C.toArray shouldBe Array("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    s1D.toArray shouldBe Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s1ASliced.toArray shouldBe Array(6, 3, 8, 4).map(Some(_))
    s1BSliced.toArray shouldBe Array(1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1CSliced.toArray shouldBe Array("ghi", "ABC", "XyZ").map(Some(_))
    s1DSliced.toArray shouldBe Array(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s2A.toArray shouldBe Array(6, 3, null, 8, 4).map(Option(_))
    s2B.toArray shouldBe Array(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    s2C.toArray shouldBe Array("ghi", "ABC", "XyZ", null, null).map(Option(_))
    s2D.toArray shouldBe Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toFlatArray: Array[T]") {
    s1A.toFlatArray shouldBe Array(6, 3, 2, 8, 4)
    s1B.toFlatArray shouldBe Array(23.1, 1.4, 1.4, 7.0, 3.1)
    s1C.toFlatArray shouldBe Array("ghi", "ABC", "XyZ", "qqq", "Uuu")
    s1D.toFlatArray shouldBe Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    s1ASliced.toFlatArray shouldBe Array(6, 3, 8, 4)
    s1BSliced.toFlatArray shouldBe Array(1.4, 1.4, 7.0, 3.1)
    s1CSliced.toFlatArray shouldBe Array("ghi", "ABC", "XyZ")
    s1DSliced.toFlatArray shouldBe Array(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))

    s2A.toFlatArray shouldBe Array(6, 3, 8, 4)
    s2B.toFlatArray shouldBe Array(1.4, 1.4, 7.0, 3.1)
    s2C.toFlatArray shouldBe Array("ghi", "ABC", "XyZ")
    s2D.toFlatArray shouldBe Array(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toFlatList: List[T]") {
    s1A.toFlatList shouldBe List(6, 3, 2, 8, 4)
    s1B.toFlatList shouldBe List(23.1, 1.4, 1.4, 7.0, 3.1)
    s1C.toFlatList shouldBe List("ghi", "ABC", "XyZ", "qqq", "Uuu")
    s1D.toFlatList shouldBe List(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    s1ASliced.toFlatList shouldBe List(6, 3, 8, 4)
    s1BSliced.toFlatList shouldBe List(1.4, 1.4, 7.0, 3.1)
    s1CSliced.toFlatList shouldBe List("ghi", "ABC", "XyZ")
    s1DSliced.toFlatList shouldBe List(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))

    s2A.toFlatList shouldBe List(6, 3, 8, 4)
    s2B.toFlatList shouldBe List(1.4, 1.4, 7.0, 3.1)
    s2C.toFlatList shouldBe List("ghi", "ABC", "XyZ")
    s2D.toFlatList shouldBe List(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toFlatSeq: Seq[T]") {
    s1A.toFlatSeq shouldBe Seq(6, 3, 2, 8, 4)
    s1B.toFlatSeq shouldBe Seq(23.1, 1.4, 1.4, 7.0, 3.1)
    s1C.toFlatSeq shouldBe Seq("ghi", "ABC", "XyZ", "qqq", "Uuu")
    s1D.toFlatSeq shouldBe Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    s1ASliced.toFlatSeq shouldBe Seq(6, 3, 8, 4)
    s1BSliced.toFlatSeq shouldBe Seq(1.4, 1.4, 7.0, 3.1)
    s1CSliced.toFlatSeq shouldBe Seq("ghi", "ABC", "XyZ")
    s1DSliced.toFlatSeq shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))

    s2A.toFlatSeq shouldBe Seq(6, 3, 8, 4)
    s2B.toFlatSeq shouldBe Seq(1.4, 1.4, 7.0, 3.1)
    s2C.toFlatSeq shouldBe Seq("ghi", "ABC", "XyZ")
    s2D.toFlatSeq shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toList: List[Option[T]]") {
    s1A.toList shouldBe List(6, 3, 2, 8, 4).map(Some(_))
    s1B.toList shouldBe List(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1C.toList shouldBe List("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    s1D.toList shouldBe List(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s1ASliced.toList shouldBe List(6, 3, 8, 4).map(Some(_))
    s1BSliced.toList shouldBe List(1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1CSliced.toList shouldBe List("ghi", "ABC", "XyZ").map(Some(_))
    s1DSliced.toList shouldBe List(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s2A.toList shouldBe List(6, 3, null, 8, 4).map(Option(_))
    s2B.toList shouldBe List(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    s2C.toList shouldBe List("ghi", "ABC", "XyZ", null, null).map(Option(_))
    s2D.toList shouldBe List(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toSeq: Seq[Option[T]]") {
    s1A.toSeq shouldBe Seq(6, 3, 2, 8, 4).map(Some(_))
    s1B.toSeq shouldBe Seq(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1C.toSeq shouldBe Seq("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    s1D.toSeq shouldBe Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s1ASliced.toSeq shouldBe Seq(6, 3, 8, 4).map(Some(_))
    s1BSliced.toSeq shouldBe Seq(1.4, 1.4, 7.0, 3.1).map(Some(_))
    s1CSliced.toSeq shouldBe Seq("ghi", "ABC", "XyZ").map(Some(_))
    s1DSliced.toSeq shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    s2A.toSeq shouldBe Seq(6, 3, null, 8, 4).map(Option(_))
    s2B.toSeq shouldBe Seq(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    s2C.toSeq shouldBe Seq("ghi", "ABC", "XyZ", null, null).map(Option(_))
    s2D.toSeq shouldBe Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toString: String") {
    s1A.toString should include("A")
    s1A.toString should include("6")
    s1A.toString should include("4")
    s1A.toString should not include ("null")
    s2A.toString should include("null")
    s2ASliced.toString should not include ("null")
    s1B.toString should include("B")
    s1B.toString should include("23.1")
    s2B.toString should not include ("23.1")
    s2C.toString should include("C")
    s2C.toString should include("XyZ")
    s2C.toString should include("null")
    s2D.toString should include("C1(E,0)")
  }

  test("toString(...): String") {
    s1A.toString(10) should include("A")
    s1A.toString(10) should include("6")
    s1A.toString(10) should include("4")
    s1A.toString(10) should not include ("null")
    s2B.toString(10) should not include ("23.1")
    s2A.toString(10) should include("null")
    s2ASliced.toString(10) should not include ("null")
    s1B.toString(10) should include("B")
    s1B.toString(10) should include("23.1")
    s2B.toString(10) should not include ("23.1")
    s2C.toString(10) should include("C")
    s2C.toString(10) should include("XyZ")
    s2C.toString(10) should include("null")
    s2D.toString(10) should include("C1(E,0)")

    s1A.toString(0) should include("A")
    s1A.toString(0) should not include ("6")
    s1B.toString(5) should include("Double")
    s1B.toString(5, annotateType = true) should include("Double")
    s1B.toString(5, annotateIndex = true) should include("Index")
    s1B.toString(5, annotateType = false) should not include "Double"
    s1B.toString(5, annotateIndex = false) should not include "Index"
    s2C.toString(2, annotateType = true, annotateIndex = true, colWidth = 100) should include("String")
    s2D.toString() should include("C1(E,0)")
  }

  test("typeString: String") {
    s1A.typeString shouldBe "Int"
    s1A.toAny.typeString shouldBe "Int"
    s1B.typeString shouldBe "Double"
    s1C.typeString shouldBe "String"
    s1D.typeString shouldBe "C1"
    Series(Array(6, 3, 2, 8, 4), Array(2, 1, 2)).typeString shouldBe "Array[Int]"
    Series(List(6, 3, 2, 8, 4), List(2, 1, 2)).typeString shouldBe "List"
    Series(Seq(6, 3, 2, 8, 4), Seq(2, 1, 2)).typeString shouldBe "Seq"
    Series(null, 3, 4).typeString shouldBe "Int"
    Series(null, "a", "b").typeString shouldBe "String"
    Series(null, C1("A", 1)).typeString shouldBe "C1"
    Series(12, "13", 14.4).typeString shouldBe "Any"
    Series(null, "13", 14.4).typeString shouldBe "Any"
  }

  test("union[T2 <: T](series: Series[T2]*): Series[T]") {
    s1A.union(s2A, s2ASliced) shouldBe Series("A")(6, 3, 2, 8, 4, 6, 3, null, 8, 4, 6, 3, 8, 4)
    s1B.union(s2A.toDouble, s2BSliced) shouldBe
      Series("B")(23.1, 1.4, 1.4, 7.0, 3.1, 6.0, 3.0, null, 8.0, 4.0, 1.4, 1.4, 7.0, 3.1)
    s2CSliced.union(s2ASliced.str) shouldBe Series("C")("ghi", "ABC", "XyZ", "6", "3", "8", "4")
    s1D.union(s1DOfC2) shouldBe Series("D")(
      C1("A", 5),
      C1("B", 1),
      C1("C", 2),
      C1("D", 2),
      C1("E", 0),
      C2(),
      C2(),
      C2(),
      C2(),
      C2(),
    )
    assertThrows[SeriesCastException](s1A.toAny.union(s1B.toAny))
    assertThrows[SeriesCastException](s1DOfC2.toAny.union(s1D))
  }

  test("unique: Series[T]") {
    s1A.unique shouldBe s1A.toFlatArray
    s1B.unique shouldBe Array(23.1, 1.4, 7.0, 3.1)
    s1C.unique shouldBe s1C.toFlatArray
    s1D.unique shouldBe s1D.toFlatArray
    s2A.unique shouldBe s1ASliced.toFlatArray
    s2B.unique shouldBe Array(1.4, 7.0, 3.1)
    s2C.unique shouldBe s1CSliced.toFlatArray
    s2D.unique shouldBe s1DSliced.toFlatArray
    s2ASliced.unique shouldBe s1ASliced.toFlatArray
    s2BSliced.unique shouldBe Array(1.4, 7.0, 3.1)
    s2CSliced.unique shouldBe s1CSliced.toFlatArray
    s2DSliced.unique shouldBe s1DSliced.toFlatArray
  }

  test("update[T2 <: T](series: Series[T2]): Series[T]") {
    s1A.update(Series[Int](null, null, null, null, null)) shouldBe s1A
    s1B.update(Series[Double](null, null, null, null, null)) shouldBe s1B
    s1C.update(Series[String](null, null, null, null, null).apply(Seq(2, 3))) shouldBe s1C
    s1D.update(Series[C1](null, null, null, null, null).apply(Seq())) shouldBe s1D

    s1A.update(s1A + 1) shouldBe s1A + 1
    s1B.update(s1B + 1.0) shouldBe s1B + 1.0
    s1C.update(s1C + "A") shouldBe s1C + "A"
    s1D.update(s1D) shouldBe s1D

    s2A.update(s1A) shouldBe s1A
    s2B.update(s1B) shouldBe s1B
    s2C.update(s1C) shouldBe s1C
    s2D.update(s1D) shouldBe s1D

    s2ASliced.update(s1A) shouldBe s1ASliced
    s2BSliced.update(s1B) shouldBe s1BSliced
    s2CSliced.update(s1C) shouldBe s1CSliced
    s2DSliced.update(s1D) shouldBe s1DSliced

    s2A.sorted.update(s1A) shouldBe Series("A")(6, 3, 2, 8, 4).apply(Seq(1, 4, 0, 3, 2))
    s2B.sorted.update(s1B) shouldBe Series("B")(23.1, 1.4, 1.4, 7.0, 3.1).apply(Seq(1, 2, 4, 3, 0))

    s1A.update(Series(null, 7, 8, null, null)) shouldBe Series("A")(6, 7, 8, 8, 4)
    s1B.update(Series(1.0, null, 8.0, null, null)) shouldBe Series("B")(1.0, 1.4, 8.0, 7.0, 3.1)
    s1C.update(Series(null, null, "ABD", null, "uhu")) shouldBe Series("C")("ghi", "ABC", "ABD", "qqq", "uhu")
    s1D.update(Series(null, null, C1("Z", 9), null, null)) shouldBe
      Series("D")(C1("A", 5), C1("B", 1), C1("Z", 9), C1("D", 2), C1("E", 0))

    s2A.update(Series(null, 7, 8, null, null)) shouldBe Series("A")(6, 7, 8, 8, 4)
    s2B.update(Series(1.0, null, 8.0, null, null)) shouldBe Series("B")(1.0, 1.4, 8.0, 7.0, 3.1)
    s2C.update(Series(null, null, "ABD", null, "uhu")) shouldBe Series("C")("ghi", "ABC", "ABD", null, "uhu")
    s2D.update(Series(null, null, C1("Z", 9), null, null)) shouldBe
      Series("D")(C1("A", 5), null, C1("Z", 9), C1("D", 2), C1("E", 0))

    s2A.update(Series(0, 7, 8, 0, 0).apply(Seq(1, 2))) shouldBe Series("A")(6, 7, 8, 8, 4)
    s2B.update(Series(1.0, 0.0, 8.0, 0.0, 0.0).apply(Seq(2, 0))) shouldBe Series("B")(1.0, 1.4, 8.0, 7.0, 3.1)
    s2C.update(Series("ABC", "ABC", "ABD", "ABC", "uhu").apply(Seq(4, 2))) shouldBe
      Series("C")("ghi", "ABC", "ABD", null, "uhu")
    s2D.update(Series(C1("A", 0), C1("A", 0), C1("Z", 9), C1("A", 0), C1("A", 0)).apply(Seq(2))) shouldBe
      Series("D")(C1("A", 5), null, C1("Z", 9), C1("D", 2), C1("E", 0))

    s2A.toAny.update(s1A) shouldBe s1A
    s2B.toAny.update(s1B) shouldBe s1B
    s2C.toAny.update(s1C.toAny) shouldBe s1C
    s2D.toAny.update(s1DOfC2) shouldBe s1DOfC2

    assertThrows[SeriesCastException](s2A.toAny.update(s1B))
    assertThrows[SeriesCastException](s2B.toAny.update(s1C))
  }

  test("extract(indices: Array[Int]): Series[T]") {
    s1A.extract(Array(0, 1, 2, 3, 4)) shouldBe s1A
    s1B.extract(Array(1, 4, 3)) shouldBe Series("B")(1.4, 3.1, 7.0)
    s1C.extract(Array(3, 0, 0)) shouldBe Series("C")("qqq", "ghi", "ghi")
    s2A.extract(Array(2, 3, 2, 3, 2, 3, 2, 3)) shouldBe Series("A")(null, 8, null, 8, null, 8, null, 8)
    s2D.extract(Array(0)) shouldBe Series("D")(C1("A", 5))
    s2ASliced.extract(Array(1, 4, 3, 3)) shouldBe Series("A")(3, 4, 8, 8)
    s2BSliced.extract(Array()) shouldBe Series("B")[Double]()
  }

  test("isBoolean: Boolean") {
    s1A.isBoolean shouldBe false
    s1B.isBoolean shouldBe false
    s1C.isBoolean shouldBe false
    s1D.isBoolean shouldBe false
    Series(true, false, null).isBoolean shouldBe true
  }

  test("isDouble: Boolean") {
    s1A.isDouble shouldBe false
    s1B.isDouble shouldBe true
    s1C.isDouble shouldBe false
    s1D.isDouble shouldBe false
  }

  test("isInt: Boolean") {
    s1A.isInt shouldBe true
    s1B.isInt shouldBe false
    s1C.isInt shouldBe false
    s1D.isInt shouldBe false
  }

  test("isString: Boolean") {
    s1A.isString shouldBe false
    s1B.isString shouldBe false
    s1C.isString shouldBe true
    s1D.isString shouldBe false
  }

  test("ops: SeriesOps") {
    // tested indirectly
  }

  test("requireTypeMatch[T2](series: Series[T2]): Unit") {
    s1A.requireTypeMatch(s1A)
    assertThrows[SeriesCastException](s1A.requireTypeMatch(s1B))
    assertThrows[SeriesCastException](s1A.requireTypeMatch(s1C))
    assertThrows[SeriesCastException](s1A.requireTypeMatch(s1D))
    // see matchType for more tests
  }

  test("requireTypeMatch[T2](value: T2): Unit") {
    s1A.requireTypeMatch(1)
    assertThrows[ValueCastException](s1A.requireTypeMatch(2.0))
    assertThrows[ValueCastException](s1A.requireTypeMatch("abc"))
    assertThrows[ValueCastException](s1A.requireTypeMatch(C2()))
    // see matchType for more tests
  }

  test("resetIndexOrCopy: Series[T]") {
    s1A.sorted.resetIndexOrCopy shouldBe Series("A")(2, 3, 4, 6, 8)
    s1B.sorted.resetIndexOrCopy shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, 23.1)
    s1C.sorted.resetIndexOrCopy shouldBe Series("C")("ABC", "ghi", "qqq", "Uuu", "XyZ")
    s1D.sorted.resetIndexOrCopy shouldBe Series("D")(C1("E", 0), C1("B", 1), C1("C", 2), C1("D", 2), C1("A", 5))
    s2A.sorted.resetIndexOrCopy shouldBe Series("A")(3, 4, 6, 8, null)
    s2B.sorted.resetIndexOrCopy shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, null)
    s2C.sorted(Order.ascNullsFirst).resetIndexOrCopy shouldBe Series("C")(null, null, "ABC", "ghi", "XyZ")
    s2D.sorted(Order.ascNullsFirst).resetIndexOrCopy shouldBe Series("D")(
      null,
      C1("E", 0),
      C1("C", 2),
      C1("D", 2),
      C1("A", 5),
    )
    s2ASliced.sorted.resetIndexOrCopy shouldBe Series("A")(3, 4, 6, 8)
    s2BSliced.sorted.resetIndexOrCopy shouldBe Series("B")(1.4, 1.4, 3.1, 7.0)
    s2CSliced.sorted(Order.ascNullsFirst).resetIndexOrCopy shouldBe Series("C")("ABC", "ghi", "XyZ")
    s2DSliced.sorted(Order.ascNullsFirst).resetIndexOrCopy shouldBe Series("D")(
      C1("E", 0),
      C1("C", 2),
      C1("D", 2),
      C1("A", 5),
    )

  }

  test("seriesName: String") {
    s1A.seriesName shouldBe "Series A"
    Series(1, 2, 3).seriesName shouldBe "Series"
  }

  test("sort(order: Order)(implicit ordering: Ordering[T]): Series[T]") {
    // tested via sorted
  }

  test("typeEquals[T2](series: Series[T2]): Boolean") {
    s1A.typeEquals(s1A) shouldBe true
    s1A.typeEquals(s1B) shouldBe false
    s1A.typeEquals(s1C) shouldBe false
    s1A.typeEquals(s1D) shouldBe false
    s1B.typeEquals(s2A) shouldBe false
    s1B.typeEquals(s2B) shouldBe true
    s1B.typeEquals(s2C) shouldBe false
    s1B.typeEquals(s2D) shouldBe false
    s1C.typeEquals(s1ASliced) shouldBe false
    s1C.typeEquals(s1BSliced) shouldBe false
    s1C.typeEquals(s1CSliced) shouldBe true
    s1C.typeEquals(s1DSliced) shouldBe false
    s1D.typeEquals(s2ASliced) shouldBe false
    s1D.typeEquals(s2BSliced) shouldBe false
    s1D.typeEquals(s2CSliced) shouldBe false
    s1D.typeEquals(s2DSliced) shouldBe true

    s1A.toAny.typeEquals(s1A) shouldBe true
    s1A.toAny.typeEquals(s1B) shouldBe false
    s1A.toAny.typeEquals(s1C) shouldBe false
    s1A.toAny.typeEquals(s1D) shouldBe false
    s1B.typeEquals(s2A.toAny) shouldBe false
    s1B.typeEquals(s2B.toAny) shouldBe true
    s1B.typeEquals(s2C.toAny) shouldBe false
    s1B.typeEquals(s2D.toAny) shouldBe false
    s1C.toAny.typeEquals(s1ASliced.toAny) shouldBe false
    s1C.toAny.typeEquals(s1BSliced.toAny) shouldBe false
    s1C.toAny.typeEquals(s1CSliced.toAny) shouldBe true
    s1C.toAny.typeEquals(s1DSliced.toAny) shouldBe false

    val s1DC2: Series[C2] = Series("No Name")(C2(), C2(), C2(), C2(), C2())
    s1D.typeEquals(s1DC2) shouldBe false
    s1DC2.typeEquals(s1D) shouldBe false
    s1D.toAny.typeEquals(s1DC2) shouldBe false
    s1DC2.toAny.typeEquals(s1D) shouldBe false
    s1D.typeEquals(s1DC2.toAny) shouldBe false
    s1DC2.typeEquals(s1D.toAny) shouldBe false
    s1D.toAny.typeEquals(s1DC2.toAny) shouldBe false
    s1DC2.toAny.typeEquals(s1D.toAny) shouldBe false
  }

  test("typeMatch[T2](series: Series[T2]): Boolean") {
    s1A.typeMatch(s1A) shouldBe true
    s1A.typeMatch(s1B) shouldBe false
    s1A.typeMatch(s1C) shouldBe false
    s1A.typeMatch(s1D) shouldBe false
    s1B.typeMatch(s2A) shouldBe false
    s1B.typeMatch(s2B) shouldBe true
    s1B.typeMatch(s2C) shouldBe false
    s1B.typeMatch(s2D) shouldBe false
    s1C.typeMatch(s1ASliced) shouldBe false
    s1C.typeMatch(s1BSliced) shouldBe false
    s1C.typeMatch(s1CSliced) shouldBe true
    s1C.typeMatch(s1DSliced) shouldBe false
    s1D.typeMatch(s2ASliced) shouldBe false
    s1D.typeMatch(s2BSliced) shouldBe false
    s1D.typeMatch(s2CSliced) shouldBe false
    s1D.typeMatch(s2DSliced) shouldBe true

    s1A.toAny.typeMatch(s1A) shouldBe true
    s1A.toAny.typeMatch(s1B) shouldBe false
    s1A.toAny.typeMatch(s1C) shouldBe false
    s1A.toAny.typeMatch(s1D) shouldBe false
    s1B.typeMatch(s2A.toAny) shouldBe false
    s1B.typeMatch(s2B.toAny) shouldBe true
    s1B.typeMatch(s2C.toAny) shouldBe false
    s1B.typeMatch(s2D.toAny) shouldBe false
    s1C.toAny.typeMatch(s1ASliced.toAny) shouldBe false
    s1C.toAny.typeMatch(s1BSliced.toAny) shouldBe false
    s1C.toAny.typeMatch(s1CSliced.toAny) shouldBe true
    s1C.toAny.typeMatch(s1DSliced.toAny) shouldBe false

    val s1DC2: Series[C2] = Series("No Name")(C2(), C2(), C2(), C2(), C2())
    s1D.typeMatch(s1DC2) shouldBe true
    s1DC2.typeMatch(s1D) shouldBe false
    s1D.toAny.typeMatch(s1DC2) shouldBe true
    s1DC2.toAny.typeMatch(s1D) shouldBe false
    s1D.typeMatch(s1DC2.toAny) shouldBe true
    s1DC2.typeMatch(s1D.toAny) shouldBe false
    s1D.toAny.typeMatch(s1DC2.toAny) shouldBe true
    s1DC2.toAny.typeMatch(s1D.toAny) shouldBe false
  }

  test("typeMatch[T2](value: T2): Boolean") {
    s1A.typeMatch(1) shouldBe true
    s1A.typeMatch(2.0) shouldBe false
    s1A.typeMatch("name") shouldBe false
    s1A.typeMatch(C1("A", 1)) shouldBe false
    s1B.typeMatch(1) shouldBe false
    s1B.typeMatch(2.0) shouldBe true
    s1B.typeMatch("name") shouldBe false
    s1B.typeMatch(C1("A", 1)) shouldBe false
    s1C.typeMatch(1) shouldBe false
    s1C.typeMatch(2.0) shouldBe false
    s1C.typeMatch("name") shouldBe true
    s1C.typeMatch(C1("A", 1)) shouldBe false
    s1D.typeMatch(1) shouldBe false
    s1D.typeMatch(2.0) shouldBe false
    s1D.typeMatch("name") shouldBe false
    s1D.typeMatch(C1("A", 1)) shouldBe true

    s1A.toAny.typeMatch(1) shouldBe true
    s1A.toAny.typeMatch(2.0) shouldBe false
    s1A.toAny.typeMatch("name") shouldBe false
    s1A.toAny.typeMatch(C1("A", 1)) shouldBe false
    s1B.toAny.typeMatch(1) shouldBe false
    s1B.toAny.typeMatch(2.0) shouldBe true
    s1B.toAny.typeMatch("name") shouldBe false
    s1B.toAny.typeMatch(C1("A", 1)) shouldBe false
    s1C.toAny.typeMatch(1) shouldBe false
    s1C.toAny.typeMatch(2.0) shouldBe false
    s1C.toAny.typeMatch("name") shouldBe true
    s1C.toAny.typeMatch(C1("A", 1)) shouldBe false
    s1D.toAny.typeMatch(1) shouldBe false
    s1D.toAny.typeMatch(2.0) shouldBe false
    s1D.toAny.typeMatch("name") shouldBe false
    s1D.toAny.typeMatch(C1("A", 1)) shouldBe true

    val s1DC2: Series[C2] = Series("No Name")(C2(), C2(), C2(), C2(), C2())
    s1D.typeMatch(C2()) shouldBe true
    s1DC2.typeMatch(C1("A", 0)) shouldBe false
    s1D.toAny.typeMatch(C2()) shouldBe true
    s1DC2.toAny.typeMatch(C1("A", 0)) shouldBe false
    s1D.typeMatch(C2().asInstanceOf[Any]) shouldBe true
    s1DC2.typeMatch(C1("A", 0).asInstanceOf[Any]) shouldBe false
    s1D.toAny.typeMatch(C2().asInstanceOf[Any]) shouldBe true
    s1DC2.toAny.typeMatch(C1("A", 0).asInstanceOf[Any]) shouldBe false
  }

  test("undefinedIndices: SeqIndex") {
    s1A.undefinedIndices shouldBe Array[Int]()
    s1B.undefinedIndices shouldBe Array[Int]()
    s1C.undefinedIndices shouldBe Array[Int]()
    s1D.undefinedIndices shouldBe Array[Int]()
    s1ASliced.undefinedIndices shouldBe Array[Int]()
    s1BSliced.undefinedIndices shouldBe Array[Int]()
    s1CSliced.undefinedIndices shouldBe Array[Int]()
    s1DSliced.undefinedIndices shouldBe Array[Int]()
    s2A.undefinedIndices shouldBe Array[Int](2)
    s2B.undefinedIndices shouldBe Array[Int](0)
    s2C.undefinedIndices shouldBe Array[Int](3, 4)
    s2D.undefinedIndices shouldBe Array[Int](1)
    s2ASliced.undefinedIndices shouldBe Array[Int]()
    s2BSliced.undefinedIndices shouldBe Array[Int]()
    s2CSliced.undefinedIndices shouldBe Array[Int]()
    s2DSliced.undefinedIndices shouldBe Array[Int]()
  }

  test("withIndex(index: BaseIndex): Series[T]") {
    s1A.withIndex(SeqIndex(s1A.index.base, Seq(0, 1, 3, 4))) shouldBe s1ASliced
    s1B.withIndex(SlicedIndex(s1B.index.base, 1 to 4)) shouldBe s1BSliced
    s1C.withIndex(SlicedIndex(s1C.index.base, 0 to 2)) shouldBe s1CSliced
    s1D.withIndex(SeqIndex(s1D.index.base, Seq(0, 2, 3, 4))) shouldBe s1DSliced
  }

  // *** TRAIT INDEXOPS ***

  test("apply(range: Range): V") {
    // see pd.index.*IndexTest for more tests
    s1A(1 to 2).toFlatSeq shouldBe Seq(3, 2)
    s1A(1 to 2).apply(Seq(2, 5)).toFlatSeq shouldBe Seq(2)
    s1A(Seq(2, 4)).apply(1 until 4).toFlatSeq shouldBe Seq(2)
    s1A(1 to 4).apply(Seq(4, 2, 0)).toFlatSeq shouldBe Seq(4, 2)
    s1A(1 to 4).apply(Seq(0, 2, 4)).toFlatSeq shouldBe Seq(2, 4)
    s1A(Seq(0, 4, 2)).apply(1 to 4).toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](s1A(0 to 10))
  }

  test("apply(seq: Seq[Int]): V") {
    // see pd.index.*IndexTest for more tests
    s1A(Seq(1, 2)).toFlatSeq shouldBe Seq(3, 2)
    s1A(Seq(2, 4)).apply(1 to 2).toFlatSeq shouldBe Seq(2)
    s1A(Seq(2, 4)).apply(1 until 4).toFlatSeq shouldBe Seq(2)
    s1A(1 to 4).apply(Seq(4, 2, 0)).toFlatSeq shouldBe Seq(4, 2)
    s1A(1 to 4).apply(Seq(0, 2, 4)).toFlatSeq shouldBe Seq(2, 4)
    s1A(Seq(0, 4, 2)).apply(1 to 4).toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](s1A(Seq(2, 10)))
  }

  test("apply(array: Array[Int]): V") {
    // see pd.index.*IndexTest for more tests
    s1A(Array(1, 2)).toFlatSeq shouldBe Seq(3, 2)
    s1A(Array(2, 4)).apply(1 to 2).toFlatSeq shouldBe Seq(2)
    s1A(Array(2, 4)).apply(1 until 4).toFlatSeq shouldBe Seq(2)
    s1A(1 to 4).apply(Array(4, 2, 0)).toFlatSeq shouldBe Seq(4, 2)
    s1A(1 to 4).apply(Array(0, 2, 4)).toFlatSeq shouldBe Seq(2, 4)
    s1A(Array(0, 4, 2)).apply(1 to 4).toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](s1A(Array(2, 10)))
  }

  test("series: Series[Boolean]): V") {
    // see pd.index.*IndexTest for more tests
    s1A(maskA).toFlatSeq shouldBe Seq(6, 3, 8, 4)
    s1B(maskB).toFlatSeq shouldBe Seq(1.4, 1.4, 7.0, 3.1)
    s1C(maskC).toFlatSeq shouldBe Seq("ghi", "ABC", "XyZ")
    s1D(maskD).toFlatSeq shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
    s1A(0 to 3).apply(maskA).toFlatSeq shouldBe Seq(6, 3, 8)
    s1B(Seq(2, 3))(maskB).toFlatSeq shouldBe Seq(1.4, 7.0)
    s1C(maskC).apply(1 to 3).toFlatSeq shouldBe Seq("ABC", "XyZ")
    s1D(maskD).apply(Seq(1, 2)).toFlatSeq shouldBe Seq(C1("C", 2))
  }

  test("head(n: Int): V") {
    // see pd.index.*IndexTest for more tests
    s1A.head(2) shouldBe s1A(Seq(0, 1))
    s1B.head(5) shouldBe s1B(0 to 4)
    s1CSliced.head(4) shouldBe s1CSliced(0 to 3)
    s1D.head(0) shouldBe s1D(Seq())
    s1A.sorted.head(1) shouldBe s1A(Seq(2))
    s1A.sorted.head(2) shouldBe s1A(Seq(2, 1))
    s1A.sorted.head(5) shouldBe s1A.sorted
    s1A.sorted.head(10) shouldBe s1A.sorted
  }

  test("sortIndex: Series[T]") {
    s1A.sortIndex shouldBe s1A
    s1B.sortIndex shouldBe s1B
    s1C.sortIndex shouldBe s1C
    s1D.sortIndex shouldBe s1D

    s1ASliced.sortIndex shouldBe s1ASliced
    s1BSliced.sortIndex shouldBe s1BSliced
    s1CSliced.sortIndex shouldBe s1CSliced
    s1DSliced.sortIndex shouldBe s1DSliced

    s1A.sorted.sortIndex shouldBe s1A
    s1B.sorted(desc).sortIndex shouldBe s1B
    s1C.sorted.sortIndex shouldBe s1C
    s1D.sorted.sortIndex shouldBe s1D

    s1ASliced.sorted.sortIndex shouldBe s1ASliced
    s1BSliced.sorted(desc).sorted.sortIndex shouldBe s1BSliced
    s1CSliced.sorted(ascNullsFirst).sorted.sortIndex shouldBe s1CSliced
    s1DSliced.sorted.sortIndex shouldBe s1DSliced
  }

  test("tail(n: Int): V") {
    // see pd.index.*IndexTest for more tests
    s1A.tail(2) shouldBe s1A(Seq(3, 4))
    s1B.tail(5) shouldBe s1B(0 to 4)
    s1CSliced.tail(2) shouldBe s1CSliced(1 to 2)
    s1D.tail(0) shouldBe s1D(Seq())
    s1A.sorted.tail(1) shouldBe s1A(Seq(3))
    s1A.sorted.tail(2) shouldBe s1A(Seq(0, 3))
    s1A.sorted.tail(5) shouldBe s1A.sorted
    s1A.sorted.tail(10) shouldBe s1A.sorted
  }

  // *** OBJECT ***

  test("Series.apply[T: ClassTag](data: (T | Null)*): Series[T]") {
    Series(6, 3, 2, 8, 4) shouldBe s1A.as("")
    Series(23.1, 1.4, 1.4, 7.0, 3.1) shouldBe s1B.as("")
    Series("ghi", "ABC", "XyZ", "qqq", "Uuu") shouldBe s1C.as("")
    Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)) shouldBe s1D.as("")
    Series(6, 3, null, 8, 4) shouldBe s2A.as("")
    Series(null, 1.4, 1.4, 7.0, 3.1) shouldBe s2B.as("")
    Series("ghi", "ABC", "XyZ", null, null) shouldBe s2C.as("")
    Series(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)) shouldBe s2D.as("")
    val none = null
    Series(6, 3, none, 8, 4) shouldBe s2A.as("")
    Series(none, 1.4, 1.4, 7.0, 3.1) shouldBe s2B.as("")
    Series("ghi", "ABC", "XyZ", none, none) shouldBe s2C.as("")
    Series(C1("A", 5), none, C1("C", 2), C1("D", 2), C1("E", 0)) shouldBe s2D.as("")

    Series(6, 3).isType[Int] shouldBe true
    Series(6, 3).isType[Double] shouldBe false
    Series(6, 3).typeMatch(s1A) shouldBe true
    Series(6, 3).typeMatch(s1B) shouldBe false
    Series(1, 1.1).isType[Int] shouldBe false
    Series(1, 1.1).isType[Double] shouldBe true
    Series(1, 1.1).typeMatch(s1A) shouldBe false
    Series(1, 1.1).typeMatch(s1B) shouldBe true
    Series(1, "A").isType[Any] shouldBe true
    Series(1, "A").isType[Int] shouldBe false
    Series(1, "A").isType[String] shouldBe false
    Series(1, "A").typeMatch(s1A) shouldBe false
    Series(1, "A").typeMatch(s1B) shouldBe false

    Series(6, null, 3).isType[Int] shouldBe true
    Series(6, null, 3).isType[Double] shouldBe false
    Series(6, null, 3).typeMatch(s1A) shouldBe true
    Series(6, null, 3).typeMatch(s1B) shouldBe false
    Series(1, null, 1.1).isType[Int] shouldBe false
    Series(1, null, 1.1).isType[Double] shouldBe false
    Series(1, null, 1.1).typeMatch(s1A) shouldBe false
    Series(1, null, 1.1).typeMatch(s1B) shouldBe false
    Series(1, null, "A").isType[Any] shouldBe true
    Series(1, null, "A").isType[Int] shouldBe false
    Series(1, null, "A").isType[String] shouldBe false
    Series(1, null, "A").typeMatch(s1A) shouldBe false
    Series(1, null, "A").typeMatch(s1B) shouldBe false
  }

  test("Series.apply(name: String): SeriesBuilder") {
    // see SeriesBuilder.apply
  }

  test("Series.empty[T: ClassTag]: Series[T]") {
    Series.empty[Int].name shouldBe ""
    Series.empty[Int].length shouldBe 0
    Series.empty[Int].typeMatch(s1A) shouldBe true
    Series.empty[Int].typeMatch(s1B) shouldBe false
  }

  test("Series.empty[T: ClassTag](length: Int): Series[T]") {
    Series.empty[Int](4).name shouldBe ""
    Series.empty[Int](4).length shouldBe 4
    Series.empty[Int](0).length shouldBe 0
    Series.empty[Int](4).typeMatch(s1A) shouldBe true
    Series.empty[Int](5).typeMatch(s1B) shouldBe false
    Series.empty[Int](4) shouldBe Series[Int](null, null, null, null)
    Series.empty[Double](4) shouldBe Series[Double](null, null, null, null)
    Series.empty[String](4) shouldBe Series[String](null, null, null, null)
    assertThrows[IllegalIndex](Series.empty[String](-1))
  }

  test("Series.fill[T: ClassTag](length: Int)(value: T): Series[T] ") {
    Series.fill(4)(1) shouldBe Series(1, 1, 1, 1)
    Series.fill(5)(1.1) shouldBe Series(1.1, 1.1, 1.1, 1.1, 1.1)
    Series.fill(length = 3)(value = "abc") shouldBe Series("abc", "abc", "abc")
    Series.fill(0)(1) shouldBe Series.empty[Int]
    assertThrows[IllegalIndex](Series.fill(-1)(1))
  }

  test("Series.from[T: ClassTag](data: Array[T]): Series[T]") {
    val c1A = Array(6, 3, 2, 8, 4)
    val c1B = Array(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = Array("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = Array("ghi", "ABC", "XyZ", null, null)
    val c2D = Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series.from(c1A) shouldBe s1A.as("")
    Series.from(c1B) shouldBe s1B.as("")
    Series.from(c1C) shouldBe s1C.as("")
    Series.from(c1D) shouldBe s1D.as("")
    Series.from(c2C) shouldBe s2C.as("")
    Series.from(c2D) shouldBe s2D.as("")
  }

  test("Series.from[T: ClassTag](data: collection.mutable.Buffer[T]): Series[T]") {
    val c1A = collection.mutable.Buffer(6, 3, 2, 8, 4)
    val c1B = collection.mutable.Buffer(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = collection.mutable.Buffer("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = collection.mutable.Buffer(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = collection.mutable.Buffer("ghi", "ABC", "XyZ", null, null)
    val c2D = collection.mutable.Buffer(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series.from(c1A) shouldBe s1A.as("")
    Series.from(c1B) shouldBe s1B.as("")
    Series.from(c1C) shouldBe s1C.as("")
    Series.from(c1D) shouldBe s1D.as("")
    Series.from(c2C) shouldBe s2C.as("")
    Series.from(c2D) shouldBe s2D.as("")
  }

  test("Series.from[T: ClassTag](data: Seq[T]): Series[T]") {
    val c1A = Seq(6, 3, 2, 8, 4)
    val c1B = Seq(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = Seq("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = Seq("ghi", "ABC", "XyZ", null, null)
    val c2D = Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series.from(c1A) shouldBe s1A.as("")
    Series.from(c1B) shouldBe s1B.as("")
    Series.from(c1C) shouldBe s1C.as("")
    Series.from(c1D) shouldBe s1D.as("")
    Series.from(c2C) shouldBe s2C.as("")
    Series.from(c2D) shouldBe s2D.as("")
  }

  test("Series.from[T: ClassTag](data: Array[Option[T]]): Series[T]") {
    val c1A = Array(6, 3, 2, 8, 4).map(Option(_))
    val c1B = Array(23.1, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c1C = Array("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Option(_))
    val c1D = Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    val c2A = Array(6, 3, null, 8, 4).map(Option(_))
    val c2B = Array(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c2C = Array("ghi", "ABC", "XyZ", null, null).map(Option(_))
    val c2D = Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    Series.from(c1A) shouldBe s1A.as("")
    Series.from(c1B) shouldBe s1B.as("")
    Series.from(c1C) shouldBe s1C.as("")
    Series.from(c1D) shouldBe s1D.as("")
    Series.from(c2A) shouldBe s2A.map(_.asInstanceOf[Any]).as("")
    Series.from(c2B) shouldBe s2B.map(_.asInstanceOf[Any]).as("")
    Series.from(c2C) shouldBe s2C.as("")
    Series.from(c2D) shouldBe s2D.as("")
  }

  test("Series.from[T: ClassTag](data: Seq[Option[T]]): Series[T]") {
    val c1A = Seq(6, 3, 2, 8, 4).map(Option(_))
    val c1B = Seq(23.1, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c1C = Seq("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Option(_))
    val c1D = Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    val c2A = Seq(6, 3, null, 8, 4).map(Option(_))
    val c2B = Seq(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c2C = Seq("ghi", "ABC", "XyZ", null, null).map(Option(_))
    val c2D = Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    Series.from(c1A) shouldBe s1A.as("")
    Series.from(c1B) shouldBe s1B.as("")
    Series.from(c1C) shouldBe s1C.as("")
    Series.from(c1D) shouldBe s1D.as("")
    Series.from(c2A) shouldBe s2A.map(_.asInstanceOf[Any]).as("")
    Series.from(c2B) shouldBe s2B.map(_.asInstanceOf[Any]).as("")
    Series.from(c2C) shouldBe s2C.as("")
    Series.from(c2D) shouldBe s2D.as("")
  }

  test("Series.union[T](series: Series[T]*): Series[T]") {
    Series.union(s1A, s2A, s2ASliced) shouldBe Series("A")(6, 3, 2, 8, 4, 6, 3, null, 8, 4, 6, 3, 8, 4)
    Series.union(s1B, s2A.toDouble, s2BSliced) shouldBe
      Series("B")(23.1, 1.4, 1.4, 7.0, 3.1, 6.0, 3.0, null, 8.0, 4.0, 1.4, 1.4, 7.0, 3.1)
    Series.union(s2CSliced, s2ASliced.str) shouldBe Series("C")("ghi", "ABC", "XyZ", "6", "3", "8", "4")
    Series.union(s1D, s1DOfC2.as[C1]) shouldBe Series("D")(
      C1("A", 5),
      C1("B", 1),
      C1("C", 2),
      C1("D", 2),
      C1("E", 0),
      C2(),
      C2(),
      C2(),
      C2(),
      C2(),
    )
    s1A.union() shouldBe s1A
    assertThrows[SeriesCastException](s1A.toAny.union(s1B.toAny))
    assertThrows[SeriesCastException](s1DOfC2.toAny.union(s1D))
    assertThrows[IllegalOperation](Series.union())
  }

  // *** OBJECT PRIVATE ***

  test("Series.ext[T](s: Series[T], op: Char, s2: Series[?]): Series[T]") {
    Series.ext(s1A, '+', s1B) shouldBe s1A.as("A+B")
    Series.ext(s1A, '*', s1C) shouldBe s1A.as("A*C")
    Series.ext(s1A, '+', s1B.as("")) shouldBe s1A
    Series.ext(s1A, '+', Series.empty[Int]) shouldBe s1A
  }

  test("Series.wrap[T: ClassTag](array: Array[T]): MutableSeries[T]") {
    Series.wrap(Array(6, 3, 2, 8, 4)) shouldBe s1A.as("")
    Series.wrap(Array(6, 3, 2, 8, 4)) shouldBe s1A.as("")
    Series.wrap(Array(23.1, 1.4, 1.4, 7.0, 3.1)) shouldBe s1B.as("")
    Series.wrap(Array("ghi", "ABC", "XyZ", "qqq", "Uuu")) shouldBe s1C.as("")
    Series.wrap(Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s1D.as("")

    Series.wrap(Array(6, 3, null, 8, 4)).equals(s2A.as("")) shouldBe false
    Series.wrap(Array(null, 1.4, 1.4, 7.0, 3.1)).equals(s2B.as("")) shouldBe false
    Series.wrap(Array("ghi", "ABC", "XyZ", null, null)) shouldBe s2C.as("")
    Series.wrap(Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s2D.as("")

    Series.wrap(Array("ghi", "ABC", "XyZ", null, null)).map(_ + "_") shouldBe
      Series("ghi_", "ABC_", "XyZ_", null, null)

    val array = Array(6, 3, 2, 8, 4)
    val s = Series.wrap(array)
    s shouldBe s1A.as("")
    array(1) = -10
    s shouldBe Series(6, -10, 2, 8, 4)
  }

  test("Series.MutableSeriesBuilder.wrap[T: ClassTag](array: Array[T]): MutableSeries[T]") {
    Series.mutable.wrap(Array(6, 3, 2, 8, 4)) shouldBe s1A.as("")
    Series("A").mutable.wrap(Array(6, 3, 2, 8, 4)) shouldBe s1A
    Series("B").mutable.wrap(Array(23.1, 1.4, 1.4, 7.0, 3.1)) shouldBe s1B
    Series("C").mutable.wrap(Array("ghi", "ABC", "XyZ", "qqq", "Uuu")) shouldBe s1C
    Series("D").mutable.wrap(Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s1D

    Series("A").mutable.wrap(Array(6, 3, null, 8, 4)).equals(s2A) shouldBe false
    Series("B").mutable.wrap(Array(null, 1.4, 1.4, 7.0, 3.1)).equals(s2B) shouldBe false
    Series("C").mutable.wrap(Array("ghi", "ABC", "XyZ", null, null)) shouldBe s2C
    Series("D").mutable.wrap(Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s2D

    Series("C").mutable.wrap(Array("ghi", "ABC", "XyZ", null, null)).map(_ + "_") shouldBe
      Series("C")("ghi_", "ABC_", "XyZ_", null, null)

    val array = Array(6, 3, 2, 8, 4)
    val s = Series("A").mutable.wrap(array)
    s shouldBe s1A
    array(1) = -10
    s shouldBe Series("A")(6, -10, 2, 8, 4)
  }

  test("Series.SeriesBuilder.apply[T: ClassTag](data: Seq[T]): Series[T]") {
    s1A.toFlatArray shouldBe Array(6, 3, 2, 8, 4)
    s1B.toFlatArray shouldBe Array(23.1, 1.4, 1.4, 7.0, 3.1)
    s1C.toFlatArray shouldBe Array("ghi", "ABC", "XyZ", "qqq", "Uuu")
    s1D.toFlatArray shouldBe Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    s2A.toArray shouldBe Array(Some(6), Some(3), None, Some(8), Some(4))
    s2B.toArray shouldBe Array(None, Some(1.4), Some(1.4), Some(7.0), Some(3.1))
    s2C.toArray shouldBe Array(Some("ghi"), Some("ABC"), Some("XyZ"), None, None)
    s2D.toArray shouldBe Array(Some(C1("A", 5)), None, Some(C1("C", 2)), Some(C1("D", 2)), Some(C1("E", 0)))

    s1A.name shouldBe "A"
    s1B.name shouldBe "B"
    s1C.name shouldBe "C"
    s1D.name shouldBe "D"
    Series("Very long name 1234")(1, 2, 3).name shouldBe "Very long name 1234"

    Series("X")(6, 3).isType[Int] shouldBe true
    Series("X")(6, 3).isType[Double] shouldBe false
    Series("X")(6, 3).typeMatch(s1A) shouldBe true
    Series("X")(6, 3).typeMatch(s1B) shouldBe false
    Series("X")(1, 1.1).isType[Int] shouldBe false
    Series("X")(1, 1.1).isType[Double] shouldBe true
    Series("X")(1, 1.1).typeMatch(s1A) shouldBe false
    Series("X")(1, 1.1).typeMatch(s1B) shouldBe true
    Series("X")(1, "A").isType[Any] shouldBe true
    Series("X")(1, "A").isType[Int] shouldBe false
    Series("X")(1, "A").isType[String] shouldBe false
    Series("X")(1, "A").typeMatch(s1A) shouldBe false
    Series("X")(1, "A").typeMatch(s1B) shouldBe false

    Series("X")(6, null, 3).isType[Int] shouldBe true
    Series("X")(6, null, 3).isType[Double] shouldBe false
    Series("X")(6, null, 3).typeMatch(s1A) shouldBe true
    Series("X")(6, null, 3).typeMatch(s1B) shouldBe false
    Series("X")(1, null, 1.1).isType[Int] shouldBe false
    Series("X")(1, null, 1.1).isType[Double] shouldBe false
    Series("X")(1, null, 1.1).typeMatch(s1A) shouldBe false
    Series("X")(1, null, 1.1).typeMatch(s1B) shouldBe false
    Series("X")(1, null, "A").isType[Any] shouldBe true
    Series("X")(1, null, "A").isType[Int] shouldBe false
    Series("X")(1, null, "A").isType[String] shouldBe false
    Series("X")(1, null, "A").typeMatch(s1A) shouldBe false
    Series("X")(1, null, "A").typeMatch(s1B) shouldBe false
  }

  test("Series.SeriesBuilder.fill[T: ClassTag](length: Int)(value: T): Series[T] ") {
    Series("X").fill(4)(1) shouldBe Series("X")(1, 1, 1, 1)
    Series("X").fill(5)(1.1) shouldBe Series("X")(1.1, 1.1, 1.1, 1.1, 1.1)
    Series("X").fill(length = 3)(value = "abc") shouldBe Series("X")("abc", "abc", "abc")
    Series("X").fill(0)(1) shouldBe Series.empty[Int].as("X")
    assertThrows[IllegalIndex](Series("X").fill(-1)(1))
  }

  test("Series.SeriesBuilder.from[T: ClassTag](data: Array[T]): Series[T]") {
    val c1A = Array(6, 3, 2, 8, 4)
    val c1B = Array(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = Array("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = Array("ghi", "ABC", "XyZ", null, null)
    val c2D = Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series("A").from(c1A) shouldBe s1A
    Series("B").from(c1B) shouldBe s1B
    Series("C").from(c1C) shouldBe s1C
    Series("D").from(c1D) shouldBe s1D
    Series("C").from(c2C) shouldBe s2C
    Series("D").from(c2D) shouldBe s2D

  }

  test("Series.SeriesBuilder.from[T: ClassTag](data: collection.mutable.Buffer[T]): Series[T]") {
    val c1A = collection.mutable.Buffer(6, 3, 2, 8, 4)
    val c1B = collection.mutable.Buffer(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = collection.mutable.Buffer("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = collection.mutable.Buffer(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = collection.mutable.Buffer("ghi", "ABC", "XyZ", null, null)
    val c2D = collection.mutable.Buffer(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series("A").from(c1A) shouldBe s1A
    Series("B").from(c1B) shouldBe s1B
    Series("C").from(c1C) shouldBe s1C
    Series("D").from(c1D) shouldBe s1D
    Series("C").from(c2C) shouldBe s2C
    Series("D").from(c2D) shouldBe s2D
  }

  test("Series.SeriesBuilder.from[T: ClassTag](data: Seq[T]): Series[T]") {
    val c1A = Seq(6, 3, 2, 8, 4)
    val c1B = Seq(23.1, 1.4, 1.4, 7.0, 3.1)
    val c1C = Seq("ghi", "ABC", "XyZ", "qqq", "Uuu")
    val c1D = Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    val c2C = Seq("ghi", "ABC", "XyZ", null, null)
    val c2D = Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
    Series("A").from(c1A) shouldBe s1A
    Series("B").from(c1B) shouldBe s1B
    Series("C").from(c1C) shouldBe s1C
    Series("D").from(c1D) shouldBe s1D
    Series("C").from(c2C) shouldBe s2C
    Series("D").from(c2D) shouldBe s2D
  }
  test("Series.SeriesBuilder.from[T: ClassTag](data: Array[Option[T]]): Series[T]") {
    val c1A = Array(6, 3, 2, 8, 4).map(Option(_))
    val c1B = Array(23.1, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c1C = Array("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Option(_))
    val c1D = Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    val c2A = Array(6, 3, null, 8, 4).map(Option(_))
    val c2B = Array(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c2C = Array("ghi", "ABC", "XyZ", null, null).map(Option(_))
    val c2D = Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    Series("A").from(c1A) shouldBe s1A
    Series("B").from(c1B) shouldBe s1B
    Series("C").from(c1C) shouldBe s1C
    Series("D").from(c1D) shouldBe s1D
    Series("A").from(c2A) shouldBe s2A.map(_.asInstanceOf[Any])
    Series("B").from(c2B) shouldBe s2B.map(_.asInstanceOf[Any])
    Series("C").from(c2C) shouldBe s2C
    Series("D").from(c2D) shouldBe s2D
  }

  test("Series.SeriesBuilder.from[T: ClassTag](data: Seq[Option[T]]): Series[T]") {
    val c1A = Seq(6, 3, 2, 8, 4).map(Option(_))
    val c1B = Seq(23.1, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c1C = Seq("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Option(_))
    val c1D = Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    val c2A = Seq(6, 3, null, 8, 4).map(Option(_))
    val c2B = Seq(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    val c2C = Seq("ghi", "ABC", "XyZ", null, null).map(Option(_))
    val c2D = Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
    Series("A").from(c1A) shouldBe s1A
    Series("B").from(c1B) shouldBe s1B
    Series("C").from(c1C) shouldBe s1C
    Series("D").from(c1D) shouldBe s1D
    Series("A").from(c2A) shouldBe s2A.map(_.asInstanceOf[Any])
    Series("B").from(c2B) shouldBe s2B.map(_.asInstanceOf[Any])
    Series("C").from(c2C) shouldBe s2C
    Series("D").from(c2D) shouldBe s2D
  }

  test("Series.SeriesBuilder.wrap[T: ClassTag](array: Array[T]): MutableSeries[T]") {
    Series("A").wrap(Array(6, 3, 2, 8, 4)) shouldBe s1A
    Series("B").wrap(Array(23.1, 1.4, 1.4, 7.0, 3.1)) shouldBe s1B
    Series("C").wrap(Array("ghi", "ABC", "XyZ", "qqq", "Uuu")) shouldBe s1C
    Series("D").wrap(Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s1D

    Series("A").wrap(Array(6, 3, null, 8, 4)).equals(s2A) shouldBe false
    Series("B").wrap(Array(null, 1.4, 1.4, 7.0, 3.1)).equals(s2B) shouldBe false
    Series("C").wrap(Array("ghi", "ABC", "XyZ", null, null)) shouldBe s2C
    Series("D").wrap(Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))) shouldBe s2D

    Series("C").wrap(Array("ghi", "ABC", "XyZ", null, null)).map(_ + "_") shouldBe
      Series("C")("ghi_", "ABC_", "XyZ_", null, null)

    val array = Array(6, 3, 2, 8, 4)
    val s = Series("A").wrap(array)
    s shouldBe s1A
    array(1) = -10
    s shouldBe Series("A")(6, -10, 2, 8, 4)
  }
