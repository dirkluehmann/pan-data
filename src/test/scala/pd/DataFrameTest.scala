/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.*
import pd.internal.index.{BaseIndex, ColIndex, UniformIndex}
import pd.internal.index.sort.Sorting
import pd.internal.series.SeriesData
import pd.internal.utils.RequireType
import pd.internal.utils.StringUtils.{fixedString, pretty}
import pd.io.ReadAdapter
import pd.plot.Plot

import java.time.LocalDate
import scala.annotation.targetName
import scala.collection.immutable.Seq
import scala.reflect.ClassTag

/**
  * @since 0.1.0
  */
class DataFrameTest extends BaseTest:

  test("|[T](series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    df | s1A shouldBe df
    df | s1D shouldBe df1
    df | s1BVar shouldBe DataFrame(s1A, s1BVar, s1C)
    df1 | s1A(0 to 2) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A") | s1B(0 to 2) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1 | s1A(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1 | s1A(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df | s1D.sorted shouldBe df1
    df | s1ASub shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df | s1BSub shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df | s1DSliced shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2) | s1A)
    assertThrows[MergeIndexException](df(1 to 2) | s2C)
  }

  test("|[T](namedSeries: (String, Series[T])): DataFrame ") {
    val df = df1(Seq("A", "B", "C"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df | "A" -> s1AX shouldBe df
    df | "D" -> s1DX shouldBe df1
    df | "X" -> s2A shouldBe DataFrame(s1A, s1B, s1C, s2A as "X")
    df | "B" -> s1BVar shouldBe DataFrame(s1A, s1BVar, s1C)
    df1 | "A" -> s1AX(0 to 2) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A") | "B" -> s1B(0 to 2) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1 | "A" -> s1AX(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1 | "A" -> s1AX(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df | "D" -> s1DX.sorted shouldBe df1
    df | "A" -> s1ASub shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df | "B" -> s1BSub shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df | "X" -> s1DSliced shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced as "X")
    assertThrows[MergeIndexException](df(1 to 2) | "A" -> s1AX)
    assertThrows[MergeIndexException](df(1 to 2) | "C" -> s2C)
  }

  test("|(df: DataFrame): DataFrame") {
    val dfA = df1(Seq("A", "B"))
    val dfB = df1(Seq("C", "D"))
    dfA | dfB shouldBe df1
    dfB | dfA shouldBe df1(Seq("C", "D", "A", "B"))
    DataFrame(s1AVar, s1BVar) | dfA shouldBe dfA
    DataFrame(s1AVar, s1BVar) | df1 shouldBe df1
    df1 | DataFrame(s1AVar, s1BVar) shouldBe DataFrame(s1AVar, s1BVar, s1C, s1D)
    dfA | dfB(0 to 2) shouldBe DataFrame.from(s1A, s1B, s1C(0 to 2), s1D(0 to 2))
    dfA.sortValues("A") | dfB shouldBe DataFrame.from(s1A, s1B, s1C, s1D).sortValues("A")
    df1 | DataFrame(s1A(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    dfB | s1A(Seq(3, 0, 1)) shouldBe DataFrame.from(s1C, s1D, s1A(Seq(0, 1, 3)))
    df1 | DataFrame(s1D.sorted) shouldBe df1
    df1 | DataFrame(s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C, s1D)
    df1 | DataFrame(s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C, s1D)
    df1 | DataFrame(s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](dfA(1 to 2) | dfA)
    assertThrows[MergeIndexException](dfA(1 to 2) | dfB)
  }

  test("::[T](series: Series[T]): DataFrame") {
    val df = df1(Seq("B", "C", "D"))
    (s1A :: df) shouldBe df1
    (s1B :: df) shouldBe df
    (s1BVar :: df) shouldBe DataFrame.from(s1B, s1C, s1D)
    (s1A(0 to 2) :: df) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    (s1ASorted :: df) shouldBe df1
    (s1B(Seq(3, 0, 1)) :: df) shouldBe DataFrame.from(s1B, s1C, s1D)
    (s1A :: df.sortValues("B")) shouldBe df1.sortValues("B")
    assertThrows[MergeIndexException](s1A :: df(0 to 2))
    assertThrows[MergeIndexException](s1BSub :: df(0 to 2))
  }

  test("::(df: DataFrame): DataFrame") {
    val dfA = df1(Seq("A", "B"))
    val dfB = df1(Seq("C", "D"))
    dfA :: dfB shouldBe df1
    dfB :: dfA shouldBe df1(Seq("C", "D", "A", "B"))
    DataFrame(s1AVar, s1BVar) :: dfA shouldBe dfA
    DataFrame(s1AVar, s1BVar) :: df1 shouldBe df1
    df1 :: DataFrame(s1AVar, s1BVar) shouldBe DataFrame(s1AVar, s1BVar, s1C, s1D)
    dfA(0 to 2) :: dfB shouldBe DataFrame.from(s1A(0 to 2), s1B(0 to 2), s1C, s1D)
    dfA.sortValues("A") :: dfB shouldBe DataFrame.from(s1A, s1B, s1C, s1D)
    dfA :: dfB.sortValues("C") shouldBe DataFrame.from(s1A, s1B, s1C, s1D).sortValues("C")
    DataFrame(s1A(Seq(3, 0, 1))) :: dfB shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1C, s1D)
    DataFrame(s1A(Seq(3, 0, 1))) :: dfA shouldBe DataFrame.from(s1A, s1B)
    DataFrame(s1D.sorted) :: df1 shouldBe df1(Seq("D", "A", "B", "C"))
    DataFrame(s1ASub) :: df1 shouldBe df1
    DataFrame(s1DSliced) :: dfB shouldBe DataFrame(s1D, s1C)
    assertThrows[MergeIndexException](dfA :: dfA(1 to 2))
    assertThrows[MergeIndexException](dfA :: dfB(1 to 2))
  }

  test("&[T](col: String, series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df & "A" -> s1AX shouldBe df
    df & "D" -> s1DX shouldBe df1
    df & "B" -> s1BVar shouldBe DataFrame(s1A, s1BVar, s1C)
    df & "A" -> s1AX(0 to 2) shouldBe df
    df & "A" -> s1ASliced shouldBe df
    df2 & "A" -> s1AX & "B" -> s1B & "C" -> s1C & "D" -> s1DX shouldBe df1
    df1 & "A" -> s2ASliced & "B" -> s2BSliced & "C" -> s1CSliced & "D" -> s1DSliced shouldBe df1
    df1 & "A" -> Series("A")(1, null, 3, null, null) shouldBe DataFrame(Series("A")(1, 3, 3, 8, 4), s1B, s1C, s1D)
    df2 & "C" -> Series("C")("1", null, null, "4", null) shouldBe
      DataFrame(s2A, s2B, Series("C")("1", "ABC", "XyZ", "4", null), s2D)
    df3 & "A" -> Series(null, null, null, -2, -3).apply(Seq(0, 3, 4)) shouldBe
      df3 | Series("A")(6, null, 2, -2, -3).apply(Seq(0, 2, 3, 4))
    df.sortValues("A") & "B" -> s1B(0 to 2) shouldBe df.sortValues("A")
    df & "A" -> s1AX(Seq(3, 0, 1)) shouldBe df
    df & "D" -> s1DX(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A, s1B, s1C, s1D(Seq(0, 1, 3)))
    df & "D" -> s1DX.sorted shouldBe df1
    df & "A" -> s1ASub shouldBe df
    df & "D" -> s1DSliced shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2) & "A" -> s1AX)
    assertThrows[MergeIndexException](df(1 to 2) & "D" -> s1DX)
    assertThrows[MergeIndexException](df.apply(Seq(0, 2, 3, 4)) & "D" -> s1DX)
    assertThrows[MergeIndexException](df3 & "A" -> s1AX)
    assertThrows[SeriesCastException](df & "A" -> s1B)
  }

  test("&[T](series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    df & s1A shouldBe df
    df & s1D shouldBe df1
    df & s1BVar shouldBe DataFrame(s1A, s1BVar, s1C)
    df & s1A(0 to 2) shouldBe df
    df & s1ASliced shouldBe df
    df2 & s1A & s1B & s1C & s1D shouldBe df1
    df1 & s2ASliced & s2BSliced & s1CSliced & s1DSliced shouldBe df1
    df1 & Series("A")(1, null, 3, null, null) shouldBe DataFrame(Series("A")(1, 3, 3, 8, 4), s1B, s1C, s1D)
    df2 & Series("C")("1", null, null, "4", null) shouldBe
      DataFrame(s2A, s2B, Series("C")("1", "ABC", "XyZ", "4", null), s2D)
    df3 & Series("A")(null, null, null, -2, -3).apply(Seq(0, 3, 4)) shouldBe
      df3 | Series("A")(6, null, 2, -2, -3).apply(Seq(0, 2, 3, 4))
    df.sortValues("A") & s1B(0 to 2) shouldBe df.sortValues("A")
    df & s1A(Seq(3, 0, 1)) shouldBe df
    df & s1D(Seq(3, 0, 1)) shouldBe DataFrame.from(s1A, s1B, s1C, s1D(Seq(0, 1, 3)))
    df & s1D.sorted shouldBe df1
    df & s1ASub shouldBe df
    df & s1DSliced shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2) & s1A)
    assertThrows[MergeIndexException](df(1 to 2) & s1D)
    assertThrows[MergeIndexException](df.apply(Seq(0, 2, 3, 4)) & s1D)
    assertThrows[MergeIndexException](df3 & s1A)
    assertThrows[SeriesCastException](df & s1B.as("A"))
  }

  test("append[T](name: String, series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1BX = s1B as "X"
    val s1CX = s1C as "X"
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df.append("A", s1AX) shouldBe df
    df.append("A", s1AVar) shouldBe df
    df.append("B", s1BX) shouldBe df
    df.append("B", s1BVar) shouldBe df
    df.append("C", s1CX) shouldBe df
    df.append("A", s1CX) shouldBe df
    df.append("D", s1DX) shouldBe df1
    df.append("D", s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).append("D", s1AX))
    assertThrows[MergeIndexException](df(1 to 2).append("D", s2C))
  }

  test("append(series: Series[?]*): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    df.append(s1A) shouldBe df
    df.append(s1AVar) shouldBe df
    df.append(s1B) shouldBe df
    df.append(s1BVar) shouldBe df
    df.append(s1C) shouldBe df
    df.append(s1A, s1B, s1C) shouldBe df
    df.append(s1C as "A") shouldBe df
    df.append(s1D) shouldBe df1
    df.append(s1BVar, s1D) shouldBe df1
    df1(Seq("A", "B")).append(s1C, s1D) shouldBe df1
    df.append(s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).append(s1D))
    assertThrows[MergeIndexException](df(1 to 2).append(s2DSliced))
  }

  test("apply(col: String): Series[Any]") {
    df1("A") shouldBe s1A
    df1("B") shouldBe s1B
    df1("C") shouldBe s1C
    df1("D") shouldBe s1D
    df2("A") shouldBe s2A
    df2("B") shouldBe s2B
    df2("C") shouldBe s2C
    df2("D") shouldBe s2D
    df3("A") shouldBe s3A
    df3("B") shouldBe s3B
    df3("C") shouldBe s3C
    df3("D") shouldBe s3D
    assertThrows[ColumnNotFoundException](df1("X"))
    assertThrows[ColumnNotFoundException](df3("A "))
  }

  test("apply[T: RequireType : Typeable : ClassTag](row: Int, col: String): Option[T]") {
    df1[Int](2, "A") shouldBe Some(2)
    df1[Double](3, "B") shouldBe Some(7.0)
    df1[String](4, "C") shouldBe Some("Uuu")
    df1[C1](1, "D") shouldBe Some(C1("B", 1))
    df2[Int](2, "A") shouldBe None
    df2[Double](3, "B") shouldBe Some(7.0)
    df2[String](4, "C") shouldBe None
    df2[C1](1, "D") shouldBe None
    df3[Int](2, "A") shouldBe Some(2)
    df3[Double](3, "B") shouldBe None
    df3[String](4, "C") shouldBe Some("Uuu")
    df3[C1](1, "D") shouldBe None

    df1(Seq(1, 4)).apply[Int](2, "A") shouldBe None
    df1[Any](1, "C") shouldBe Some("ABC")
    df2[Any](3, "B") shouldBe Some(7.0)

    assertThrows[IndexBoundsException](df1[Int](-1, "A"))
    assertThrows[IndexBoundsException](df1[Int](5, "A"))
    assertThrows[ColumnNotFoundException](df1[Int](1, "X"))
    assertThrows[ColumnNotFoundException](df1[Int](5, "B "))
    assertThrows[SeriesCastException](df1[String](0, "A"))
    assertThrows[SeriesCastException](df1[Double](0, "A"))
    assertThrows[SeriesCastException](df1[String](0, "B"))
    assertThrows[SeriesCastException](df1[Int](0, "C"))
    assertThrows[SeriesCastException](df1[C2](0, "D"))
    assertThrows[SeriesCastException](df3[String](1, "A"))
  }

  test("apply[T: RequireType : Typeable : ClassTag](row: Option[Int], col: String): Option[T]") {
    df1[Int](None, "A") shouldBe None
    df2[Double](None, "B") shouldBe None
    df3[String](None, "C") shouldBe None
    df1[C1](None, "D") shouldBe None

    df1[Int](Some(2), "A") shouldBe Some(2)
    df1[Double](Some(3), "B") shouldBe Some(7.0)
    df1[String](Some(4), "C") shouldBe Some("Uuu")
    df1[C1](Some(1), "D") shouldBe Some(C1("B", 1))
    df2[Int](Some(2), "A") shouldBe None
    df2[Double](Some(3), "B") shouldBe Some(7.0)
    df2[String](Some(4), "C") shouldBe None
    df2[C1](Some(1), "D") shouldBe None
    df3[Int](Some(2), "A") shouldBe Some(2)
    df3[Double](Some(3), "B") shouldBe None
    df3[String](Some(4), "C") shouldBe Some("Uuu")
    df3[C1](Some(1), "D") shouldBe None

    df1(Seq(1, 4)).apply[Int](Some(2), "A") shouldBe None
    df1[Any](Some(1), "C") shouldBe Some("ABC")
    df2[Any](Some(3), "B") shouldBe Some(7.0)

    assertThrows[IndexBoundsException](df1[Int](Some(-1), "A"))
    assertThrows[IndexBoundsException](df1[Int](Some(5), "A"))
    assertThrows[ColumnNotFoundException](df1[Int](Some(1), "X"))
    assertThrows[ColumnNotFoundException](df1[Int](Some(5), "B "))
    assertThrows[SeriesCastException](df1[String](Some(0), "A"))
    assertThrows[SeriesCastException](df1[Double](Some(0), "A"))
    assertThrows[SeriesCastException](df1[String](Some(0), "B"))
    assertThrows[SeriesCastException](df1[Int](Some(0), "C"))
    assertThrows[SeriesCastException](df1[C2](Some(0), "D"))
    assertThrows[SeriesCastException](df3[String](Some(1), "A"))
    assertThrows[SeriesCastException](df3[String](None, "A"))
  }

  test("apply[T: Typeable : ClassTag](row: Int, col: String, default: => T): T") {
    df1[Int](2, "A", 5) shouldBe 2
    df1[Double](3, "B", 5) shouldBe 7.0
    df1[String](4, "C", "none") shouldBe "Uuu"
    df1[C1](1, "D", C2()) shouldBe C1("B", 1)
    df2(2, "A", 5) shouldBe 5
    df2(3, "B", 5.0) shouldBe 7.0
    df2(4, "C", "none") shouldBe "none"
    df2(1, "D", C1("X", 0)) shouldBe C1("X", 0)
    df3(2, "A", 5) shouldBe 2
    df3(3, "B", 5.0) shouldBe 5.0
    df3(4, "C", "none") shouldBe "Uuu"
    df3[C1](1, "D", C2()) shouldBe C2()

    df1(Seq(1, 4)).apply(2, "A", 5) shouldBe 5
    df1[Any](1, "C", "none") shouldBe "ABC"
    df2[Any](3, "B", 5.0) shouldBe 7.0
    assertThrows[IndexBoundsException](df1(-1, "A", 5))
    assertThrows[IndexBoundsException](df1[Int](5, "A", 5))
    assertThrows[ColumnNotFoundException](df1(1, "X", 1))
    assertThrows[ColumnNotFoundException](df1(5, "B ", 5.0))
    assertThrows[SeriesCastException](df1[String](0, "A", "none"))
    assertThrows[SeriesCastException](df1(0, "A", "none"))
    assertThrows[SeriesCastException](df1(0, "A", 5.0))
    assertThrows[SeriesCastException](df1(0, "B", "none"))
    assertThrows[SeriesCastException](df1(0, "C", 5))
    assertThrows[SeriesCastException](df1(0, "D", C2()))
    assertThrows[SeriesCastException](df3(1, "A", "none"))
    assertThrows[RuntimeException](df3[Int](1, "A", throw RuntimeException()))
  }

  test("apply[T: Typeable : ClassTag](row: Option[Int], col: String, default: => T): T") {
    df1(None, "A", 5) shouldBe 5
    df2(None, "B", 5.0) shouldBe 5.0
    df3(None, "C", "none") shouldBe "none"
    df1[C1](None, "D", C2()) shouldBe C2()

    df1[Int](Some(2), "A", 5) shouldBe 2
    df1[Double](Some(3), "B", 5.0) shouldBe 7.0
    df1[String](Some(4), "C", "none") shouldBe "Uuu"
    df1[C1](Some(1), "D", C2()) shouldBe C1("B", 1)
    df2(Some(2), "A", 5) shouldBe 5
    df2(Some(3), "B", 5.0) shouldBe 7.0
    df2(Some(4), "C", "none") shouldBe "none"
    df2(Some(1), "D", C1("X", 0)) shouldBe C1("X", 0)
    df3(Some(2), "A", 5) shouldBe 2
    df3(Some(3), "B", 5.0) shouldBe 5.0
    df3(Some(4), "C", "none") shouldBe "Uuu"
    df3[C1](Some(1), "D", C2()) shouldBe C2()

    df1(Seq(1, 4)).apply(Some(2), "A", 5) shouldBe 5
    df1[Any](Some(1), "C", "none") shouldBe "ABC"
    df2[Any](Some(3), "B", 5.0) shouldBe 7.0

    assertThrows[IndexBoundsException](df1[Int](Some(-1), "A", 5))
    assertThrows[IndexBoundsException](df1(Some(5), "A", 5))
    assertThrows[ColumnNotFoundException](df1(Some(1), "X", 5))
    assertThrows[ColumnNotFoundException](df1(Some(5), "B ", 5))
    assertThrows[SeriesCastException](df1[String](Some(0), "A", "none"))
    assertThrows[SeriesCastException](df1(Some(0), "A", "none"))
    assertThrows[SeriesCastException](df1(Some(0), "A", 5.0))
    assertThrows[SeriesCastException](df1(Some(0), "B", "none"))
    assertThrows[SeriesCastException](df1(Some(0), "C", 5))
    assertThrows[SeriesCastException](df1(Some(0), "D", C2()))
    assertThrows[SeriesCastException](df3(Some(1), "A", "none"))
    assertThrows[SeriesCastException](df3(None, "A", "none"))
    assertThrows[RuntimeException](df3[Int](1, "A", throw RuntimeException()))
  }

  test("apply(range: Range, col: String): Series[Any]") {
    df1(0 to 4, "A") shouldBe s1A
    df1(0 to 2, "A") shouldBe s1A(0 to 2)
    df1(1 to 2, "B") shouldBe s1B(1 to 2)
    df1(2 to 3, "C") shouldBe s1C(2 to 3)
    df1(4 to 4, "D") shouldBe s1D(4 to 4)
    df2(0 to 2, "A") shouldBe s2A(0 to 2)
    df2(1 to 2, "B") shouldBe s2B(1 to 2)
    df2(2 to 3, "C") shouldBe s2C(2 to 3)
    df2(4 to 4, "D") shouldBe s2D(4 to 4)
    df3(0 to 2, "A") shouldBe s3A(Seq(0, 2))
    df3(1 to 2, "B") shouldBe s3B(Seq(2))
    df3(2 to 3, "C") shouldBe s3C(Seq(2, 3))
    df3(4 to 4, "D") shouldBe s3D(Seq(4))
    assertThrows[IllegalIndex](df1(-1 to 2, "A"))
    assertThrows[IllegalIndex](df1(0 to 6, "A"))
    assertThrows[ColumnNotFoundException](df1(1 to 2, "X"))
    assertThrows[ColumnNotFoundException](df1(0 to 6, "X"))
  }

  test("apply(seq: Seq[Int], col: String): Series[Any]") {
    df1(Seq(0, 1, 2, 3, 4), "A") shouldBe s1A
    df1(Seq(0, 1, 2), "A") shouldBe s1A(0 to 2)
    df1(Seq(1, 3), "B") shouldBe s1B(Seq(1, 3))
    df1(Seq(2, 3), "C") shouldBe s1C(2 to 3)
    df1(Seq(4), "D") shouldBe s1D(4 to 4)
    df2(Seq(0, 1, 2), "A") shouldBe s2A(0 to 2)
    df2(Seq(1, 3), "B") shouldBe s2B(Seq(1, 3))
    df2(Seq(2, 3), "C") shouldBe s2C(2 to 3)
    df2(Seq(4), "D") shouldBe s2D(4 to 4)
    df3(Seq(0, 1, 2), "A") shouldBe s3A(Seq(0, 2))
    df3(Seq(1, 3), "B") shouldBe s3B(Seq(3))
    df3(Seq(2, 3), "C") shouldBe s3C(Seq(2, 3))
    df3(Seq(4), "D") shouldBe s3D(Seq(4))
    assertThrows[IllegalIndex](df1(Seq(-1, 0, 1), "A"))
    assertThrows[IllegalIndex](df1(Seq(5, 6), "A"))
    assertThrows[ColumnNotFoundException](df1(Seq(1, 2), "X"))
    assertThrows[ColumnNotFoundException](df1(Seq(5, 6), "X"))
  }

  test("apply(array: Array[Int], col: String): Series[Any]") {
    df1(Array(0, 1, 2, 3, 4), "A") shouldBe s1A
    df1(Array(0, 1, 2), "A") shouldBe s1A(0 to 2)
    df1(Array(1, 3), "B") shouldBe s1B(Seq(1, 3))
    df1(Array(2, 3), "C") shouldBe s1C(2 to 3)
    df1(Array(4), "D") shouldBe s1D(4 to 4)
    df2(Array(0, 1, 2), "A") shouldBe s2A(0 to 2)
    df2(Array(1, 3), "B") shouldBe s2B(Seq(1, 3))
    df2(Array(2, 3), "C") shouldBe s2C(2 to 3)
    df2(Array(4), "D") shouldBe s2D(4 to 4)
    df3(Array(0, 1, 2), "A") shouldBe s3A(Seq(0, 2))
    df3(Array(1, 3), "B") shouldBe s3B(Seq(3))
    df3(Array(2, 3), "C") shouldBe s3C(Seq(2, 3))
    df3(Array(4), "D") shouldBe s3D(Seq(4))
    assertThrows[IllegalIndex](df1(Array(-1, 0, 1), "A"))
    assertThrows[IllegalIndex](df1(Array(5, 6), "A"))
    assertThrows[ColumnNotFoundException](df1(Array(1, 2), "X"))
    assertThrows[ColumnNotFoundException](df1(Array(5, 6), "X"))
  }

  test("apply(series: Series[Boolean], col: String): Series[Any]") {
    df1(maskA, "A") shouldBe s1ASliced
    df1(maskB, "B") shouldBe s1BSliced
    df1(maskC, "C") shouldBe s1CSliced
    df1(maskD, "D") shouldBe s1DSliced
    df2(maskA, "A") shouldBe s2ASliced
    df2(maskB, "B") shouldBe s2BSliced
    df2(maskC, "C") shouldBe s2CSliced
    df2(maskD, "D") shouldBe s2DSliced
    val m = df1("B").asDouble < 4.0
    df3(m, "A") shouldBe s3A(Seq(2, 4))
    df3(m, "B") shouldBe s3B(Seq(2, 4))
    df3(m, "C") shouldBe s3C(Seq(2, 4))
    df3(m, "D") shouldBe s3D(Seq(2, 4))
    assertThrows[BaseIndexException](df1(Series(true, false, false), "A"))
    assertThrows[BaseIndexException](df1(Series.fill(10)(false), "A"))
    assertThrows[ColumnNotFoundException](df1(m, "X"))
    assertThrows[ColumnNotFoundException](df1(Series(true, false, false), "X"))
  }

  test("apply(cols: Seq[String]): DataFrame") {
    df1(Seq("A", "B", "C", "D")) shouldBe df1
    df1(Seq("B")) shouldBe DataFrame(s1B)
    df1(Seq("A", "C")) shouldBe DataFrame(s1A, s1C)
    df1(Seq("A", "B")) shouldBe DataFrame(s1A, s1B)
    df1(Seq("C", "D")) shouldBe DataFrame(s1C, s1D)
    df2(Seq("A", "B", "C", "D")) shouldBe df2
    df2(Seq("B")) shouldBe DataFrame(s2B)
    df2(Seq("A", "C")) shouldBe DataFrame(s2A, s2C)
    df2(Seq("A", "B")) shouldBe DataFrame(s2A, s2B)
    df2(Seq("C", "D")) shouldBe DataFrame(s2C, s2D)
    df3(Seq("A", "B", "C", "D")) shouldBe df3
    df3(Seq("B")) shouldBe DataFrame(s3B)
    df3(Seq("A", "C")) shouldBe DataFrame(s3A, s3C)
    df3(Seq("A", "B")) shouldBe DataFrame(s3A, s3B)
    df3(Seq("C", "D")) shouldBe DataFrame(s3C, s3D)
    assertThrows[ColumnNotFoundException](df1(Seq("X")))
    assertThrows[ColumnNotFoundException](df1(Seq("A", "B", "X", "D")))
  }

  test("col[T](series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    df.col(s1A) shouldBe df
    df.col(s1D) shouldBe df1
    df.col(s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df1.col(s1A(0 to 2)) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A").col(s1B(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1.col(s1A(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1.col(s1A(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df.col(s1D.sorted) shouldBe df1
    df.col(s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df.col(s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df.col(s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).col(s1A))
    assertThrows[MergeIndexException](df(1 to 2).col(s2C))
  }

  test("col[T](col: String, series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df.col("A", s1AX) shouldBe df
    df.col("D", s1DX) shouldBe df1
    df.col("X", s2A) shouldBe DataFrame(s1A, s1B, s1C, s2A as "X")
    df.col("B", s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df1.col("A", s1AX(0 to 2)) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A").col("B", s1B(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1.col("A", s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1.col("A", s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df.col("D", s1DX.sorted) shouldBe df1
    df.col("A", s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df.col("B", s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df.col("X", s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced as "X")
    assertThrows[MergeIndexException](df(1 to 2).col("A", s1AX))
    assertThrows[MergeIndexException](df(1 to 2).col("C", s2C))
  }

  test("columnArray: Array[Series[Any]]") {
    df1.columnArray shouldBe Array(s1A, s1B, s1C, s1D)
    df1(Seq("B", "A", "D")).columnArray shouldBe Array(s1B, s1A, s1D)
    df2(Seq()).columnArray shouldBe Array[Series[Any]]()
    (df3 | s3B).columnArray shouldBe Array(s3A, s3B, s3C, s3D)
    (df3 | "X" -> s3B).columnArray shouldBe Array(s3A, s3B, s3C, s3D, s3B as "X")
  }

  test("columnIterator: Iterable[Series[Any]]") {
    df1.columnIterator.toSeq shouldBe Seq(s1A, s1B, s1C, s1D)
    df1(Seq("B", "A", "D")).columnIterator.toSeq shouldBe Seq(s1B, s1A, s1D)
    df2(Seq()).columnIterator.toSeq shouldBe Seq[Series[Any]]()
    (df3 | s3B).columnIterator.toSeq shouldBe Seq(s3A, s3B, s3C, s3D)
    (df3 | "X" -> s3B).columnIterator.toSeq shouldBe Seq(s3A, s3B, s3C, s3D, s3B as "X")
  }

  test("columns: Seq[String]") {
    df1.columns shouldBe Seq("A", "B", "C", "D")
    df1(Seq("B", "A", "D")).columns shouldBe Seq("B", "A", "D")
    df2(Seq()).columns shouldBe Seq[String]()
    (df3 | s3B).columns shouldBe Seq("A", "B", "C", "D")
    (df3 | "X" -> s3B).columns shouldBe Seq("A", "B", "C", "D", "X")
  }

  test("cols: DataFrameAppender") {
    // see DataFrameAppender
  }

  test("contains(col: String): Boolean") {
    df1 contains "A" shouldBe true
    df1 contains "B" shouldBe true
    df2.contains("C") shouldBe true
    df3.contains("D") shouldBe true
    df1.contains("X") shouldBe false
    df1.contains("A ") shouldBe false
    df1.contains("a") shouldBe false
  }

  test("canEqual(a: Any): Boolean") {
    df1.canEqual(df1) shouldBe true
    df2.canEqual(df3) shouldBe true
    df1.canEqual("abc") shouldBe false
    df2.canEqual(3) shouldBe false
  }

  test("display(n: Int, width: Int, colWidth: Int): Unit") {
    // see toString
  }

  test("dropUndefined: DataFrame") {
    df1.dropUndefined() shouldBe df1
    df2.dropUndefined() shouldBe df2(Seq[Int]())
    df3.dropUndefined shouldBe df3(Seq(0, 4))
    DataFrame.empty.dropUndefined shouldBe DataFrame.empty
  }

  test("dropUndefined(cols: String*): DataFrame") {
    df1.dropUndefined("A", "B", "C", "D") shouldBe df1
    df2.dropUndefined("A", "B", "C", "D") shouldBe df2(Seq[Int]())
    df3.dropUndefined("A", "B", "C", "D") shouldBe df3(Seq(0, 4))
    df1.dropUndefined("A", "C", "D") shouldBe df1
    df2.dropUndefined("A", "C", "D") shouldBe df2(Seq[Int](0))
    df3.dropUndefined("A", "C", "D") shouldBe df3(Seq(0, 3, 4))
    df1.dropUndefined("C") shouldBe df1
    df2.dropUndefined("C") shouldBe df2(0 to 2)
    df3.dropUndefined("C") shouldBe df3(Seq(0, 3, 4))
    assertThrows[ColumnNotFoundException](df1.dropUndefined("X"))
    assertThrows[ColumnNotFoundException](df1.dropUndefined("A", "X"))
  }

  test("equals(df: Any): Boolean") {
    df1.equals(df1) shouldBe true
    df2.equals(df2) shouldBe true
    df3.equals(df3) shouldBe true
    df1.equals(DataFrame(s1A, s1B, s1C, s1D)) shouldBe true
    df1.equals(DataFrame(s1B, s1A, s1D, s1C)) shouldBe true
    df2.equals(DataFrame(s2A, s2B, s2C, s2D)) shouldBe true
    df2.equals(DataFrame(s2B, s2A, s2C, s2D)) shouldBe true
    df3.equals(DataFrame(s3A, s3B, s3C, s3D)) shouldBe true
    df3.equals(DataFrame(s3B, s3A, s3C, s3D)) shouldBe true

    df1.equals(DataFrame(s1A, s1B, s1C)) shouldBe false
    df1.equals(DataFrame(s1A, s2B, s1C, s1D)) shouldBe false
    df2.equals(DataFrame(s2A, s2B, s1C, s1D)) shouldBe false
    df1.equals(DataFrame(s1A, s1B, s3C, s1D)) shouldBe false
    df1.equals(df2) shouldBe false
    df2.equals(df1) shouldBe false
    df1.equals(df1(0 to 3)) shouldBe false
    df1(0 to 3).equals(df1) shouldBe false
    df2.equals(df2.sortValues("A")) shouldBe false
    df2.sortValues("A").equals(df2) shouldBe false
    df1.sortValues("A").equals(df1.sortValues("B")) shouldBe false
  }

  test("get[T: RequireType : Typeable : ClassTag](row: Int, col: String): T") {
    df1.get[Int](2, "A") shouldBe 2
    df1.get[Double](3, "B") shouldBe 7.0
    df1.get[String](4, "C") shouldBe "Uuu"
    df1.get[C1](1, "D") shouldBe C1("B", 1)
    assertThrows[NoSuchElementException](df2.get[Int](2, "A"))
    df2.get[Double](3, "B") shouldBe 7.0
    assertThrows[NoSuchElementException](df2.get[String](4, "C"))
    assertThrows[NoSuchElementException](df2.get[C1](1, "D"))
    df3.get[Int](2, "A") shouldBe 2
    assertThrows[NoSuchElementException](df3.get[Double](3, "B"))
    df3.get[String](4, "C") shouldBe "Uuu"
    assertThrows[NoSuchElementException](df3.get[C1](1, "D"))

    assertThrows[NoSuchElementException](df1(Seq(1, 4)).get[Int](2, "A"))
    df1.get[Any](1, "C") shouldBe "ABC"
    df2.get[Any](3, "B") shouldBe 7.0

    assertThrows[NoSuchElementException](df1.get[Int](-1, "A"))
    assertThrows[NoSuchElementException](df1.get[Int](5, "A"))
    assertThrows[ColumnNotFoundException](df1.get[Int](1, "X"))
    assertThrows[ColumnNotFoundException](df1.get[Int](5, "B "))
    assertThrows[SeriesCastException](df1.get[String](0, "A"))
    assertThrows[SeriesCastException](df1.get[Double](0, "A"))
    assertThrows[SeriesCastException](df1.get[String](0, "B"))
    assertThrows[SeriesCastException](df1.get[Int](0, "C"))
    assertThrows[SeriesCastException](df1.get[C2](0, "D"))
    assertThrows[SeriesCastException](df3.get[String](1, "A"))
  }

  test("get[T: RequireType : Typeable : ClassTag](row: Option[Int], col: String): T") {
    assertThrows[NoSuchElementException](df1.get[Int](None, "A"))
    assertThrows[NoSuchElementException](df1.get[Double](None, "B"))

    df1.get[Int](Some(2), "A") shouldBe 2
    df1.get[Double](Some(3), "B") shouldBe 7.0
    df1.get[String](Some(4), "C") shouldBe "Uuu"
    df1.get[C1](Some(1), "D") shouldBe C1("B", 1)
    assertThrows[NoSuchElementException](df2.get[Int](Some(2), "A"))
    df2.get[Double](Some(3), "B") shouldBe 7.0
    assertThrows[NoSuchElementException](df2.get[String](Some(4), "C"))
    assertThrows[NoSuchElementException](df2.get[C1](Some(1), "D"))
    df3.get[Int](Some(2), "A") shouldBe 2
    assertThrows[NoSuchElementException](df3.get[Double](Some(3), "B"))
    df3.get[String](Some(4), "C") shouldBe "Uuu"
    assertThrows[NoSuchElementException](df3.get[C1](Some(1), "D"))

    assertThrows[NoSuchElementException](df1(Seq(1, 4)).get[Int](Some(2), "A"))
    df1.get[Any](Some(1), "C") shouldBe "ABC"
    df2.get[Any](Some(3), "B") shouldBe 7.0

    assertThrows[NoSuchElementException](df1.get[Int](Some(-1), "A"))
    assertThrows[NoSuchElementException](df1.get[Int](Some(5), "A"))
    assertThrows[ColumnNotFoundException](df1.get[Int](Some(1), "X"))
    assertThrows[ColumnNotFoundException](df1.get[Int](Some(5), "B "))
    assertThrows[SeriesCastException](df1.get[String](Some(0), "A"))
    assertThrows[SeriesCastException](df1.get[Double](Some(0), "A"))
    assertThrows[SeriesCastException](df1.get[String](Some(0), "B"))
    assertThrows[SeriesCastException](df1.get[Int](Some(0), "C"))
    assertThrows[SeriesCastException](df1.get[C2](Some(0), "D"))
    assertThrows[SeriesCastException](df3.get[String](Some(1), "A"))
  }

//  val gfDouble = DataFrame(
//    "A" -> Series(1.0, Double.NaN, null, 1.0, 2.1, Double.PositiveInfinity, Double.NegativeInfinity, 2.1,
//      Double.NaN, Double.NaN, Double.PositiveInfinity, 2.1),
//    "B" -> Series("a", "a", "a", null, "b", "b", "b", "b", "c", "c", "c", "c"),
//    "E" -> Series(0, null, 1, 2, null, 3, 4, 5, 6, 7, 8, 9),
//  )

  test("groupBy(col: String): DataMap.Groups[Any]") {
    gf1.groupBy("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(A = Seq("a", "b"), count = Seq(3, 4))
    gf1.groupBy("B").countRows().sortValues("B").resetIndex shouldBe DataFrame(B = Seq(1, 2, 3), count = Seq(3, 3, 1))
    gf1.groupBy("C").countRows().sortValues("C").resetIndex shouldBe DataFrame(C = Seq(false, true), count = Seq(2, 6))
    gfDouble.groupBy("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Seq(Double.NegativeInfinity, 1.0, 2.1, Double.PositiveInfinity, Double.NaN, Double.NaN, Double.NaN),
      count = Seq(1, 2, 3, 2, 1, 1, 1),
    )
    assertThrows[ColumnNotFoundException](gf1.groupBy("X"))
  }

  test("groupBy(col1: String, col2: String): DataMap.Groups[(Any, Any)]") {
    gf1.groupBy("A", "B").countRows().sortValues("A", "B").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b", "b", "b"), B = Seq(1, 2, 1, 2, 3), count = Seq(1, 1, 1, 2, 1))
    gf1.groupBy("B", "C").countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Seq(1, 1, 2, 3), C = Seq(false, true, true, true), count = Seq(1, 2, 3, 1))
    gf1.groupBy("A", "C").countRows().sortValues("A", "C").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b"), C = Seq(false, true, true), count = Seq(1, 2, 4))
    assertThrows[ColumnNotFoundException](gf1.groupBy("A", "X"))
  }

  test("groupBy(col1: String, col2: String, col3: String): DataMap.Groups[(Any, Any, Any)]") {
    val result = gf1.groupBy("A", "B", "C").countRows().sortValues("A", "B", "C").resetIndex
    result shouldBe
      DataFrame(
        A = Seq("a", "a", "b", "b", "b"),
        B = Seq(1, 2, 1, 2, 3),
        C = Seq(true, true, true, true, true),
        count = Seq(1, 1, 1, 2, 1),
      )
    gf1.groupBy("B", "C", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    gf1.groupBy("C", "B", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    assertThrows[ColumnNotFoundException](gf1.groupBy("A", "X", "C"))
  }

  test("groupBy(cols: Seq[String]): DataMap.Groups[Seq[Any]]") {
    gf1.groupBy(Seq("A")).countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Seq("a", "b"),
      count = Seq(3, 4),
    )
    gf1.groupBy(Seq("B", "C")).countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Seq(1, 1, 2, 3), C = Seq(false, true, true, true), count = Seq(1, 2, 3, 1))
    gf1.groupBy(Seq("A", "B", "C")).countRows().sortValues("A", "B", "C").resetIndex shouldBe
      DataFrame(
        A = Seq("a", "a", "b", "b", "b"),
        B = Seq(1, 2, 1, 2, 3),
        C = Seq(true, true, true, true, true),
        count = Seq(1, 1, 1, 2, 1),
      )
    assertThrows[ColumnNotFoundException](gf1.groupBy(Seq("A", "X")))
  }

  test("groupByCol[T](col: String): DataMap.Groups[T]") {
    gf1.groupByCol[String]("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Seq("a", "b"),
      count = Seq(3, 4),
    )
    gf1.groupByCol[Int]("B").countRows().sortValues("B").resetIndex shouldBe DataFrame(
      B = Seq(1, 2, 3),
      count = Seq(3, 3, 1),
    )
    gf1.groupByCol[Boolean]("C").countRows().sortValues("C").resetIndex shouldBe DataFrame(
      C = Seq(false, true),
      count = Seq(2, 6),
    )
    gfDouble.groupByCol[Double]("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Seq(Double.NegativeInfinity, 1.0, 2.1, Double.PositiveInfinity, Double.NaN, Double.NaN, Double.NaN),
      count = Seq(1, 2, 3, 2, 1, 1, 1),
    )
    assertThrows[ColumnNotFoundException](gf1.groupByCol[String]("X"))
    assertThrows[SeriesCastException](gf1.groupByCol[Int]("A"))
  }

  test("groupByCol[T1, T2](col1: String, col2: String): DataMap.Groups[(T1, T2)]") {
    gf1.groupByCol[String, Int]("A", "B").countRows().sortValues("A", "B").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b", "b", "b"), B = Seq(1, 2, 1, 2, 3), count = Seq(1, 1, 1, 2, 1))
    gf1.groupByCol[Int, Boolean]("B", "C").countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Seq(1, 1, 2, 3), C = Seq(false, true, true, true), count = Seq(1, 2, 3, 1))
    gf1.groupByCol[String, Boolean]("A", "C").countRows().sortValues("A", "C").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b"), C = Seq(false, true, true), count = Seq(1, 2, 4))
    assertThrows[ColumnNotFoundException](gf1.groupByCol[String, String]("A", "X"))
    assertThrows[SeriesCastException](gf1.groupByCol[Int, Int]("A", "B"))
  }

  test("groupByCol[T1, T2, T3](col1: String, col2: String, col3: String): DataMap.Groups[(T1, T2, T3)]") {
    val result = gf1.groupByCol[String, Int, Boolean]("A", "B", "C").countRows().sortValues("A", "B", "C").resetIndex
    result shouldBe
      DataFrame(
        A = Seq("a", "a", "b", "b", "b"),
        B = Seq(1, 2, 1, 2, 3),
        C = Seq(true, true, true, true, true),
        count = Seq(1, 1, 1, 2, 1),
      )
    gf1.groupByCol[Int, Boolean, String]("B", "C", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    gf1.groupByCol[Boolean, Int, String]("C", "B", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    assertThrows[ColumnNotFoundException](gf1.groupByCol[String, Int, Boolean]("A", "X", "C"))
    assertThrows[SeriesCastException](gf1.groupByCol[Int, Int, Boolean]("A", "B", "C"))
  }

  test("groupByColOption[T](col: String): DataMap.Groups[Option[T]]") {
    gf1.groupByColOption[String]("A").countRows().sortValues("A").resetIndex shouldBe
      DataFrame(A = Series("a", "b", null), count = Seq(3, 4, 1))
    gf1.groupByColOption[Int]("B").countRows().sortValues("B").resetIndex shouldBe
      DataFrame(B = Series(1, 2, 3, null), count = Seq(3, 3, 1, 1))
    gf1.groupByColOption[Boolean]("C").countRows().sortValues("C").resetIndex shouldBe
      DataFrame(C = Seq(false, true), count = Seq(2, 6))
    gf1.groupByColOption[Int]("E").countRows().sortValues("E").resetIndex shouldBe
      DataFrame(E = Series(0, 1, 2, 3, 4, 5, null), count = Seq(1, 1, 1, 1, 1, 1, 2))
    gfDouble.groupByColOption[Double]("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Series(Double.NegativeInfinity, 1.0, 2.1, Double.PositiveInfinity, Double.NaN, Double.NaN, Double.NaN, null),
      count = Seq(1, 2, 3, 2, 1, 1, 1, 1),
    )
    assertThrows[ColumnNotFoundException](gf1.groupByColOption[String]("X"))
    assertThrows[SeriesCastException](gf1.groupByColOption[Int]("A"))
  }

  test("groupByColOption[T1, T2](col1: String, col2: String): DataMap.Groups[(Option[T1], Option[T2])]") {
    gf1.groupByColOption[String, Int]("A", "B").countRows().sortValues("A", "B").resetIndex shouldBe
      DataFrame(
        A = Series("a", "a", "a", "b", "b", "b", null),
        B = Series(1, 2, null, 1, 2, 3, 1),
        count = Seq(1, 1, 1, 1, 2, 1, 1),
      )
    gf1.groupByColOption[Int, Boolean]("B", "C").countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Series(1, 1, 2, 3, null), C = Seq(false, true, true, true, false), count = Seq(1, 2, 3, 1, 1))
    gf1.groupByColOption[String, Boolean]("A", "C").countRows().sortValues("A", "C").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b", null), C = Seq(false, true, true, false), count = Seq(1, 2, 4, 1))
    assertThrows[ColumnNotFoundException](gf1.groupByColOption[String, String]("A", "X"))
    assertThrows[SeriesCastException](gf1.groupByColOption[Int, Int]("A", "B"))
  }

  test(
    "groupByColOption[T1, T2, T3](col1: String, col2: String, col3: String): DataMap.Groups[(Option[T1], Option[T2], Option[T3])]"
  ) {
    val result =
      gf1.groupByColOption[String, Int, Boolean]("A", "B", "C").countRows().sortValues("A", "B", "C").resetIndex
    result shouldBe
      DataFrame(
        A = Series("a", "a", "a", "b", "b", "b", null),
        B = Series(1, 2, null, 1, 2, 3, 1),
        C = Series(true, true, false, true, true, true, false),
        count = Seq(1, 1, 1, 1, 2, 1, 1),
      )
    gf1
      .groupByColOption[Int, Boolean, String]("B", "C", "A")
      .countRows()
      .sortValues("A", "B", "C")
      .resetIndex shouldBe result
    gf1
      .groupByColOption[Boolean, Int, String]("C", "B", "A")
      .countRows()
      .sortValues("A", "B", "C")
      .resetIndex shouldBe result
    assertThrows[ColumnNotFoundException](gf1.groupByColOption[String, Int, Boolean]("A", "X", "C"))
  }

  test("groupByOption(col: String): DataMap.Groups[Option[Any]]") {
    gf1.groupByOption("A").countRows().sortValues("A").resetIndex shouldBe
      DataFrame(A = Series("a", "b", null), count = Seq(3, 4, 1))
    gf1.groupByOption("B").countRows().sortValues("B").resetIndex shouldBe
      DataFrame(B = Series(1, 2, 3, null), count = Seq(3, 3, 1, 1))
    gf1.groupByOption("C").countRows().sortValues("C").resetIndex shouldBe
      DataFrame(C = Seq(false, true), count = Seq(2, 6))
    gf1.groupByOption("E").countRows().sortValues("E").resetIndex shouldBe
      DataFrame(E = Series(0, 1, 2, 3, 4, 5, null), count = Seq(1, 1, 1, 1, 1, 1, 2))
    gfDouble.groupByOption("A").countRows().sortValues("A").resetIndex shouldBe DataFrame(
      A = Series(Double.NegativeInfinity, 1.0, 2.1, Double.PositiveInfinity, Double.NaN, Double.NaN, Double.NaN, null),
      count = Seq(1, 2, 3, 2, 1, 1, 1, 1),
    )
    assertThrows[ColumnNotFoundException](gf1.groupByOption("X"))

  }

  test("groupByOption(col1: String, col2: String): DataMap.Groups[(Option[Any], Option[Any])]") {
    gf1.groupByOption("A", "B").countRows().sortValues("A", "B").resetIndex shouldBe
      DataFrame(
        A = Series("a", "a", "a", "b", "b", "b", null),
        B = Series(1, 2, null, 1, 2, 3, 1),
        count = Seq(1, 1, 1, 1, 2, 1, 1),
      )
    gf1.groupByOption("B", "C").countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Series(1, 1, 2, 3, null), C = Seq(false, true, true, true, false), count = Seq(1, 2, 3, 1, 1))
    gf1.groupByOption("A", "C").countRows().sortValues("A", "C").resetIndex shouldBe
      DataFrame(A = Seq("a", "a", "b", null), C = Seq(false, true, true, false), count = Seq(1, 2, 4, 1))
    assertThrows[ColumnNotFoundException](gf1.groupByOption("A", "X"))
  }

  test(
    "groupByOption(col1: String, col2: String, col3: String): DataMap.Groups[(Option[Any], Option[Any], Option[Any])]"
  ) {
    val result = gf1.groupByOption("A", "B", "C").countRows().sortValues("A", "B", "C").resetIndex
    result shouldBe
      DataFrame(
        A = Series("a", "a", "a", "b", "b", "b", null),
        B = Series(1, 2, null, 1, 2, 3, 1),
        C = Series(true, true, false, true, true, true, false),
        count = Seq(1, 1, 1, 1, 2, 1, 1),
      )
    gf1.groupByOption("B", "C", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    gf1.groupByOption("C", "B", "A").countRows().sortValues("A", "B", "C").resetIndex shouldBe result
    assertThrows[ColumnNotFoundException](gf1.groupByOption("A", "X", "C"))
  }

  test("groupByOption(cols: Seq[String]): DataMap.Groups[Seq[Option[Any]]]") {
    gf1.groupByOption(Seq("A")).countRows().sortValues("A").resetIndex shouldBe
      DataFrame(A = Series("a", "b", null), count = Seq(3, 4, 1))
    gf1.groupByOption(Seq("B", "C")).countRows().sortValues("B", "C").resetIndex shouldBe
      DataFrame(B = Series(1, 1, 2, 3, null), C = Seq(false, true, true, true, false), count = Seq(1, 2, 3, 1, 1))
    gf1.groupByOption(Seq("A", "B", "C")).countRows().sortValues("A", "B", "C").resetIndex shouldBe
      DataFrame(
        A = Series("a", "a", "a", "b", "b", "b", null),
        B = Series(1, 2, null, 1, 2, 3, 1),
        C = Series(true, true, false, true, true, true, false),
        count = Seq(1, 1, 1, 1, 2, 1, 1),
      )
    assertThrows[ColumnNotFoundException](gf1.groupByOption(Seq("A", "X")))
  }

  test("indexBy[T](col: String): DataMap[T]") {
    // see also groupBy*
    gf1.indexBy[String]("A").keys.toSeq.sorted shouldBe Seq("a", "b")
    gf1.indexBy[Int]("B").keys.toSeq.sorted shouldBe Seq(1, 2, 3)
    gf1.indexBy[Boolean]("C").keys.toSeq.sorted shouldBe Seq(false, true)
    Series(gfDouble.indexBy[Double]("A").keys.toSeq.sorted*) shouldBe
      Series(Double.NegativeInfinity, 1.0, 2.1, Double.PositiveInfinity, Double.NaN, Double.NaN, Double.NaN)
    assertThrows[ColumnNotFoundException](gf1.indexBy[String]("X"))
    assertThrows[SeriesCastException](gf1.indexBy[Int]("A"))
  }

  test("indexBy[T1, T2](col1: String, col2: String): DataMap[(T1, T2)]") {
    // see also groupBy*
    gf1.indexBy[String, Int]("A", "B").keys.toSeq.sorted shouldBe Seq(("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3))
    gf1.indexBy[Int, Boolean]("B", "C").keys.toSeq.sorted shouldBe Seq((1, false), (1, true), (2, true), (3, true))
    gf1.indexBy[String, Boolean]("A", "C").keys.toSeq.sorted shouldBe Seq(("a", false), ("a", true), ("b", true))
    assertThrows[ColumnNotFoundException](gf1.indexBy[String, String]("A", "X"))
    assertThrows[SeriesCastException](gf1.indexBy[Int, Int]("A", "B"))
  }

  test("indexBy[T1, T2, T3](col1: String, col2: String, col3: String): DataMap[(T1, T2, T3)]") {
    // see also groupBy*
    val result = gf1.indexBy[String, Int, Boolean]("A", "B", "C").keys.toSeq.sorted
    result shouldBe Seq(("a", 1, true), ("a", 2, true), ("b", 1, true), ("b", 2, true), ("b", 3, true))
    assertThrows[ColumnNotFoundException](gf1.indexBy[String, Int, Boolean]("A", "X", "C"))
    assertThrows[SeriesCastException](gf1.indexBy[Int, Int, Boolean]("A", "B", "C"))
  }

  test("indexBy(cols: Seq[String]): DataMap[Seq[Any]]") {
    // see also groupBy*
    val x = gf1.indexBy(Seq("A")).keys.toSeq
    x.length shouldBe 2
    x should contain(Seq("a"))
    x should contain(Seq("b"))
    val y = gf1.indexBy(Seq("A", "B")).keys.toSeq
    y.length shouldBe 5
    y should contain(Seq("a", 1))
    y should contain(Seq("a", 2))
    y should contain(Seq("b", 1))
    y should contain(Seq("b", 2))
    y should contain(Seq("b", 3))
    val z = gf1.indexBy(Seq("A", "B", "C")).keys.toSeq
    z.length shouldBe 5
    z should contain(Seq("a", 1, true))
    z should contain(Seq("a", 2, true))
    z should contain(Seq("b", 1, true))
    z should contain(Seq("b", 2, true))
    z should contain(Seq("b", 3, true))
    assertThrows[ColumnNotFoundException](gf1.indexBy(Seq("A", "X")))
  }

  test("indexByOption[T](col: String): DataMap[Option[T]]") {
    // see also groupBy*
    gf1.indexByOption[String]("A").keys.toSeq.sorted shouldBe Seq(null, "a", "b").map(Option(_))
    gf1.indexByOption[Int]("B").keys.toSeq.sorted shouldBe Seq(null, 1, 2, 3).map(Option(_))
    gf1.indexByOption[Boolean]("C").keys.toSeq.sorted shouldBe Seq(false, true).map(Option(_))
    Series.from(gfDouble.indexByOption[Double]("A").keys.toSeq.sorted) shouldBe
      Series.from(
        Seq(
          None,
          Some(Double.NegativeInfinity),
          Some(1.0),
          Some(2.1),
          Some(Double.PositiveInfinity),
          Some(Double.NaN),
          Some(Double.NaN),
          Some(Double.NaN),
        )
      )
    assertThrows[ColumnNotFoundException](gf1.indexByOption[String]("X"))
    assertThrows[SeriesCastException](gf1.indexByOption[Int]("A"))
  }

  test("indexByOption[T1, T2](col1: String, col2: String): DataMap[(Option[T1], Option[T2])]") {
    // see also groupBy*
    gf1.indexByOption[String, Int]("A", "B").keys.toSeq.sorted shouldBe Seq(
      (None, Some(1)),
      (Some("a"), None),
      (Some("a"), Some(1)),
      (Some("a"), Some(2)),
      (Some("b"), Some(1)),
      (Some("b"), Some(2)),
      (Some("b"), Some(3)),
    )
    gf1.indexByOption[Int, Boolean]("B", "C").keys.toSeq.sorted shouldBe Seq(
      (None, Some(false)),
      (Some(1), Some(false)),
      (Some(1), Some(true)),
      (Some(2), Some(true)),
      (Some(3), Some(true)),
    )
    gf1.indexByOption[String, Boolean]("A", "C").keys.toSeq.sorted shouldBe Seq(
      (None, Some(false)),
      (Some("a"), Some(false)),
      (Some("a"), Some(true)),
      (Some("b"), Some((true))),
    )
    assertThrows[ColumnNotFoundException](gf1.indexByOption[String, String]("A", "X"))
    assertThrows[SeriesCastException](gf1.indexByOption[Int, Int]("A", "B"))
  }

  test(
    "indexByOption[T1, T2, T3](col1: String, col2: String, col3: String): DataMap[(Option[T1], Option[T2], Option[T3])]"
  ) {
    // see also groupBy*
    val result = gf1.indexByOption[String, Int, Boolean]("A", "B", "C").keys.toSeq.sorted
    result shouldBe Seq(
      (None, Some(1), Some(false)),
      (Some("a"), None, Some(false)),
      (Some("a"), Some(1), Some(true)),
      (Some("a"), Some(2), Some(true)),
      (Some("b"), Some(1), Some(true)),
      (Some("b"), Some(2), Some(true)),
      (Some("b"), Some(3), Some(true)),
    )
    assertThrows[ColumnNotFoundException](gf1.indexByOption[String, Int, Boolean]("A", "X", "C"))
    assertThrows[SeriesCastException](gf1.indexByOption[Int, Int, Boolean]("A", "B", "C"))
  }

  test("indexIterator: Iterator[Int]") {
    {
      val it = df1.indexIterator
      it.next() shouldBe 0
      it.next() shouldBe 1
      it.next() shouldBe 2
      it.next() shouldBe 3
      it.next() shouldBe 4
      it.hasNext shouldBe false
    }
    {
      val it = df3.indexIterator
      it.next() shouldBe 0
      it.next() shouldBe 2
      it.next() shouldBe 3
      it.next() shouldBe 4
      it.hasNext shouldBe false
    }
    {
      val it = df3.sortValues("B").indexIterator
      it.next() shouldBe 2
      it.next() shouldBe 4
      it.next() shouldBe 0
      it.next() shouldBe 3
      it.hasNext shouldBe false
    }
  }

  test("info: String") {
    df1.info should include("A [Int]")
    df1.info should include("B [Double]")
    df1.info should include("C [String]")
    df1.info should include("D [C1]")
    df1.info should include("Uniform")
    df1.info should include("5")
    df2.info should include("A [Int?]")
    df2.info should include("B [Double?]")
    df2.info should include("C [String?]")
    df2.info should include("D [C1?]")
    df2.info should include("Uniform")
    df2.info should include("5")
    df3.info should include("A [Int?]")
    df3.info should include("B [Double?]")
    df3.info should include("C [String?]")
    df3.info should include("D [C1?]")
    df3.info should include("Sequential")
    df3.info should include("4")
  }

  test("joinInner(df: DataFrame, cols: String*): DataFrame") {
    gf1.joinInner(gf2, "A", "B", "C").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "b"),
      "B" -> Series(1, 1, 2, 2, 3),
      "C" -> Series(true, true, true, true, true),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 6.0),
      "E" -> Series(0, 0, null, null, 3),
      "X" -> Series(1, 7, 2, 8, 3),
      "Y" -> Series("A", "G", "B", null, null),
    )
    gf1.joinInner(gf2, "A", "B").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "b", "b"),
      "B" -> Series(1, 1, 2, 2, 1, 3),
      "C" -> Series(true, true, true, true, true, true),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 7.0, 6.0),
      "E" -> Series(0, 0, null, null, 4, 3),
      "X" -> Series(1, 7, 2, 8, 4, 3),
      "Y" -> Series("A", "G", "B", null, "D", null),
    )
    gf1.joinInner(gf2, "A").numRows shouldBe 20
    gf1.joinInner(gf2, "E") shouldBe gf1.joinInner(gf2, "A")(Seq[Int]()).resetIndex
  }

  test("joinLeft(df: DataFrame, cols: String*): DataFrame") {
    gf1.joinLeft(gf2, "A", "B", "C").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "a", "b", "b", "b", "b", null),
      "B" -> Series(1, 1, 2, 2, null, 1, 2, 2, 3, 1),
      "C" -> Series(true, true, true, true, false, true, true, true, true, false),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 3.0, 7.0, 5.0, 8.0, 6.0, 4.0),
      "E" -> Series(0, 0, null, null, 1, 4, null, 5, 3, 2),
      "X" -> Series(1, 7, 2, 8, null, null, null, null, 3, null),
      "Y" -> Series("A", "G", "B", null, null, null, null, null, null, null),
    )
    gf1.joinLeft(gf2, "A", "B").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "a", "b", "b", "b", "b", null),
      "B" -> Series(1, 1, 2, 2, null, 1, 2, 2, 3, 1),
      "C" -> Series(true, true, true, true, false, true, true, true, true, false),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 3.0, 7.0, 5.0, 8.0, 6.0, 4.0),
      "E" -> Series(0, 0, null, null, 1, 4, null, 5, 3, 2),
      "X" -> Series(1, 7, 2, 8, null, 4, null, null, 3, null),
      "Y" -> Series("A", "G", "B", null, null, "D", null, null, null, null),
    )
    gf1.joinLeft(gf2, "A").numRows shouldBe 21
    gf1.joinLeft(gf2, "E").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe
      gf1.sortValues("A", "B", "C", "D", "E").resetIndex.cols("X" -> gf1(Seq(), "B"), "Y" -> gf1(Seq(), "A"))
  }

  test("joinOuter(df: DataFrame, cols: String*): DataFrame") {
    gf1.joinOuter(gf2, "A", "B", "C").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", null),
      "B" -> Series(1, 1, 2, 2, null, 1, 1, 2, 2, 3, 2, null, 1),
      "C" -> Series(true, true, true, true, false, false, true, true, true, true, true, true, false),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 3.0, null, 7.0, 5.0, 8.0, 6.0, null, null, 4.0),
      "E" -> Series(0, 0, null, null, 1, null, 4, null, 5, 3, null, null, 2),
      "X" -> Series(1, 7, 2, 8, null, 4, null, null, null, 3, 5, 6, null),
      "Y" -> Series("A", "G", "B", null, null, "D", null, null, null, null, "E", "F", null),
    )
    gf1.joinOuter(gf2, "A", "B").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "a", "b", "b", "b", "b", "c", "c", null),
      "B" -> Series(1, 1, 2, 2, null, 1, 2, 2, 3, 2, null, 1),
      "C" -> Series(true, true, true, true, false, true, true, true, true, null, null, false),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 3.0, 7.0, 5.0, 8.0, 6.0, null, null, 4.0),
      "E" -> Series(0, 0, null, null, 1, 4, null, 5, 3, null, null, 2),
      "X" -> Series(1, 7, 2, 8, null, 4, null, null, 3, 5, 6, null),
      "Y" -> Series("A", "G", "B", null, null, "D", null, null, null, "E", "F", null),
    )
    gf1.joinOuter(gf2, "A").numRows shouldBe 23
    gf1.joinOuter(gf2, "E").sortValues("A", "B", "C", "E").resetIndex shouldBe
      DataFrame
        .union(
          gf1.cols("X" -> gf1(Seq(), "B"), "Y" -> gf1(Seq(), "A")),
          gf2(Seq("E", "X", "Y"))
            .cols(
              "A" -> gf1(Seq(), "A"),
              "B" -> gf1(Seq(), "B"),
              "C" -> gf1(Seq(), "C"),
              "D" -> gf1(Seq(), "D"),
              "D" -> gf1(Seq(), "D"),
            ),
        )
        .sortValues("A", "B", "C", "E")
        .resetIndex
  }

  test("joinRight(df: DataFrame, cols: String*): DataFrame") {
    gf1.joinRight(gf2, "A", "B", "C").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "b", "b", "c", "c"),
      "B" -> Series(1, 1, 2, 2, 1, 3, 2, null),
      "C" -> Series(true, true, true, true, false, true, true, true),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, null, 6.0, null, null),
      "E" -> Series(0, 0, null, null, null, 3, null, null),
      "X" -> Series(1, 7, 2, 8, 4, 3, 5, 6),
      "Y" -> Series("A", "G", "B", null, "D", null, "E", "F"),
    )
    gf1.joinRight(gf2, "A", "B").sortValues("A", "B", "C", "D", "E").resetIndex shouldBe DataFrame(
      "A" -> Series("a", "a", "a", "a", "b", "b", "c", "c"),
      "B" -> Series(1, 1, 2, 2, 1, 3, 2, null),
      "C" -> Series(true, true, true, true, true, true, null, null),
      "D" -> Series(1.0, 1.0, 2.0, 2.0, 7.0, 6.0, null, null),
      "E" -> Series(0, 0, null, null, 4, 3, null, null),
      "X" -> Series(1, 7, 2, 8, 4, 3, 5, 6),
      "Y" -> Series("A", "G", "B", null, "D", null, "E", "F"),
    )
    gf1.joinRight(gf2, "A").numRows shouldBe 22
    gf1.joinRight(gf2, "E").sortValues("E", "X", "Y").resetIndex shouldBe
      gf2(Seq("E", "X", "Y"))
        .cols(
          "A" -> gf1(Seq(), "A"),
          "B" -> gf1(Seq(), "B"),
          "C" -> gf1(Seq(), "C"),
          "D" -> gf1(Seq(), "D"),
          "D" -> gf1(Seq(), "D"),
        )
        .sortValues("E", "X", "Y")
        .resetIndex
  }

  test("merge(df: DataFrame): DataFrame") {
    val sA = Series(60, 30, 20, 80, 40, 30) as "A"
    val sB = Series(23, 1, 1, 7, 3, 2) as "B"
    val sE = Series(3, 1, 1, 7, 3, 2) as "E"
    val sE2 = Series(3, 1, 1, 7, 3) as "E"
    val dfM = sA | sB | sE
    df1.merge(dfM) shouldBe DataFrame(
      "A" -> Series(60, 30, 20, 80, 40, 30),
      "B" -> Series(23, 1, 1, 7, 3, 2),
      "C" -> Series("ghi", "ABC", "XyZ", "qqq", "Uuu", null),
      "D" -> Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0), null),
      "E" -> Series(3, 1, 1, 7, 3, 2),
    )
    df3(Seq(3, 2, 0)).merge(dfM) shouldBe DataFrame(
      "A" -> Series(60, 30, 20, 80, 40, 30),
      "B" -> Series(23, 1, 1, 7, 3, 2),
      "C" -> Series("ghi", null, null, "qqq", null, null),
      "D" -> Series(C1("A", 5), null, C1("C", 2), C1("D", 2), null, null),
      "E" -> Series(3, 1, 1, 7, 3, 2),
    )
    df1.sortValues("B").merge(DataFrame(sE2)) shouldBe DataFrame(
      "A" -> Series(6, 3, 2, 8, 4),
      "B" -> Series(23.1, 1.4, 1.4, 7.0, 3.1),
      "C" -> Series("ghi", "ABC", "XyZ", "qqq", "Uuu"),
      "D" -> Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)),
      "E" -> Series(3, 1, 1, 7, 3),
    )
  }

  test("merge(series: Series[?]*)") {
    val sA = Series(60, 30, 20, 80, 40, 30) as "A"
    val sB = Series(23, 1, 1, 7, 3, 2) as "B"
    val sE = Series(3, 1, 1, 7, 3, 2) as "E"
    val sE2 = Series(3, 1, 1, 7, 3) as "E"
    val dfM = sA | sB | sE
    df1.merge(sA, sB, sE) shouldBe DataFrame(
      "A" -> Series(60, 30, 20, 80, 40, 30),
      "B" -> Series(23, 1, 1, 7, 3, 2),
      "C" -> Series("ghi", "ABC", "XyZ", "qqq", "Uuu", null),
      "D" -> Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0), null),
      "E" -> Series(3, 1, 1, 7, 3, 2),
    )
    df3(Seq(3, 2, 0)).merge(sE, sB, sA) shouldBe DataFrame(
      "A" -> Series(60, 30, 20, 80, 40, 30),
      "B" -> Series(23, 1, 1, 7, 3, 2),
      "C" -> Series("ghi", null, null, "qqq", null, null),
      "D" -> Series(C1("A", 5), null, C1("C", 2), C1("D", 2), null, null),
      "E" -> Series(3, 1, 1, 7, 3, 2),
    )
    df1.sortValues("B").merge(sE2) shouldBe DataFrame(
      "A" -> Series(6, 3, 2, 8, 4),
      "B" -> Series(23.1, 1.4, 1.4, 7.0, 3.1),
      "C" -> Series("ghi", "ABC", "XyZ", "qqq", "Uuu"),
      "D" -> Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)),
      "E" -> Series(3, 1, 1, 7, 3),
    )
  }

  test("numCols: Int") {
    df1.numCols shouldBe 4
    df1(Seq("B", "D")).numCols shouldBe 2
    df1(Seq("A")).numCols shouldBe 1
    (df1 | "E" -> df1("A")).numCols shouldBe 5
    df1(Seq[Int]()).numCols shouldBe 4
  }

  test("numRows: Int") {
    df1.numRows shouldBe 5
    df3.numRows shouldBe 4
    df1.sortValues("B").numRows shouldBe 5
    df1(2 to 3).numRows shouldBe 2
    df1(Seq(3, 2, 0)).numRows shouldBe 3
    df1(Seq[Int]()).numRows shouldBe 0
  }

  test("numRowsBase: Int") {
    df1.numRowsBase shouldBe 5
    df3.numRowsBase shouldBe 5
    df1.sortValues("B").numRowsBase shouldBe 5
    df1(2 to 3).numRowsBase shouldBe 5
    df1(Seq(3, 2, 0)).numRowsBase shouldBe 5
    df1(Seq[Int]()).numRowsBase shouldBe 5
    df1.sortValues("B").resetIndex.numRowsBase shouldBe 5
    df1(2 to 3).resetIndex.numRowsBase shouldBe 2
    df1(Seq(3, 2, 0)).resetIndex.numRowsBase shouldBe 3
    df1(Seq[Int]()).resetIndex.numRowsBase shouldBe 0
  }

  test("plot: Plot") {
    // see Plot
  }

  test("requires: Requirement") {
    // see Requirement
  }

  test("resetIndex: Series[T]") {
    df1.resetIndex shouldBe df1
    df2.resetIndex shouldBe df2
    df3.resetIndex shouldBe DataFrame(
      "A" -> Series(6, 2, 8, 4),
      "B" -> Series(23.1, 1.4, null, 3.1),
      "C" -> Series("ghi", null, "qqq", "Uuu"),
      "D" -> Series(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0)),
    )

    df1.sorted[Int]("A").resetIndex shouldBe DataFrame(
      "A" -> Series(2, 3, 4, 6, 8),
      "B" -> Series(1.4, 1.4, 3.1, 23.1, 7.0),
      "C" -> Series("XyZ", "ABC", "Uuu", "ghi", "qqq"),
      "D" -> Series(C1("C", 2), C1("B", 1), C1("E", 0), C1("A", 5), C1("D", 2)),
    )
    df1.sorted[Int]("A").resetIndex.A shouldBe Series("A")(2, 3, 4, 6, 8)
    df1.sorted[Double]("B").resetIndex.B shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, 23.1)
    df1.sorted[String]("C").resetIndex.C shouldBe Series("C")("ABC", "ghi", "qqq", "Uuu", "XyZ")
    df1.sorted[C1]("D").resetIndex.D shouldBe Series("D")(C1("E", 0), C1("B", 1), C1("C", 2), C1("D", 2), C1("A", 5))
    df2.sorted[Int]("A").resetIndex.A shouldBe Series("A")(3, 4, 6, 8, null)
    df2.sorted[Double]("B").resetIndex.B shouldBe Series("B")(1.4, 1.4, 3.1, 7.0, null)
    df2.sorted[String]("C" -> Order.ascNullsFirst).resetIndex.C shouldBe Series("C")(null, null, "ABC", "ghi", "XyZ")
    df2.sorted[C1]("D" -> Order.ascNullsFirst).resetIndex.D shouldBe
      Series("D")(null, C1("E", 0), C1("C", 2), C1("D", 2), C1("A", 5))
    df3.sorted[Int]("A").resetIndex.A shouldBe Series("A")(2, 4, 6, 8)
    df3.sorted[Double]("B").resetIndex.B shouldBe Series("B")(1.4, 3.1, 23.1, null)
    df3.sorted[String]("C" -> Order.ascNullsFirst).resetIndex.C shouldBe Series("C")(null, "ghi", "qqq", "Uuu")
    df3.sorted[C1]("D" -> Order.ascNullsFirst).resetIndex.D shouldBe
      Series("D")(C1("E", 0), C1("C", 2), C1("D", 2), C1("A", 5))

    df1("A").sortValues.resetIndex.sortValues.resetIndex shouldBe Series("A")(2, 3, 4, 6, 8)
    df1("A").sortValues.resetIndex.sortValues.resetIndex.sortValues.resetIndex shouldBe Series("A")(2, 3, 4, 6, 8)
  }

  test("select(cols: Array[String]): DataFrame") {
    df1.select(Array("A", "B", "C", "D")) shouldBe df1
    df1.select(Array("B")) shouldBe DataFrame(s1B)
    df1.select(Array("A", "C")) shouldBe DataFrame(s1A, s1C)
    df1.select(Array("A", "B")) shouldBe DataFrame(s1A, s1B)
    df1.select(Array("C", "D")) shouldBe DataFrame(s1C, s1D)
    df2.select(Array("A", "B", "C", "D")) shouldBe df2
    df2.select(Array("B")) shouldBe DataFrame(s2B)
    df2.select(Array("A", "C")) shouldBe DataFrame(s2A, s2C)
    df2.select(Array("A", "B")) shouldBe DataFrame(s2A, s2B)
    df2.select(Array("C", "D")) shouldBe DataFrame(s2C, s2D)
    df3.select(Array("A", "B", "C", "D")) shouldBe df3
    df3.select(Array("B")) shouldBe DataFrame(s3B)
    df3.select(Array("A", "C")) shouldBe DataFrame(s3A, s3C)
    df3.select(Array("A", "B")) shouldBe DataFrame(s3A, s3B)
    df3.select(Array("C", "D")) shouldBe DataFrame(s3C, s3D)
    assertThrows[ColumnNotFoundException](df1.select(Array("X")))
    assertThrows[ColumnNotFoundException](df1.select(Array("A", "B", "X", "D")))
  }

  test("select(cols: Seq[String]): DataFrame") {
    df1.select(Seq("A", "B", "C", "D")) shouldBe df1
    df1.select(Seq("B")) shouldBe DataFrame(s1B)
    df1.select(Seq("A", "C")) shouldBe DataFrame(s1A, s1C)
    df1.select(Seq("A", "B")) shouldBe DataFrame(s1A, s1B)
    df1.select(Seq("C", "D")) shouldBe DataFrame(s1C, s1D)
    df2.select(Seq("A", "B", "C", "D")) shouldBe df2
    df2.select(Seq("B")) shouldBe DataFrame(s2B)
    df2.select(Seq("A", "C")) shouldBe DataFrame(s2A, s2C)
    df2.select(Seq("A", "B")) shouldBe DataFrame(s2A, s2B)
    df2.select(Seq("C", "D")) shouldBe DataFrame(s2C, s2D)
    df3.select(Seq("A", "B", "C", "D")) shouldBe df3
    df3.select(Seq("B")) shouldBe DataFrame(s3B)
    df3.select(Seq("A", "C")) shouldBe DataFrame(s3A, s3C)
    df3.select(Seq("A", "B")) shouldBe DataFrame(s3A, s3B)
    df3.select(Seq("C", "D")) shouldBe DataFrame(s3C, s3D)
    assertThrows[ColumnNotFoundException](df1.select(Seq("X")))
    assertThrows[ColumnNotFoundException](df1.select(Seq("A", "B", "X", "D")))
  }

  test("select(cols: String*): DataFrame") {
    df1.select("A", "B", "C", "D") shouldBe df1
    df1.select("B") shouldBe DataFrame(s1B)
    df1.select("A", "C") shouldBe DataFrame(s1A, s1C)
    df1.select("A", "B") shouldBe DataFrame(s1A, s1B)
    df1.select("C", "D") shouldBe DataFrame(s1C, s1D)
    df2.select("A", "B", "C", "D") shouldBe df2
    df2.select("B") shouldBe DataFrame(s2B)
    df2.select("A", "C") shouldBe DataFrame(s2A, s2C)
    df2.select("A", "B") shouldBe DataFrame(s2A, s2B)
    df2.select("C", "D") shouldBe DataFrame(s2C, s2D)
    df3.select("A", "B", "C", "D") shouldBe df3
    df3.select("B") shouldBe DataFrame(s3B)
    df3.select("A", "C") shouldBe DataFrame(s3A, s3C)
    df3.select("A", "B") shouldBe DataFrame(s3A, s3B)
    df3.select("C", "D") shouldBe DataFrame(s3C, s3D)
    assertThrows[ColumnNotFoundException](df1.select("X"))
    assertThrows[ColumnNotFoundException](df1.select("A", "B", "X", "D"))
  }

  test("selectDynamic(field: String): Series[Any]") {
    df1.A shouldBe s1A
    df1.B shouldBe s1B
    df1.C shouldBe s1C
    df1.D shouldBe s1D
    df2.A shouldBe s2A
    df2.B shouldBe s2B
    df2.C shouldBe s2C
    df2.D shouldBe s2D
    df3.A shouldBe s3A
    df3.B shouldBe s3B
    df3.C shouldBe s3C
    df3.D shouldBe s3D
    assertThrows[ColumnNotFoundException](df1.X)
    assertThrows[ColumnNotFoundException](df3.`A `)
  }

  test("sorted[T](col: String): DataFrame") {
    df1.sorted[Int]("A") shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sorted[Double]("B") shouldBe df1(Seq(1, 2, 4, 3, 0))
    df1.sorted[String]("C") shouldBe df1(Seq(1, 0, 3, 4, 2))
    df1.sorted[C1]("D") shouldBe df1(Seq(4, 1, 2, 3, 0))
    df3.sorted[Int]("A") shouldBe df3(Seq(2, 4, 0, 3))
    df3.sorted[Double]("B") shouldBe df3(Seq(2, 4, 0, 3))
    df3.sorted[String]("C") shouldBe df3(Seq(0, 3, 4, 2))
    df3.sorted[C1]("D") shouldBe df3(Seq(4, 2, 3, 0))
  }

  test("sorted[T1, T2](col1: String, col2: String): DataFrame") {
    df1.sorted[Double, Int]("B", "A") shouldBe df1(Seq(2, 1, 4, 3, 0))
    df1.sorted[Double, String]("B", "C") shouldBe df1(Seq(1, 2, 4, 3, 0))
    df1.sorted[Double, Double]("B", "B") shouldBe df1(Seq(1, 2, 4, 3, 0))

    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, 4.0, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sorted[Int, Double]("A", "B") shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sorted[Int, String]("A", "C") shouldBe df(Seq(2, 3, 1, 0, 4))
    df.sorted[Double, Int]("B", "A") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sorted[Double, String]("B", "C") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sorted[String, Int]("C", "A") shouldBe df(Seq(2, 3, 0, 1, 4))
    df.sorted[String, Double]("C", "B") shouldBe df(Seq(0, 3, 2, 1, 4))

    val dfNull = DataFrame(
      "A" -> Series[Int](null, null, null, null),
      "B" -> Series[Double](null, null, null, null),
      "C" -> Series[Boolean](null, null, null, null),
      "D" -> Series[Int](4, 3, 1, 2),
    )
    dfNull(Seq("A", "D")).sorted[Int, Int]("A", "D") shouldBe dfNull(Seq("A", "D"))(Seq(2, 3, 1, 0))

    assertThrows[ClassCastException](df1.sorted[String, Double]("A", "B"))
    assertThrows[ClassCastException](df1.sorted[Double, Double]("B", "A"))
  }

  test("sorted[T1, T2, T3](col1: String, col2: String, col3: String): DataFrame") {
    df1.sorted[Int, Double, String]("A", "B", "C") shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sorted[String, Int, Double]("C", "A", "B") shouldBe df1(Seq(1, 0, 3, 4, 2))

    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, 4.0, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sorted[Int, Double, String]("A", "B", "C") shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sorted[Double, Int, String]("B", "A", "C") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sorted[String, Int, Double]("C", "A", "B") shouldBe df(Seq(3, 2, 0, 1, 4))
    df.sorted[String, Double, Int]("C", "B", "A") shouldBe df(Seq(0, 3, 2, 1, 4))

    assertThrows[ClassCastException](df1.sorted[String, Double, String]("A", "B", "C"))
    assertThrows[ClassCastException](df1.sorted[Double, Double, String]("B", "A", "C"))
  }

  test("sorted[T](key: (String, Order)): DataFrame") {
    df1.sorted[Int]("A" -> Order.asc) shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sorted[Double]("B" -> Order.desc) shouldBe df1(Seq(0, 3, 4, 1, 2))
    df1.sorted[String]("C" -> Order.ascNullsFirst) shouldBe df1(Seq(1, 0, 3, 4, 2))
    df1.sorted[C1]("D" -> Order.descNullsFirst) shouldBe df1(Seq(4, 1, 2, 3, 0).reverse)
    df3.sorted[Int]("A" -> Order.asc) shouldBe df3(Seq(2, 4, 0, 3))
    df3.sorted[Double]("B" -> Order.asc) shouldBe df3(Seq(2, 4, 0, 3))
    df3.sorted[Double]("B" -> Order.ascNullsFirst) shouldBe df3(Seq(3, 2, 4, 0))
    df3.sorted[String]("C" -> Order.desc) shouldBe df3(Seq(4, 3, 0, 2))
    df3.sorted[String]("C" -> Order.descNullsFirst) shouldBe df3(Seq(2, 4, 3, 0))
    df3.sorted[C1]("D" -> Order.desc) shouldBe df3(Seq(4, 2, 3, 0).reverse)
  }

  test("sorted[T1, T2](key1: (String, Order), key2: (String, Order)): DataFrame") {
    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, null, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sorted[Int, Double]("A" -> Order.asc, "B" -> Order.asc) shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sorted[Int, Double]("A" -> Order.asc, "B" -> Order.desc) shouldBe df(Seq(3, 2, 1, 4, 0))
    df.sorted[Int, Double]("A" -> Order.asc, "B" -> Order.descNullsFirst) shouldBe df(Seq(2, 3, 1, 4, 0))
    df.sorted[Int, String]("A" -> Order.asc, "C" -> Order.desc) shouldBe df(Seq(2, 3, 1, 4, 0))
    df.sorted[Double, Int]("B" -> Order.desc, "A" -> Order.desc) shouldBe df(Seq(3, 4, 0, 1, 2))
    df.sorted[Double, String]("B" -> Order.descNullsFirst, "C" -> Order.asc) shouldBe df(Seq(1, 0, 4, 3, 2).reverse)
    df.sorted[String, Int]("C" -> Order.desc, "A" -> Order.desc) shouldBe df(Seq(4, 1, 0, 2, 3))
    df.sorted[String, Double]("C" -> Order.ascNullsFirst, "B" -> Order.ascNullsFirst) shouldBe df(Seq(2, 0, 3, 1, 4))

    assertThrows[ClassCastException](df1.sorted[String, Double]("A" -> Order.asc, "B" -> Order.descNullsFirst))
    assertThrows[ClassCastException](df1.sorted[Double, Double]("B" -> Order.asc, "A" -> Order.descNullsFirst))
  }

  test("sorted[T1, T2, T3](key1: (String, Order), key2: (String, Order), key3: (String, Order)): DataFrame") {

    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, null, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sorted[Int, Double, String]("A" -> Order.desc, "B" -> Order.desc, "C" -> Order.desc) shouldBe df(
      Seq(4, 0, 1, 3, 2)
    )
    df.sorted[Double, Int, String]("B" -> Order.ascNullsFirst, "A" -> Order.desc, "C" -> Order.asc) shouldBe df(
      Seq(2, 1, 0, 4, 3)
    )
    df.sorted[String, Int, Double]("C" -> Order.asc, "A" -> Order.asc, "B" -> Order.ascNullsFirst) shouldBe df(
      Seq(2, 3, 0, 1, 4)
    )
    df.sorted[String, Double, Int]("C" -> Order.asc, "B" -> Order.descNullsFirst, "A" -> Order.asc) shouldBe df(
      Seq(2, 3, 0, 4, 1)
    )

    assertThrows[ClassCastException](
      df1.sorted[String, Double]("A" -> Order.asc, "B" -> Order.descNullsFirst),
      "C" -> Order.desc,
    )
    assertThrows[ClassCastException](
      df1.sorted[Double, Double]("B" -> Order.asc, "A" -> Order.descNullsFirst),
      "C" -> Order.desc,
    )
  }

  test("sortValues(cols: String*): DataFrame") {
    df1.sortValues("A") shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sortValues("B") shouldBe df1(Seq(1, 2, 4, 3, 0))
    df1.sortValues("C") shouldBe df1(Seq(1, 0, 3, 4, 2))
    df3.sortValues("A") shouldBe df3(Seq(2, 4, 0, 3))
    df3.sortValues("B") shouldBe df3(Seq(2, 4, 0, 3))
    df3.sortValues("C") shouldBe df3(Seq(0, 3, 4, 2))

    df1.sortValues("A", "B", "C") shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sortValues("C", "A", "B") shouldBe df1(Seq(1, 0, 3, 4, 2))
    df1.sortValues("B", "A") shouldBe df1(Seq(2, 1, 4, 3, 0))
    df1.sortValues("B", "C") shouldBe df1(Seq(1, 2, 4, 3, 0))
    df1.sortValues("B", "B") shouldBe df1(Seq(1, 2, 4, 3, 0))

    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, 4.0, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sortValues("A") shouldBe df(Seq(2, 3, 1, 0, 4))
    df.sortValues("B") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sortValues("C") shouldBe df(Seq(0, 2, 3, 1, 4))
    df.sortValues("A", "B") shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sortValues("A", "C") shouldBe df(Seq(2, 3, 1, 0, 4))
    df.sortValues("B", "A") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sortValues("B", "C") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sortValues("C", "A") shouldBe df(Seq(2, 3, 0, 1, 4))
    df.sortValues("C", "B") shouldBe df(Seq(0, 3, 2, 1, 4))
    df.sortValues("A", "B", "C") shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sortValues("B", "A", "C") shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sortValues("C", "A", "B") shouldBe df(Seq(3, 2, 0, 1, 4))
    df.sortValues("C", "B", "A") shouldBe df(Seq(0, 3, 2, 1, 4))

    val dfNull = DataFrame(
      "A" -> Series[Int](null, null, null, null),
      "B" -> Series[Double](null, null, null, null),
      "C" -> Series[Boolean](null, null, null, null),
      "D" -> Series[Int](4, 3, 1, 2),
    )
    dfNull(Seq("A", "D")).sortValues("A", "D") shouldBe dfNull(Seq("A", "D"))(Seq(2, 3, 1, 0))
    dfNull.sortValues("A", "B", "C", "D") shouldBe dfNull(Seq(2, 3, 1, 0))

    val dfDate = DataFrame(
      "A" -> Series[LocalDate](LocalDate.now().plusWeeks(1), LocalDate.now().plusDays(-1), LocalDate.now())
    )
    dfDate.sortValues("A").resetIndex shouldBe DataFrame(
      "A" -> Series[LocalDate](LocalDate.now().plusDays(-1), LocalDate.now(), LocalDate.now().plusWeeks(1))
    )

    assertThrows[IllegalOperation](df1.sortValues("D"))
    assertThrows[IllegalOperation](df3.sortValues("D"))
  }

  test("sortValues(keys: (String, Order)*): DataFrame") {
    df1.sortValues("A" -> Order.asc) shouldBe df1(Seq(2, 1, 4, 0, 3))
    df1.sortValues("B" -> Order.desc) shouldBe df1(Seq(0, 3, 4, 1, 2))
    df1.sortValues("C" -> Order.ascNullsFirst) shouldBe df1(Seq(1, 0, 3, 4, 2))
    df3.sortValues("A" -> Order.asc) shouldBe df3(Seq(2, 4, 0, 3))
    df3.sortValues("B" -> Order.asc) shouldBe df3(Seq(2, 4, 0, 3))
    df3.sortValues("B" -> Order.ascNullsFirst) shouldBe df3(Seq(3, 2, 4, 0))
    df3.sortValues("C" -> Order.desc) shouldBe df3(Seq(4, 3, 0, 2))
    df3.sortValues("C" -> Order.descNullsFirst) shouldBe df3(Seq(2, 4, 3, 0))

    val df = DataFrame(
      Series(3, 2, 1, 1, 3) as "A",
      Series(0.1, -0.1, null, 1.0, 0.5) as "B",
      Series("A", "B", "A", "A", "B") as "C",
    )
    df.sortValues("A" -> Order.desc) shouldBe df(Seq(0, 4, 1, 2, 3))
    df.sortValues("B" -> Order.asc) shouldBe df(Seq(1, 0, 4, 3, 2))
    df.sortValues("B" -> Order.ascNullsFirst) shouldBe df(Seq(2, 1, 0, 4, 3))
    df.sortValues("B" -> Order.descNullsFirst) shouldBe df(Seq(1, 0, 4, 3, 2).reverse)
    df.sortValues("C" -> Order.descNullsFirst) shouldBe df(Seq(1, 4, 0, 2, 3))
    df.sortValues("A" -> Order.asc, "B" -> Order.asc) shouldBe df(Seq(3, 2, 1, 0, 4))
    df.sortValues("A" -> Order.asc, "B" -> Order.desc) shouldBe df(Seq(3, 2, 1, 4, 0))
    df.sortValues("A" -> Order.asc, "B" -> Order.descNullsFirst) shouldBe df(Seq(2, 3, 1, 4, 0))
    df.sortValues("A" -> Order.asc, "C" -> Order.desc) shouldBe df(Seq(2, 3, 1, 4, 0))
    df.sortValues("B" -> Order.desc, "A" -> Order.desc) shouldBe df(Seq(3, 4, 0, 1, 2))
    df.sortValues("B" -> Order.descNullsFirst, "C" -> Order.asc) shouldBe df(Seq(1, 0, 4, 3, 2).reverse)
    df.sortValues("C" -> Order.desc, "A" -> Order.desc) shouldBe df(Seq(4, 1, 0, 2, 3))
    df.sortValues("C" -> Order.ascNullsFirst, "B" -> Order.ascNullsFirst) shouldBe df(Seq(2, 0, 3, 1, 4))
    df.sortValues("A" -> Order.desc, "B" -> Order.desc, "C" -> Order.desc) shouldBe df(Seq(4, 0, 1, 3, 2))
    df.sortValues("B" -> Order.ascNullsFirst, "A" -> Order.desc, "C" -> Order.asc) shouldBe df(Seq(2, 1, 0, 4, 3))
    df.sortValues("C" -> Order.asc, "A" -> Order.asc, "B" -> Order.ascNullsFirst) shouldBe df(Seq(2, 3, 0, 1, 4))
    df.sortValues("C" -> Order.asc, "B" -> Order.descNullsFirst, "A" -> Order.asc) shouldBe df(Seq(2, 3, 0, 4, 1))

    assertThrows[IllegalOperation](df1.sortValues("D" -> Order.ascNullsFirst))
    assertThrows[IllegalOperation](df3.sortValues("D" -> Order.descNullsFirst))
  }

  test("show(n: Int, width: Int, annotateIndex: Boolean, annotateType: Boolean, colWidth: Int): Unit") {
    // see toString
  }

  test("toArray[T](col: String): Array[Option[T]]") {
    df1.toArray[Int]("A") shouldBe Array(6, 3, 2, 8, 4).map(Some(_))
    df1.toArray[Double]("B") shouldBe Array(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    df1.toArray[String]("C") shouldBe Array("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    df1.toArray[C1]("D") shouldBe Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    df2.toArray[Int]("A") shouldBe Array(6, 3, null, 8, 4).map(Option(_))
    df2.toArray[Double]("B") shouldBe Array(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    df2.toArray[String]("C") shouldBe Array("ghi", "ABC", "XyZ", null, null).map(Option(_))
    df2.toArray[C1]("D") shouldBe Array(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toFlatArray[T](col: String): Array[T]") {
    df1.toFlatArray[Int]("A") shouldBe Array(6, 3, 2, 8, 4)
    df1.toFlatArray[Double]("B") shouldBe Array(23.1, 1.4, 1.4, 7.0, 3.1)
    df1.toFlatArray[String]("C") shouldBe Array("ghi", "ABC", "XyZ", "qqq", "Uuu")
    df1.toFlatArray[C1]("D") shouldBe Array(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    df2.toFlatArray[Int]("A") shouldBe Array(6, 3, 8, 4)
    df2.toFlatArray[Double]("B") shouldBe Array(1.4, 1.4, 7.0, 3.1)
    df2.toFlatArray[String]("C") shouldBe Array("ghi", "ABC", "XyZ")
    df2.toFlatArray[C1]("D") shouldBe Array(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toFlatList[T](col: String): List[T]") {
    df1.toFlatList[Int]("A") shouldBe List(6, 3, 2, 8, 4)
    df1.toFlatList[Double]("B") shouldBe List(23.1, 1.4, 1.4, 7.0, 3.1)
    df1.toFlatList[String]("C") shouldBe List("ghi", "ABC", "XyZ", "qqq", "Uuu")
    df1.toFlatList[C1]("D") shouldBe List(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    df2.toFlatList[Int]("A") shouldBe List(6, 3, 8, 4)
    df2.toFlatList[Double]("B") shouldBe List(1.4, 1.4, 7.0, 3.1)
    df2.toFlatList[String]("C") shouldBe List("ghi", "ABC", "XyZ")
    df2.toFlatList[C1]("D") shouldBe List(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toFlatSeq[T](col: String): Seq[T]") {
    df1.toFlatSeq[Int]("A") shouldBe Seq(6, 3, 2, 8, 4)
    df1.toFlatSeq[Double]("B") shouldBe Seq(23.1, 1.4, 1.4, 7.0, 3.1)
    df1.toFlatSeq[String]("C") shouldBe Seq("ghi", "ABC", "XyZ", "qqq", "Uuu")
    df1.toFlatSeq[C1]("D") shouldBe Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    df2.toFlatSeq[Int]("A") shouldBe Seq(6, 3, 8, 4)
    df2.toFlatSeq[Double]("B") shouldBe Seq(1.4, 1.4, 7.0, 3.1)
    df2.toFlatSeq[String]("C") shouldBe Seq("ghi", "ABC", "XyZ")
    df2.toFlatSeq[C1]("D") shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
  }

  test("toList[T](col: String): List[Option[T]]") {
    df1.toList[Int]("A") shouldBe List(6, 3, 2, 8, 4).map(Some(_))
    df1.toList[Double]("B") shouldBe List(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    df1.toList[String]("C") shouldBe List("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    df1.toList[C1]("D") shouldBe List(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    df2.toList[Int]("A") shouldBe List(6, 3, null, 8, 4).map(Option(_))
    df2.toList[Double]("B") shouldBe List(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    df2.toList[String]("C") shouldBe List("ghi", "ABC", "XyZ", null, null).map(Option(_))
    df2.toList[C1]("D") shouldBe List(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toSeq[T](col: String): Seq[Option[T]]") {
    df1.toSeq[Int]("A") shouldBe Seq(6, 3, 2, 8, 4).map(Some(_))
    df1.toSeq[Double]("B") shouldBe Seq(23.1, 1.4, 1.4, 7.0, 3.1).map(Some(_))
    df1.toSeq[String]("C") shouldBe Seq("ghi", "ABC", "XyZ", "qqq", "Uuu").map(Some(_))
    df1.toSeq[C1]("D") shouldBe Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)).map(Some(_))

    df2.toSeq[Int]("A") shouldBe Seq(6, 3, null, 8, 4).map(Option(_))
    df2.toSeq[Double]("B") shouldBe Seq(null, 1.4, 1.4, 7.0, 3.1).map(Option(_))
    df2.toSeq[String]("C") shouldBe Seq("ghi", "ABC", "XyZ", null, null).map(Option(_))
    df2.toSeq[C1]("D") shouldBe Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).map(Option(_))
  }

  test("toString: String") {
    val s = df1.toString
    s should include("A")
    s should include("B")
    s should include("C")
    s should include("D")
    s should include("Index")
    s should include("Int")
    s should include("Double")
    s should include("String")
    s should include("8")
    s should include("23.1")
    s should include("XyZ")
    s should not include "null"
  }

  test(
    "toString(n: Int, width: Int, annotateIndex: Boolean, annotateType: Boolean, colWidth: Int, indexWidth: Int): String"
  ) {
    df1.toString(annotateIndex = false) should not include ("Index")
    df1.toString() should include("Index")
    df1.toString() should include("[Double]")
    df1.toString(annotateType = false) should not include "[Double]'"
    df2.toString should include("null")
    df2.toString() should include("[Double?]")
    df1.toString(n = 1) should include("1 of 5 rows")
    df1.toString(annotateIndex = false, width = 24) should include("1 of 4 columns")
    df1.toString(width = 24) should include("0 of 4 columns")
    df1.toString(annotateIndex = false, width = 42) should include("2 of 4 columns")
    df1.toString(width = 42) should include("1 of 4 columns")

    (25 to 40).foreach(w => df1.toString(width = 125, colWidth = w).split("\n").head.length should be <= 125)
  }

  test("union(df: DataFrame*): DataFrame") {
    df1.union(df2, df3) shouldBe DataFrame(
      "A" -> Series.union(s1A, s2A, s3A),
      "B" -> Series.union(s1B, s2B, s3B),
      "C" -> Series.union(s1C, s2C, s3C),
      "D" -> Series.union(s1D, s2D, s3D),
    )
    df1.union(df2(Seq("B", "D", "A", "C")), df3)
    df1.union() shouldBe df1
    df1.union(df2(Seq[Int]())) shouldBe df1
    assertThrows[ColumnNotFoundException](df1.union(df2(Seq("A", "B", "C")), df3))
    assertThrows[ColumnNotFoundException](df1.col(s1A as "E").union(df2))
    assertThrows[ColumnNotFoundException](df1.union(df2.col(s1A as "E")))
    assertThrows[ColumnNotFoundException](df1.union(df2, df3.col(s3A as "E")))
    assertThrows[SeriesCastException](df1.union(df2.col(s2C as "A")))
  }

  test("update[T](col: String, series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    val s1AX: Series[Int] = Series("X")(6, 3, 2, 8, 4)
    val s1DX: Series[C1] = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df.update("A", s1AX) shouldBe df
    df.update("D", s1DX) shouldBe df1
    df.update("B", s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df.update("A", s1AX(0 to 2)) shouldBe df
    df.update("A", s1ASliced) shouldBe df
    df2.update("A", s1AX).update("B", s1B).update("C", s1C).update("D", s1DX) shouldBe df1
    df1.update("A", s2ASliced).update("B", s2BSliced).update("C", s1CSliced).update("D", s1DSliced) shouldBe df1
    df1.update("A", Series("A")(1, null, 3, null, null)) shouldBe DataFrame(Series("A")(1, 3, 3, 8, 4), s1B, s1C, s1D)
    df2.update("C", Series("C")("1", null, null, "4", null)) shouldBe
      DataFrame(s2A, s2B, Series("C")("1", "ABC", "XyZ", "4", null), s2D)
    df3.update("A", Series(null, null, null, -2, -3).apply(Seq(0, 3, 4))) shouldBe
      df3 | Series("A")(6, null, 2, -2, -3).apply(Seq(0, 2, 3, 4))
    df.sortValues("A").update("B", s1B(0 to 2)) shouldBe df.sortValues("A")
    df.update("A", s1AX(Seq(3, 0, 1))) shouldBe df
    df.update("D", s1DX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A, s1B, s1C, s1D(Seq(0, 1, 3)))
    df.update("D", s1DX.sorted) shouldBe df1
    df.update("A", s1ASub) shouldBe df
    df.update("D", s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).update("A", s1AX))
    assertThrows[MergeIndexException](df(1 to 2).update("D", s1DX))
    assertThrows[MergeIndexException](df.apply(Seq(0, 2, 3, 4)).update("D", s1DX))
    assertThrows[MergeIndexException](df3.update("A", s1AX))
    assertThrows[SeriesCastException](df.update("A", s1B))
  }

  test("update[T](series: Series[T]): DataFrame") {
    val df = df1(Seq("A", "B", "C"))
    df.update(s1A) shouldBe df
    df.update(s1D) shouldBe df1
    df.update(s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df.update(s1A(0 to 2)) shouldBe df
    df.update(s1ASliced) shouldBe df
    df2.update(s1A).update(s1B).update(s1C).update(s1D) shouldBe df1
    df1.update(s2ASliced).update(s2BSliced).update(s1CSliced).update(s1DSliced) shouldBe df1
    df1.update(Series("A")(1, null, 3, null, null)) shouldBe DataFrame(Series("A")(1, 3, 3, 8, 4), s1B, s1C, s1D)
    df2.update(Series("C")("1", null, null, "4", null)) shouldBe
      DataFrame(s2A, s2B, Series("C")("1", "ABC", "XyZ", "4", null), s2D)
    df3.update(Series("A")(null, null, null, -2, -3).apply(Seq(0, 3, 4))) shouldBe
      df3 | Series("A")(6, null, 2, -2, -3).apply(Seq(0, 2, 3, 4))
    df.sortValues("A").update(s1B(0 to 2)) shouldBe df.sortValues("A")
    df.update(s1A(Seq(3, 0, 1))) shouldBe df
    df.update(s1D(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A, s1B, s1C, s1D(Seq(0, 1, 3)))
    df.update(s1D.sorted) shouldBe df1
    df.update(s1ASub) shouldBe df
    df.update(s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).update(s1A))
    assertThrows[MergeIndexException](df(1 to 2).update(s1D))
    assertThrows[MergeIndexException](df.apply(Seq(0, 2, 3, 4)).update(s1D))
    assertThrows[MergeIndexException](df3.update(s1A))
    assertThrows[SeriesCastException](df.update(s1B.as("A")))
  }

  test("update(series: Series[?]*): DataFrame") {
    val dfA = df1(Seq("A", "B"))
    dfA.update(s1A, s1B) shouldBe dfA
    dfA.update(s1C, s1D) shouldBe df1
    df2.update(s1A, s1B, s1C, s1D) shouldBe df1
    df1.update(s2ASliced, s2BSliced, s1CSliced, s1DSliced) shouldBe df1
    df2.update(Series("A")(1, null, 3, null, null), Series("C")("1", null, null, "4", null)) shouldBe
      DataFrame(Series("A")(1, 3, 3, 8, 4), s2B, Series("C")("1", "ABC", "XyZ", "4", null), s2D)
    assertThrows[MergeIndexException](dfA(1 to 2).update(s1A, s1D))
    assertThrows[MergeIndexException](df3.update(s1A, s1D))
    assertThrows[SeriesCastException](dfA.update(s1B.as("A"), s1D))
  }

  test("valueCounts(cols: String*): DataFrame") {
    gf1.valueCounts("A") shouldBe DataFrame(
      "A" -> Series("b", "a", null),
      "count" -> Series(4, 3, 1),
    )
    gf1.valueCounts("A", "C").sortValues("count" -> Order.desc, "A" -> Order.asc).resetIndex shouldBe DataFrame(
      "A" -> Series("b", "a", "a", null),
      "C" -> Series(true, true, false, false),
      "count" -> Series(4, 2, 1, 1),
    )
  }

  test(
    "valueCounts(cols: Seq[String], countCol: String, dropUndefined: Boolean, order: Order, asFraction: Boolean): DataFrame"
  ) {
    gf1.valueCounts(Seq("A")) shouldBe DataFrame(
      "A" -> Series("b", "a", null),
      "count" -> Series(4, 3, 1),
    )
    gf1.valueCounts(Seq("A"), order = Order.asc) shouldBe DataFrame(
      "A" -> Series(null, "a", "b"),
      "count" -> Series(1, 3, 4),
    )
    gf1.valueCounts(Seq("A", "C")).sortValues("count" -> Order.desc, "A" -> Order.asc).resetIndex shouldBe DataFrame(
      "A" -> Series("b", "a", "a", null),
      "C" -> Series(true, true, false, false),
      "count" -> Series(4, 2, 1, 1),
    )
    gf1.valueCounts(Seq("A", "C"), countCol = "counts", dropUndefined = true) shouldBe DataFrame(
      "A" -> Series("b", "a", "a"),
      "C" -> Series(true, true, false),
      "counts" -> Series(4, 2, 1),
    )
    gf1
      .valueCounts(Seq("A", "C"), countCol = "fraction", dropUndefined = false, asFraction = true)
      .sortValues("fraction" -> Order.desc, "A" -> Order.asc)
      .resetIndex shouldBe DataFrame(
      "A" -> Series("b", "a", "a", null),
      "C" -> Series(true, true, false, false),
      "fraction" -> Series(4 / 8.0, 2 / 8.0, 1 / 8.0, 1 / 8.0),
    )
    gf1.valueCounts(Seq("A", "C"), dropUndefined = true, asFraction = true) shouldBe DataFrame(
      "A" -> Series("b", "a", "a"),
      "C" -> Series(true, true, false),
      "count" -> Series(4 / 8.0, 2 / 8.0, 1 / 8.0),
    )
  }

  // *** PRIVATE ***

  test("undefinedIndices: Array[Int]") {
    df1.undefinedIndices shouldBe Array[Int]()
    df2.undefinedIndices.sorted shouldBe Array(0, 1, 2, 3, 4)
    df3.undefinedIndices.sorted shouldBe Array(2, 3)
    DataFrame.empty.undefinedIndices shouldBe Array[Int]()
  }

  test("undefinedIndices(cols: String*): Array[Int]") {
    df1.undefinedIndices("A", "B", "C", "D") shouldBe Array[Int]()
    df2.undefinedIndices("A", "B", "C", "D").sorted shouldBe Array(0, 1, 2, 3, 4)
    df3.undefinedIndices("D", "B", "C", "A").sorted shouldBe Array(2, 3)
    df1.undefinedIndices("A", "C", "D") shouldBe Array[Int]()
    df2.undefinedIndices("A", "C", "D").sorted shouldBe Array(1, 2, 3, 4)
    df3.undefinedIndices("D", "C", "A") shouldBe Array(2)
    df1.undefinedIndices("C") shouldBe Array[Int]()
    df2.undefinedIndices("C").sorted shouldBe Array(3, 4)
    df3.undefinedIndices("C") shouldBe Array(2)
    assertThrows[ColumnNotFoundException](df1.undefinedIndices("X"))
    assertThrows[ColumnNotFoundException](df1.undefinedIndices("A", "X"))
  }

  // *** TRAIT INDEXOPS ***

  test("apply(range: Range): V") {
    // see pd.index.*IndexTest for more tests
    df1(1 to 2)("A").toFlatSeq shouldBe Seq(3, 2)
    df1(1 to 2)("A").apply(Seq(2, 5)).toFlatSeq shouldBe Seq(2)
    df1(Seq(2, 4)).apply(1 until 4)("A").toFlatSeq shouldBe Seq(2)
    df1(1 to 4).apply(Seq(4, 2, 0)).apply("A").toFlatSeq shouldBe Seq(4, 2)
    df1(1 to 4).apply(Seq(0, 2, 4)).apply("A").toFlatSeq shouldBe Seq(2, 4)
    df1(Seq(0, 4, 2)).apply(1 to 4).apply("A").toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](df1(0 to 10))
  }

  test("apply(seq: Seq[Int]): V") {
    // see pd.index.*IndexTest for more tests
    df1(Seq(1, 2))("A").toFlatSeq shouldBe Seq(3, 2)
    df1(Seq(2, 4))("A").apply(1 to 2).toFlatSeq shouldBe Seq(2)
    df1(Seq(2, 4))("A").apply(1 until 4).toFlatSeq shouldBe Seq(2)
    df1(1 to 4).apply(Seq(4, 2, 0)).apply("A").toFlatSeq shouldBe Seq(4, 2)
    df1(1 to 4).apply(Seq(0, 2, 4)).apply("A").toFlatSeq shouldBe Seq(2, 4)
    df1(Seq(0, 4, 2)).apply(1 to 4).apply("A").toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](df1(Seq(2, 10)))
  }

  test("apply(array: Array[Int]): V") {
    // see pd.index.*IndexTest for more tests
    df1(Array(1, 2))("A").toFlatSeq shouldBe Seq(3, 2)
    df1(Array(2, 4)).apply(1 to 2)("A").toFlatSeq shouldBe Seq(2)
    df1(Array(2, 4)).apply(1 until 4)("A").toFlatSeq shouldBe Seq(2)
    df1(1 to 4).apply(Array(4, 2, 0)).apply("A").toFlatSeq shouldBe Seq(4, 2)
    df1(1 to 4).apply(Array(0, 2, 4)).apply("A").toFlatSeq shouldBe Seq(2, 4)
    df1(Array(0, 4, 2)).apply(1 to 4).apply("A").toFlatSeq shouldBe Seq(2, 4)
    assertThrows[IllegalIndex](df1(Array(2, 10)))
  }

  test("series: Series[Boolean]): V") {
    // see pd.index.*IndexTest for more tests
    df1(maskA).A.toFlatSeq shouldBe Seq(6, 3, 8, 4)
    df1(maskB).B.toFlatSeq shouldBe Seq(1.4, 1.4, 7.0, 3.1)
    df1(maskC).C.toFlatSeq shouldBe Seq("ghi", "ABC", "XyZ")
    df1(maskD).D.toFlatSeq shouldBe Seq(C1("A", 5), C1("C", 2), C1("D", 2), C1("E", 0))
    df1(0 to 3).apply(maskA)("A").toFlatSeq shouldBe Seq(6, 3, 8)
    df1(Seq(2, 3))(maskB)("B").toFlatSeq shouldBe Seq(1.4, 7.0)
    df1(maskC).apply(1 to 3)("C").toFlatSeq shouldBe Seq("ABC", "XyZ")
    df1(maskD).apply(Seq(1, 2))("D").toFlatSeq shouldBe Seq(C1("C", 2))
  }

  test("head(n: Int): V") {
    // see pd.index.*IndexTest for more tests
    df1.head(2).A shouldBe s1A(Seq(0, 1))
    df1.head(5).B shouldBe s1B(0 to 4)
    df3.head(2).A shouldBe s3A(Seq(0, 2))
    df3.head(4).A shouldBe s3A
    df1.head(0).D shouldBe s1D(Seq())
    df1.sorted[Int]("A").head(1).A shouldBe s1A(Seq(2))
    df1.sorted[Int]("A").head(2).A shouldBe s1A(Seq(2, 1))
    df1.sorted[Int]("A").head(5).A shouldBe s1A.sorted
    df1.sorted[Int]("A").head(10).A shouldBe s1A.sorted
  }

  test("sortIndex: Series[T]") {
    df1.sortIndex.A shouldBe s1A
    df1.sortIndex.B shouldBe s1B
    df1.sortIndex.C shouldBe s1C
    df1.sortIndex.D shouldBe s1D
    df3.sortIndex.A shouldBe s3A
    df3.sortIndex.B shouldBe s3B
    df3.sortIndex.C shouldBe s3C
    df3.sortIndex.D shouldBe s3D

    df1.sorted[Int]("A").sortIndex.A shouldBe s1A
    df1.sorted[Double]("B" -> Order.desc).sortIndex.B shouldBe s1B
    df1.sorted[String]("C").sortIndex.C shouldBe s1C
    df1.sorted[C1]("D").sortIndex.D shouldBe s1D
    df3.sorted[Int]("A").sortIndex.A shouldBe s3A
    df3.sorted[Double]("B" -> Order.desc).sortIndex.B shouldBe s3B
    df3.sorted[Double]("B" -> Order.ascNullsFirst).sortIndex.C shouldBe s3C
    df3.sorted[C1]("D").sortIndex.D shouldBe s3D
  }

  test("tail(n: Int): V") {
    // see pd.index.*IndexTest for more tests
    s1A.tail(2) shouldBe s1A(Seq(3, 4))
    s1B.tail(5) shouldBe s1B(0 to 4)
    df3.tail(2).C shouldBe s3C(3 to 4)
    df3.tail(4).C shouldBe s3C
    df1.tail(0).D shouldBe s1D(Seq())
    df1.sorted[Int]("A").tail(1).A shouldBe s1A(Seq(3))
    df1.sorted[Int]("A").tail(2).A shouldBe s1A(Seq(0, 3))
    df1.sorted[Int]("A").tail(5).A shouldBe s1A.sorted
    df1.sorted[Int]("A").tail(10).A shouldBe s1A.sorted
  }

  // ** OBJECT **

  test("DataFrame.applyDynamic(method: String)(args: ((String, Series[?]) | Series[?])*): DataFrame") {

    { // apply

      df1("A") shouldBe s1A
      df1("B") shouldBe s1B
      df1("C") shouldBe s1C
      df1("D") shouldBe s1D
      df1.requires.all.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      df2("A") shouldBe s2A
      df2("B") shouldBe s2B
      df2("C") shouldBe s2C
      df2("D") shouldBe s2D
      df2.requires.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")
      assertThrows[RequirementException](df2.requires.all.isType[Int]("A"))
      assertThrows[RequirementException](df2.requires.all.isType[Double]("B"))
      assertThrows[RequirementException](df2.requires.all.isType[String]("C"))
      assertThrows[RequirementException](df2.requires.all.isType[C1]("D"))

      val dfFrom1 = DataFrame(s1A, s1B, s1C, s1D)
      dfFrom1 shouldBe df1
      dfFrom1.requires.all.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      val dfFrom2 = DataFrame(s2A, s2B, s2C, s2D)
      dfFrom2 shouldBe df2
      dfFrom2.requires.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      assertThrows[BaseIndexException](DataFrame(s2A, s2B, s2COtherBase, s2D))

      val e3 = Series("E")(1, 2, 3, 4)
      assertThrows[BaseIndexException](DataFrame(s1A, s1B(Seq(0, 4)), s2C(1 to 3), Series("D")(1, 2, 3, 4), e3(1 to 2)))

      val e4 = Series("E")(1, 2, 3)
      assertThrows[BaseIndexException](DataFrame(s1A, s1B(Seq(0, 4)), s2C(1 to 3), Series("D")(1, 2, 3), e4(1 to 2)))

      val dfFromEmpty = DataFrame(sEmptyA, sEmptyB)
      dfFromEmpty shouldBe dfEmpty
      dfFromEmpty.requires.all.strictly.isType[Int]("A").isType[Double]("B")

      assertThrows[BaseIndexException](DataFrame(s1A, sEmptyB))
    }

    { // from

      val dfFrom1 = DataFrame.from(s1A, s1B, s1C, s1D)
      dfFrom1 shouldBe df1
      dfFrom1.requires.all.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      val dfFrom2 = DataFrame.from(s2A, s2B, s2C, s2D)
      dfFrom2 shouldBe df2
      dfFrom2.requires.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      val dfFrom2b = DataFrame.from(s2A, s2B, s2COtherBase, s2D)
      dfFrom2b shouldBe df2
      dfFrom2b.requires.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

      val e3 = Series("E")(1, 2, 3, 4)
      val dfFrom3 = DataFrame.from(s1A, s1B(Seq(0, 4)), s2C(1 to 3), Series("D")(1, 2, 3, 4), e3(1 to 2))
      val expected3 = DataFrame(
        "A" -> Series(6, 3, 2, 8, 4),
        "B" -> Series(23.1, null, null, null, 3.1),
        "C" -> Series(null, "ABC", "XyZ", null, null),
        "D" -> Series(1, 2, 3, 4, null),
        "E" -> Series(null, 2, 3, null, null),
      )
      dfFrom3 shouldBe expected3

      val e4 = Series("E")(1, 2, 3)
      val dfFrom4 = DataFrame.from(s1A, s1B(Seq(0, 4)), s2C(1 to 3), Series("D")(1, 2, 3), e4(1 to 2))
      val expected4 = DataFrame(
        "A" -> Seq(6, 3, 2, 8, 4),
        "B" -> Series(23.1, null, null, null, 3.1),
        "C" -> Series(null, "ABC", "XyZ", null, null),
        "D" -> Series(1, 2, 3, null, null),
        "E" -> Series(null, 2, 3, null, null),
      )
      dfFrom4 shouldBe expected4
      dfFrom4.requires.strictly
        .isType[Int]("A", true)
        .isType[Double]("B")
        .isType[String]("C")
        .isType[Int]("D")
        .isType[Int]("E")

      val dfFromEmpty = DataFrame.from(sEmptyA, sEmptyB)
      dfFromEmpty shouldBe dfEmpty
      dfFromEmpty.requires.all.strictly.isType[Int]("A").isType[Double]("B")

      val dfFromHalfEmpty = DataFrame.from(s1A, sEmptyB)
      dfFromHalfEmpty shouldBe dfHalfEmpty
      dfFromHalfEmpty.requires.strictly.isType[Int]("A", true).isType[Double]("B")

      DataFrame.from(
        "A" -> Series(6, 3, null, 8, 4),
        "B" -> Series(null, 1.4, 1.4, 7.0, 3.1),
        "C" -> Series("ghi", "ABC", "XyZ"),
        "D" -> Series(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)),
      ) shouldBe df2
    }

  }

  test("DataFrame.applyDynamicNamed(method: String)(kwargs: (String, Series[?])*): DataFrame") {

    // apply

    val df1: DataFrame = DataFrame(
      A = Seq(6, 3, 2, 8, 4),
      B = Seq(23.1, 1.4, 1.4, 7.0, 3.1),
      C = Seq("ghi", "ABC", "XyZ", "qqq", "Uuu"),
      D = Seq(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)),
    )
    df1("A") shouldBe s1A
    df1("B") shouldBe s1B
    df1("C") shouldBe s1C
    df1("D") shouldBe s1D
    df1.requires.all.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

    val df2Seq: DataFrame = DataFrame(
      A = Seq(6, 3, null, 8, 4),
      `B` = Seq(null, 1.4, 1.4, 7.0, 3.1),
      `C` = Seq("ghi", "ABC", "XyZ", null, null),
      D = Seq(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)),
    )
    df2Seq("A") shouldBe s2A.map(_.asInstanceOf[Any])
    df2Seq("B") shouldBe s2B.map(_.asInstanceOf[Any])
    df2Seq("C") shouldBe s2C
    df2Seq("D") shouldBe s2D
    assertThrows[RequirementException](df2Seq.requires.strictly.isType[Int]("A"))
    assertThrows[RequirementException](df2Seq.requires.strictly.isType[Double]("B"))
    assertThrows[RequirementException](df2Seq.requires.all.strictly.isType[String]("C"))
    assertThrows[RequirementException](df2Seq.requires.all.strictly.isType[C1]("D"))
    df2Seq.requires.isType[Any]("A").isType[Any]("B").isType[String]("C").isType[C1]("D")

    // from

    DataFrame.from(
      A = Series(6, 3, null, 8, 4),
      B = Series(null, 1.4, 1.4, 7.0, 3.1),
      C = Series("ghi", "ABC", "XyZ"),
      D = Series(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)),
    ) shouldBe df2

  }

  test("DataFrame.io: ReadAdapter") {
    // see ReadAdapter
  }

  test("DataFrame.union(df: DataFrame*): DataFrame") {
    DataFrame.union(df1, df2, df3) shouldBe DataFrame(
      "A" -> Series.union(s1A, s2A, s3A),
      "B" -> Series.union(s1B, s2B, s3B),
      "C" -> Series.union(s1C, s2C, s3C),
      "D" -> Series.union(s1D, s2D, s3D),
    )
    DataFrame.union(df1, df2(Seq("B", "D", "A", "C")), df3)
    DataFrame.union(df1) shouldBe df1
    DataFrame.union(df1, df2(Seq[Int]())) shouldBe df1
    assertThrows[IllegalOperation](DataFrame.union())
    assertThrows[ColumnNotFoundException](DataFrame.union(df1, df2(Seq("A", "B", "C")), df3))
    assertThrows[ColumnNotFoundException](DataFrame.union(df1.col(s1A as "E"), df2))
    assertThrows[ColumnNotFoundException](DataFrame.union(df1, df2.col(s1A as "E")))
    assertThrows[ColumnNotFoundException](DataFrame.union(df1, df2, df3.col(s3A as "E")))
    assertThrows[SeriesCastException](DataFrame.union(df1, df2.col(s2C as "A")))
  }

  // ** PRIVATE **

  test("DataFrame.create(colIndex: ColIndex, index: BaseIndex): DataFrame") {
    // tested indirectly
  }

  test("DataFrame.empty: DataFrame") {
    DataFrame.empty.numRows shouldBe 0
    DataFrame.empty.numCols shouldBe 0
    DataFrame.empty.columns shouldBe Seq()
    DataFrame.empty.columnIterator.isEmpty shouldBe true
    DataFrame.empty.toString should include("Index")
  }

  test("DataFrame.fromSeries(series: Series[?])") {
    // tested indirectly
  }

  test("DataFrame.fromSeries(columns: Seq[Series[?]], forced: Boolean): DataFrame") {
    // see also applyDynamic and applyDynamicNamed
    DataFrame.fromSeries(Seq(), false).requires.hasNumCols(0).hasNumRows(0)

    // first strategy: same indices
    DataFrame
      .fromSeries(Seq(s1A, s1B, s1C, s1D), forced = false)
      .requires
      .hasNumCols(4)
      .hasNumRows(5)
      .equalsCol(s1A)
      .equalsCol(s1B)
      .equalsCol(s1C)
      .equalsCol(s1D)
    DataFrame
      .fromSeries(Seq(s3A, s3B, s3C, s3D), forced = false)
      .requires
      .hasNumCols(4)
      .hasNumRows(4)
      .equalsCol(s3A)
      .equalsCol(s3B)
      .equalsCol(s3C)
      .equalsCol(s3D)

    // second strategy: join arbitrary [[UniformIndex]] (only with force = true)
    assertThrows[BaseIndexException](DataFrame.fromSeries(Seq(s1A, Series(1.0, 2.0), s1C), forced = false))
    DataFrame
      .fromSeries(Seq(s1A, Series(1.0, 2.0) as "X", s2C), forced = true)
      .requires
      .hasNumCols(3)
      .hasNumRows(5)
      .equalsCol(s1A)
      .equalsCol("X", Series(1.0, 2.0, null, null, null))
      .equalsCol(s2C)

    // third strategy: union indices if base indices are the same
    DataFrame
      .fromSeries(Seq(s1ASorted, s1B, s2CSliced, s3D), forced = false)
      .requires
      .hasNumCols(4)
      .hasNumRows(5)
      .equalsCol(s1A)
      .equalsCol(s1B)
      .equalsCol(s2C)
      .equalsCol(Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)))

    // forth strategy: union indices regardless (only with force = true)
    assertThrows[BaseIndexException](
      DataFrame.fromSeries(Seq(s1ASorted, s1B, Series("X")(1.0, 2.0, 3.0), s2CSliced, s3D), forced = false)
    )
    DataFrame
      .fromSeries(Seq(s1ASorted, s1B, Series("X")(1.0, 2.0, 3.0), s2CSliced, s3D), forced = true)
      .requires
      .hasNumCols(5)
      .hasNumRows(5)
      .equalsCol(s1B)
      .equalsCol(s1B)
      .equalsCol("X", Series(1.0, 2.0, 3.0, null, null))
      .equalsCol(s2C)
      .equalsCol(Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)))
    DataFrame
      .fromSeries(Seq(s3A, s3B, Series("X")(1.0, 2.0, 3.0, 4.0, 5.0, 6.0).apply(Seq(5))), forced = true)
      .display()
    DataFrame
      .fromSeries(Seq(s3A, s3B, Series("X")(1.0, 2.0, 3.0, 4.0, 5.0, 6.0).apply(Seq(5))), forced = true)
      .requires
      .hasNumCols(3)
      .hasNumRows(5)
      .equalsCol(Series("A")(6, null, 2, 8, 4, null).apply(Seq(0, 2, 3, 4, 5)))
      .equalsCol(Series("B")(23.1, null, 1.4, null, 3.1, null).apply(Seq(0, 2, 3, 4, 5)))
      .equalsCol(Series("X")(null, null, null, null, null, 6.0).apply(Seq(0, 2, 3, 4, 5)))

  }

  // ** DATA FRAME APPENDER **

  test("DataFrameAppender.applyDynamic(method: String)(args: ((String, Series[?]) | Series[?])*): DataFrame") {
    { // examples
      val df = DataFrame(Series(1, 2) as "position")
      df.cols(Series(10.0, 20.0) as "price", Series(5, 2) as "quantity")
      df.cols(Series("price")(10.0, 20.0), Series("quantity")(5, 2))
      df.cols("price" -> Series(10.0, 20.0), "quantity" -> Series(5, 2))
    }

    val df = df1(Seq("A", "B", "C"))
    val dfB = df1(Seq("B"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    // Mixed arguments
    df.cols() shouldBe df
    df.cols(s1D, "X" -> s1A) shouldBe DataFrame(s1A, s1B, s1C, s1D, s1A as "X")
    df.cols("X" -> s1A, s1D, "y " -> s1C) shouldBe DataFrame(s1A, s1B, s1C, s1A as "X", s1D, s1C as "y ")
    dfB.cols("D" -> s1DX, "A" -> s1AX, s1C) shouldBe DataFrame(s1B, s1D, s1A, s1C)

    // (String, Series[?])*
    df.cols("A" -> s1A, "A" -> s1A) shouldBe df
    dfB.cols("D" -> s1DX, "A" -> s1AX, "C" -> s1C) shouldBe DataFrame(s1B, s1D, s1A, s1C)
    df.cols("A" -> s1AX) shouldBe df
    df.cols("D" -> s1DX) shouldBe df1
    df.cols("X" -> s2A) shouldBe DataFrame(s1A, s1B, s1C, s2A as "X")
    df.cols("B" -> s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df1.cols("A" -> s1AX(0 to 2)) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A").cols("B" -> s1B(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1.cols("A" -> s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1.cols("A" -> s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df.cols("D" -> s1DX.sorted) shouldBe df1
    df.cols("A" -> s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df.cols("B" -> s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df.cols("X" -> s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced as "X")
    assertThrows[MergeIndexException](df(1 to 2).cols("A" -> s1AX))
    assertThrows[MergeIndexException](df(1 to 2).cols("C" -> s2C))
    assertThrows[MergeIndexException](df(1 to 2).cols("C" -> s2C(1 to 2), "A" -> s1A))

    // Series[?]*
    df.cols(s1A, s1A) shouldBe df
    dfB.cols(s1D, s1A, s1C) shouldBe DataFrame(s1B, s1D, s1A, s1C)
    df.cols(s1A) shouldBe df
    df.cols(s1D) shouldBe df1
    df.cols(s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df1.cols(s1A(0 to 2)) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A").cols(s1B(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1.cols(s1A(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1.cols(s1A(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df.cols(s1D.sorted) shouldBe df1
    df.cols(s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df.cols(s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df.cols(s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced)
    assertThrows[MergeIndexException](df(1 to 2).cols(s1A))
    assertThrows[MergeIndexException](df(1 to 2).cols(s2C))
    assertThrows[MergeIndexException](df(1 to 2).cols(s2C(1 to 2), s1A))
  }

  test("DataFrameAppender.applyDynamicNamed(method: String)(kwargs: (String, Series[?])*): DataFrame") {
    { // examples
      val df = DataFrame(Series(1, 2) as "position")
      df.cols(price = Series(10.0, 20.0), quantity = Series(5, 2))
    }

    val df = df1(Seq("A", "B", "C"))
    val dfB = df1(Seq("B"))
    val s1AX = Series("X")(6, 3, 2, 8, 4)
    val s1DX = Series("X")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))

    df.cols(D = s1D, X = s1A) shouldBe DataFrame(s1A, s1B, s1C, s1D, s1A as "X")
    df.cols(X = s1A, D = s1D, `y ` = s1C) shouldBe DataFrame(s1A, s1B, s1C, s1A as "X", s1D, s1C as "y ")
    dfB.cols(D = s1DX, A = s1AX, C = s1C) shouldBe DataFrame(s1B, s1D, s1A, s1C)
    df.cols(A = s1A, A = s1A) shouldBe df
    dfB.cols(D = s1DX, A = s1AX, C = s1C) shouldBe DataFrame(s1B, s1D, s1A, s1C)
    df.cols(A = s1AX) shouldBe df
    df.cols(D = s1DX) shouldBe df1
    df.cols(X = s2A) shouldBe DataFrame(s1A, s1B, s1C, s2A as "X")
    df.cols(B = s1BVar) shouldBe DataFrame(s1A, s1BVar, s1C)
    df1.cols(A = s1AX(0 to 2)) shouldBe DataFrame.from(s1A(0 to 2), s1B, s1C, s1D)
    df1.sortValues("A").cols(B = s1B(0 to 2)) shouldBe DataFrame.from(s1A, s1B(0 to 2), s1C, s1D).sortValues("A")
    df1.cols(A = s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df1.cols(A = s1AX(Seq(3, 0, 1))) shouldBe DataFrame.from(s1A(Seq(0, 1, 3)), s1B, s1C, s1D)
    df.cols(D = s1DX.sorted) shouldBe df1
    df.cols(A = s1ASub) shouldBe DataFrame.from(s1ASub, s1B, s1C)
    df.cols(B = s1BSub) shouldBe DataFrame.from(s1A, s1BSub, s1C)
    df.cols(X = s1DSliced) shouldBe DataFrame.from(s1A, s1B, s1C, s1DSliced as "X")
    assertThrows[MergeIndexException](df(1 to 2).cols(A = s1AX))
    assertThrows[MergeIndexException](df(1 to 2).cols(C = s2C))
    assertThrows[MergeIndexException](df(1 to 2).cols(C = s2C(1 to 2), A = s1A))
  }
