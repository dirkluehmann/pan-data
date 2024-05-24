/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import org.scalactic.source
import org.scalatest.{Sequential, SequentialNestedSuiteExecution, Tag}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
  * @since 0.1.0
  */
class BaseTest extends AnyFunSuite with Matchers with SequentialNestedSuiteExecution:

  Settings.setTestDefaults()

  /**
    * Test with single and multi core settings.
    *
    * @param testName
    *   Base test name.
    * @param testTags
    *   Tags.
    * @param testFun
    *   Test function.
    * @since 0.1.0
    */
  inline def testMulti(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */ ): Unit = {
    test(s"$testName @ single core", testTags: _*)({
      Settings.setTestSingleCore()
      testFun
      Settings.setTestDefaults()
    })
    test(s"$testName @ multi core", testTags: _*) {
      Settings.setTestMultiCore()
      testFun
      Settings.setTestDefaults()
    }
  }

  // *** DEFAULT DATA *** //

  case class C1(s: String, i: Int) extends Ordered[C1] {
    override def compare(that: C1): Int = i.compare(that.i) match
      case 0 => s.compare(that.s)
      case v => v
  }

  class C2 extends C1("A", 0)

  val seriesRowLength: Int = 5
  val df1: DataFrame = DataFrame(
    "A" -> Series(6, 3, 2, 8, 4),
    "B" -> Series(23.1, 1.4, 1.4, 7.0, 3.1),
    "C" -> Series("ghi", "ABC", "XyZ", "qqq", "Uuu"),
    "D" -> Series(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0)),
  )

  val s1A: Series[Int] = Series("A")(6, 3, 2, 8, 4)
  val s1B: Series[Double] = Series("B")(23.1, 1.4, 1.4, 7.0, 3.1)
  val s1C: Series[String] = Series("C")("ghi", "ABC", "XyZ", "qqq", "Uuu")
  val s1D: Series[C1] = Series("D")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
  val series1: Seq[Series[Any]] = Seq(s1A, s1B, s1C, s1D).map(_.asAny)
  val series1Sorted: Seq[Series[Any]] = Seq(s1A.sorted, s1B.sorted, s1C.sorted, s1D.sorted).map(_.asAny)
  val s1ASorted: Series[Int] = s1A.sorted
  val s1ASub: Series[Int] = s1A(Seq(0, 2, 3))
  val s1BSorted: Series[Double] = s1B.sorted
  val s1BSub: Series[Double] = s1B(Seq(0, 2, 3))

  // Variation of series in its last element
  val s1AVar: Series[Int] = Series("A")(6, 3, 2, 8, 3)
  val s1BVar: Series[Double] = Series("B")(23.1, 1.4, 1.4, 7.0, 3.2)
  val s1CVar: Series[String] = Series("C")("ghi", "ABC", "XyZ", "qqq", "UUU")
  val s1DVar: Series[C1] = Series("D")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("F", 0))
  val series1Var: Seq[Series[?]] = Seq(s1AVar, s1BVar, s1CVar, s1DVar)
  val s1DOfC2: Series[C2] = Series("D")(C2(), C2(), C2(), C2(), C2())

  val df2: DataFrame = DataFrame(
    "A" -> Series(6, 3, null, 8, 4),
    "B" -> Series(null, 1.4, 1.4, 7.0, 3.1),
    "C" -> Series("ghi", "ABC", "XyZ", null, null),
    "D" -> Series(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)),
  )

  val s2A: Series[Int] = Series("A")(6, 3, null, 8, 4)
  val s2B: Series[Double] = Series("B")(null, 1.4, 1.4, 7.0, 3.1)
  val s2C: Series[String] = Series("C")("ghi", "ABC", "XyZ", null, null)
  val s2COtherBase: Series[String] = Series("C")("ghi", "ABC", "XyZ")
  val s2D: Series[C1] = Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))
  val series2: Seq[Series[Any]] = Seq(s2A, s2B, s2C, s2D).map(_.asAny)

  val maskA: Seq[Boolean] = Seq(true, true, false, true, true)
  val maskB: Seq[Boolean] = Seq(false, true, true, true, true)
  val maskC: Seq[Boolean] = Seq(true, true, true, false, false)
  val maskD: Seq[Boolean] = Seq(true, false, true, true, true)
  val s1ASliced: Series[Int] = s1A(maskA)
  val s1BSliced: Series[Double] = s1B(maskB)
  val s1CSliced: Series[String] = s1C(maskC)
  val s1DSliced: Series[C1] = s1D(maskD)
  val series1Sliced: Seq[Series[Any]] = Seq(s1ASliced, s1BSliced, s1CSliced, s1DSliced).map(_.asAny)
  val s2ASliced: Series[Int] = s2A(maskA)
  val s2BSliced: Series[Double] = s2B(maskB)
  val s2CSliced: Series[String] = s2C(maskC)
  val s2DSliced: Series[C1] = s2D(maskD)
  val series2Sliced: Seq[Series[Any]] = Seq(s2ASliced, s2BSliced, s2CSliced, s2DSliced).map(_.asAny)

  val df3: DataFrame = DataFrame(
    "A" -> Series(6, null, 2, 8, 4),
    "B" -> Series(23.1, null, 1.4, null, 3.1),
    "C" -> Series("ghi", null, null, "qqq", "Uuu"),
    "D" -> Series(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)),
  ).apply(Seq(0, 2, 3, 4))

  val s3A: Series[Int] = Series("A")(6, null, 2, 8, 4).apply(Seq(0, 2, 3, 4))
  val s3B: Series[Double] = Series("B")(23.1, null, 1.4, null, 3.1).apply(Seq(0, 2, 3, 4))
  val s3C: Series[String] = Series("C")("ghi", null, null, "qqq", "Uuu").apply(Seq(0, 2, 3, 4))
  val s3D: Series[C1] = Series("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0)).apply(Seq(0, 2, 3, 4))

  val gf1 = DataFrame(
    "A" -> Series("a", "a", "a", null, "b", "b", "b", "b"),
    "B" -> Series(1, 2, null, 1, 2, 3, 1, 2),
    "C" -> Series(true, true, false, false, true, true, true, true),
    "D" -> Series(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0),
    "E" -> Series(0, null, 1, 2, null, 3, 4, 5),
  )

  val gf2 = DataFrame(
    "A" -> Series("a", "a", "b", "b", "c", "c", "a", "a"),
    "B" -> Series(1, 2, 3, 1, 2, null, 1, 2),
    "C" -> Series(true, true, true, false, true, true, true, true),
    "E" -> Series(10, 11, 12, 13, 14, 15, 16, 17),
    "X" -> Series(1, 2, 3, 4, 5, 6, 7, 8),
    "Y" -> Series("A", "B", null, "D", "E", "F", "G", null),
  )

  val gfDouble = DataFrame(
    "A" -> Series(
      1.0,
      Double.NaN,
      null,
      1.0,
      2.1,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      2.1,
      Double.NaN,
      Double.NaN,
      Double.PositiveInfinity,
      2.1,
    ),
    "B" -> Series("a", "a", "a", null, "b", "b", "b", "b", "c", "c", "c", "c"),
    "E" -> Series(0, null, 1, 2, null, 3, 4, 5, 6, 7, 8, 9),
  )

  val dfEmpty: DataFrame = DataFrame(
    "A" -> Seq[Int](),
    "B" -> Seq[Double](),
  )
  val dfHalfEmpty: DataFrame = DataFrame(
    "A" -> Series(6, 3, 2, 8, 4),
    "B" -> Series[Double](null, null, null, null, null),
  )
  val sEmptyA: Series[Int] = Series("A")()
  val sEmptyB: Series[Double] = Series("B")()
