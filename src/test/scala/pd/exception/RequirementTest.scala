/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.exception.RequirementException
import pd.{BaseTest, DataFrame, Series}

class RequirementTest extends BaseTest:

  case class C3(s: String)

  private val df3a = DataFrame(s1A, s1A as "A1", s1C, s1C as "C 1", s1C as "c-2")
  private val df3b = DataFrame(s1A, s2A as "A1", s2C, s2C as "C 1", s2C as "c-2")
  private val df4a = DataFrame(Series(C2(), "A", 1.0) as "A", Series(C3("A"), 23, 1.0) as "B")
  private val df4b = DataFrame(Series(C2(), null, "A", 1.0) as "A", Series(C3("A"), 23, null, 1.0) as "B")

  test("<mixed usage>") {
    df1.requires
      .isType[Int]("A", true)
      .check[Int]("A", _ > 0)
      .has("B", "A")
      .all
      .isType[String]("C")
      .strictly
      .isType[C1]("D")
  }

  test("check[T](col: String, condition: T => Boolean, all: Boolean): Requirement") {
    df1.requires.check[Double]("B", _ >= 1.0)
    df2.requires.check[Double]("B", _ >= 1.0)
    df2.requires.check[String]("C", _.nonEmpty)
    assertThrows[RequirementException](df1.requires.check[Int]("A", _ < 1))
    assertThrows[RequirementException](df1.requires.check[String]("B", _ == "failed"))
  }

  test("checkAll[T](col: String, condition: T => Boolean): Requirement") {
    df1.requires.checkAll[Double]("B", _ >= 1.0)
    assertThrows[RequirementException](df1.requires.checkAll[Double]("B", _ >= 2.0))
    assertThrows[RequirementException](df2.requires.checkAll[Double]("B", _ >= 1.0))
    assertThrows[RequirementException](df2.requires.checkAll[String]("C", _.nonEmpty))

    df1.requires.all.check[Double]("B", _ >= 1.0)
    assertThrows[RequirementException](df1.requires.all.check[Double]("B", _ >= 2.0))
    assertThrows[RequirementException](df2.requires.all.check[Double]("B", _ >= 1.0))
    assertThrows[RequirementException](df2.requires.all.check[String]("C", _.nonEmpty))
    assertThrows[RequirementException](df2.requires.all.checkAll[String]("C", _.nonEmpty))
  }

  test("equals[T](col: String, series: Series[T]): Requirement") {
    df1.requires.equalsCol("A", s1A).equalsCol("B", s1B.as("")).equalsCol("C", s1C.as("col")).equalsCol("D", s1D)
    df2.requires.equalsCol("A", s2A).equalsCol("B", s2B.as("")).equalsCol("C", s2C.as("col")).equalsCol("D", s2D)

    assertThrows[RequirementException](df1.requires.equalsCol("A", Series(1, 2, 3, 4, 5)))
    assertThrows[RequirementException](df1.requires.equalsCol("A", Series(6, 3, 2)))
    assertThrows[RequirementException](df1.requires.equalsCol("A", Series(6, 3, 2, 8)))
    assertThrows[RequirementException](df1.requires.equalsCol("A", Series(6, 3, 2, 8, 4, 99).head(5)))
    assertThrows[RequirementException](df1.requires.equalsCol("A", s2A))
    assertThrows[RequirementException](df1.requires.equalsCol("A", s1ASliced))
    assertThrows[RequirementException](df2.requires.equalsCol("A", s1A))
    assertThrows[RequirementException](df1.requires.equalsCol("B", s1A))
    assertThrows[RequirementException](df1.requires.equalsCol("X", s1A))
  }

  test("equals[T](series: Series[T]): Requirement") {
    df1.requires.equalsCol(s1A).equalsCol(s1B).equalsCol(s1C).equalsCol(s1D)
    df2.requires.equalsCol(s2A).equalsCol(s2B).equalsCol(s2C).equalsCol(s2D)

    assertThrows[RequirementException](df1.requires.equalsCol(Series(1, 2, 3, 4, 5) as "A"))
    assertThrows[RequirementException](df1.requires.equalsCol(Series(6, 3, 2) as "A"))
    assertThrows[RequirementException](df1.requires.equalsCol(Series("A")(6, 3, 2, 8)))
    assertThrows[RequirementException](df1.requires.equalsCol(Series("A")(6, 3, 2, 8, 4, 99).head(5)))
    assertThrows[RequirementException](df1.requires.equalsCol(s2A as "A"))
    assertThrows[RequirementException](df1.requires.equalsCol(s1ASliced as "A"))
    assertThrows[RequirementException](df2.requires.equalsCol(s1A as "A"))
    assertThrows[RequirementException](df1.requires.equalsCol(s1A as "B"))
    assertThrows[RequirementException](df1.requires.equalsCol(s1A as "X"))
    assertThrows[RequirementException](df1.requires.equalsCol(Series(6, 3, 2, 8, 4)))
  }

  test("equals[T](col: String)(values: (T | Null)*): Requirement") {
    df1.requires
      .equalsCol("A")(6, 3, 2, 8, 4)
      .equalsCol("B")(23.1, 1.4, 1.4, 7.0, 3.1)
      .equalsCol("C")("ghi", "ABC", "XyZ", "qqq", "Uuu")
      .equalsCol("D")(C1("A", 5), C1("B", 1), C1("C", 2), C1("D", 2), C1("E", 0))
    df2.requires
      .equalsCol("A")(6, 3, null, 8, 4)
      .equalsCol("B")(null, 1.4, 1.4, 7.0, 3.1)
      .equalsCol("C")("ghi", "ABC", "XyZ", null, null)
      .equalsCol("D")(C1("A", 5), null, C1("C", 2), C1("D", 2), C1("E", 0))

    assertThrows[RequirementException](df1.requires.equalsCol("A")(1, 2, 3, 4, 5))
    assertThrows[RequirementException](df1.requires.equalsCol("A")(6, 3, 2))
    assertThrows[RequirementException](df1.requires.equalsCol("A")(6, 3, 2, 8))
    assertThrows[RequirementException](df1.requires.equalsCol("A")(6, 3, null, 8, 4))
    assertThrows[RequirementException](df2.requires.equalsCol("A")(6, 3, 2, 8, 4))
    assertThrows[RequirementException](df1.requires.equalsCol("B")(6, 3, 2, 8, 4))
    assertThrows[RequirementException](df1.requires.equalsCol("X")(6, 3, 2, 8, 4))
  }

  test("equals(df: DataFrame): Requirement") {
    df1.requires.equals(df1)
    df2.requires.equals(df2)
    assertThrows[RequirementException](df1.requires.equals(df2))
  }

  test("has(cols: String*): Requirement") {
    df1.requires has "A" has "B" has "C" has "D"
    df1.requires.has("A", "B", "C", "D")
    assertThrows[RequirementException](df1.requires.has("Unknown"))
    assertThrows[RequirementException](df1.requires.has("A", "B", "Unknown", "D"))
  }

  test("hasExactly(cols: String*): Requirement") {
    df1.requires.hasExactly("A", "B", "C", "D")
    assertThrows[RequirementException](df1.requires.hasExactly("A", "B", "Unknown", "D"))
    assertThrows[RequirementException](df1.requires.hasExactly("A", "B", "D"))
    assertThrows[RequirementException](df1.requires.hasExactly("A", "B", "C", "D "))
    assertThrows[RequirementException](df1.requires.hasExactly("A", "B", "C", "D", "E"))
    assertThrows[RequirementException](df1.requires.hasExactly("A", "B", "C", "D", "A"))
  }

  test("hasNumCols(n: Int): Requirement") {
    DataFrame.empty.requires.hasNumCols(0)
    df1.requires.hasNumCols(4)
    df3.requires.hasNumCols(4)
  }

  test("hasNumRows(n: Int): Requirement") {
    DataFrame.empty.requires.hasNumRows(0)
    df1.requires.hasNumRows(5)
    df3.requires.hasNumRows(4)
  }

  test("isType[T](col: String, all: Boolean): Requirement") {
    // DataFrame
    df1.requires.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")
    df1.requires.isType[C1]("D")
    df1.requires.isType[Int]("A", true).isType[Double]("B", true).isType[String]("C", true).isType[C1]("D", true)
    df1.requires.isType[C1]("D", true)
    df1.requires.isType[Any]("D")
    df1.requires.isType[Any]("C")
    df1.requires.isType[Any]("A")
    df3a.requires.isType[Int]("A", "A1")
    df3a.requires.isType[String]("C", "C 1", "c-2")

    assertThrows[RequirementException](df1.requires.isType[C1]("A"))
    assertThrows[RequirementException](df1.requires.isType[Int]("B"))
    assertThrows[RequirementException](df1.requires.isType[Double]("C"))
    assertThrows[RequirementException](df1.requires.isType[String]("D"))
    assertThrows[RequirementException](df3a.requires.isType[Double]("A", "A1"))
    assertThrows[RequirementException](df1.requires.isType[Double]("Unknown"))

    // DataFrame with nulls
    df2.requires.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")
    df2.requires.isType[C1]("D")
    assertThrows[RequirementException](df2.requires.isType[C1]("A"))
    assertThrows[RequirementException](df2.requires.isType[Int]("B"))
    assertThrows[RequirementException](df2.requires.isType[Double]("C"))
    assertThrows[RequirementException](df2.requires.isType[String]("D"))
    assertThrows[RequirementException](df2.requires.isType[Int]("A", true))
    assertThrows[RequirementException](df2.requires.isType[Double]("B", true))
    assertThrows[RequirementException](df2.requires.isType[String]("C", true))
    assertThrows[RequirementException](df2.requires.isType[C1]("D", true))

    df4a.requires.isType[C2]("A") // look-up on head only
    df4a.requires.isType[C3]("B") // look-up on head only
    df4b.requires.isType[C2]("A") // look-up on head only
    df4b.requires.isType[C3]("B") // look-up on head only
  }

  test("isType[T](cols: String*): Requirement") {
    df3b.requires.isType[Int]("A", "A1").isType[String]("C", "C 1", "c-2")
    assertThrows[RequirementException](df3a.requires.isType[Int]("A", "B"))
    assertThrows[RequirementException](df3a.requires.isType[Int]("C 1", "C-2"))
  }

  test("isTypeAll[T](cols: String*): Requirement") {
    df1.requires.isTypeAll[Int]("A").isTypeAll[Double]("B").isTypeAll[String]("C").isTypeAll[C1]("D")
    df1.requires.all.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

    assertThrows[RequirementException](df1.requires.isTypeAll[C1]("A"))
    assertThrows[RequirementException](df1.requires.isTypeAll[Int]("B"))
    assertThrows[RequirementException](df1.requires.isTypeAll[Double]("C"))
    assertThrows[RequirementException](df1.requires.isTypeAll[String]("D"))
    assertThrows[RequirementException](df3a.requires.isTypeAll[Int]("C 1", "c-2"))

    assertThrows[RequirementException](df2.requires.isTypeAll[Int]("A"))
    assertThrows[RequirementException](df2.requires.isTypeAll[Double]("B"))
    assertThrows[RequirementException](df2.requires.isTypeAll[String]("C"))
    assertThrows[RequirementException](df2.requires.isTypeAll[C1]("D"))
    assertThrows[RequirementException](df3b.requires.isTypeAll[Int]("A", "A1"))
    assertThrows[RequirementException](df3b.requires.isTypeAll[String]("C 1", "c-2"))

    assertThrows[RequirementException](df2.requires.isTypeAll[C1]("A"))
    assertThrows[RequirementException](df2.requires.isTypeAll[Int]("B"))
    assertThrows[RequirementException](df2.requires.isTypeAll[Double]("C"))
    assertThrows[RequirementException](df2.requires.isTypeAll[String]("D"))

    assertThrows[RequirementException](df1.requires.all.isType[C1]("A"))
    assertThrows[RequirementException](df1.requires.all.isType[Int]("B"))
    assertThrows[RequirementException](df1.requires.all.isType[Double]("C"))
    assertThrows[RequirementException](df1.requires.all.isType[String]("D"))
    assertThrows[RequirementException](df3a.requires.all.isType[Int]("C 1", "c-2"))

    assertThrows[RequirementException](df2.requires.all.isType[Int]("A"))
    assertThrows[RequirementException](df2.requires.all.isType[Double]("B"))
    assertThrows[RequirementException](df2.requires.all.isType[String]("C"))
    assertThrows[RequirementException](df2.requires.all.isType[C1]("D"))
    assertThrows[RequirementException](df3b.requires.all.isType[Int]("A", "A1"))
    assertThrows[RequirementException](df3b.requires.all.isType[String]("C 1", "c-2"))

    assertThrows[RequirementException](df2.requires.all.isType[C1]("A"))
    assertThrows[RequirementException](df2.requires.all.isType[Int]("B"))
    assertThrows[RequirementException](df2.requires.all.isType[Double]("C"))
    assertThrows[RequirementException](df2.requires.all.isType[String]("D"))

    df4a.requires.isTypeAll[C2]("A") // look-up on head only
    df4a.requires.all.isType[C3]("B") // look-up on head only
    assertThrows[RequirementException](df4b.requires.isTypeAll[C2]("A"))
    assertThrows[RequirementException](df4b.requires.all.isType[C3]("B"))
  }

  test("isTypeAllStrictly[T](cols: String*): Requirement") {
    df1.requires
      .isTypeAllStrictly[Int]("A")
      .isTypeAllStrictly[Double]("B")
      .isTypeAllStrictly[String]("C")
      .isTypeAllStrictly[C1]("D")
    df1.requires.all.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

    assertThrows[RequirementException](df2.requires.isTypeAllStrictly[C1]("A"))
    assertThrows[RequirementException](df2.requires.isTypeAllStrictly[Int]("B"))
    assertThrows[RequirementException](df2.requires.isTypeAllStrictly[Double]("C"))
    assertThrows[RequirementException](df2.requires.isTypeAllStrictly[String]("D"))
    assertThrows[RequirementException](df3b.requires.isTypeAllStrictly[Int]("C 1", "c-2"))

    df4a.requires.isTypeAllStrictly[Any]("A")
    df4a.requires.isTypeAllStrictly[Any]("B")
    assertThrows[RequirementException](df4b.requires.isTypeAllStrictly[Any]("A"))
    assertThrows[RequirementException](df4b.requires.isTypeAllStrictly[Any]("B"))
    assertThrows[RequirementException](df4a.requires.isTypeAllStrictly[C2]("A"))
    assertThrows[RequirementException](df4a.requires.isTypeAllStrictly[C3]("B"))
    assertThrows[RequirementException](df4b.requires.isTypeAllStrictly[C2]("A"))
    assertThrows[RequirementException](df4b.requires.isTypeAllStrictly[C3]("B"))

    df4a.requires.strictly.all.isType[Any]("A")
    df4a.requires.all.strictly.isType[Any]("B")
    assertThrows[RequirementException](df4b.requires.all.strictly.isType[Any]("A"))
    assertThrows[RequirementException](df4b.requires.all.strictly.isType[Any]("B"))
    assertThrows[RequirementException](df4a.requires.all.strictly.isType[C2]("A"))
    assertThrows[RequirementException](df4a.requires.all.strictly.isType[C3]("B"))
    assertThrows[RequirementException](df4b.requires.all.strictly.isType[C2]("A"))
    assertThrows[RequirementException](df4b.requires.all.strictly.isType[C3]("B"))
  }

  test("isTypeStrictly[T](col: String, all: Boolean): Requirement") {

    df1.requires
      .isTypeStrictly[Int]("A")
      .isTypeStrictly[Double]("B")
      .isTypeStrictly[String]("C")
      .isTypeStrictly[C1]("D")
    df1.requires.strictly.isType[Int]("A").isType[Double]("B").isType[String]("C").isType[C1]("D")

    df2.requires.isTypeStrictly[Int]("A")
    df2.requires.strictly.isType[Double]("B")
    df2.requires.strictly.isType[String]("C")
    df2.requires.isTypeStrictly[C1]("D")
    df3b.requires.isTypeStrictly[String]("C 1", "c-2")

    df4a.requires.isTypeStrictly[Any]("A")
    df4a.requires.isTypeStrictly[Any]("B")
    df4b.requires.isTypeStrictly[Any]("A")
    df4b.requires.isTypeStrictly[Any]("B")
    assertThrows[RequirementException](df4a.requires.isTypeStrictly[C2]("A"))
    assertThrows[RequirementException](df4a.requires.isTypeStrictly[C3]("B"))
    assertThrows[RequirementException](df4b.requires.isTypeStrictly[C2]("A"))
    assertThrows[RequirementException](df4b.requires.isTypeStrictly[C3]("B"))

    df4a.requires.strictly.isType[Any]("A")
    df4a.requires.strictly.isType[Any]("B")
    df4b.requires.strictly.isType[Any]("A")
    df4b.requires.strictly.isType[Any]("B")
    assertThrows[RequirementException](df4a.requires.strictly.isType[C2]("A"))
    assertThrows[RequirementException](df4a.requires.strictly.isType[C3]("B"))
    assertThrows[RequirementException](df4b.requires.strictly.isType[C2]("A"))
    assertThrows[RequirementException](df4b.requires.strictly.isType[C3]("B"))
  }

  test("isTypeStrictly[T](cols: String*): Requirement") {
    df3b.requires.isType[Int]("A", "A1").isType[String]("C", "C 1", "c-2")
    assertThrows[RequirementException](df3a.requires.isType[Int]("A", "B"))
    assertThrows[RequirementException](df3a.requires.isType[Int]("C 1", "C-2"))
  }
