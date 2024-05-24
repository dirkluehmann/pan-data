/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.implicits.SeriesAny
import pd.{BaseTest, Order, Series}

class MutableSeriesAnyTest extends BaseTest:

  def execMutable(a: SeriesAny, op: MutableSeriesAny => Unit): Series[Any] =
    val m = a.mutable
    op(m)
    m.series

  test("assign") {
    execMutable(df1("A"), x => x += df1("B")) shouldBe df1("A").toInt + df1("B").toDouble
    execMutable(df2("A"), x => x += df2("B")) shouldBe df2("A").toInt + df2("B").toDouble
    execMutable(df3("B"), x => x += df2("A")) shouldBe df3("B").toDouble + df2("A").toInt
    execMutable(df1("A"), x => x -= df1("B")) shouldBe df1("A").toInt - df1("B").toDouble
    execMutable(df2("A"), x => x -= df2("B")) shouldBe df2("A").toInt - df2("B").toDouble
    execMutable(df3("B"), x => x -= df2("A")) shouldBe df3("B").toDouble - df2("A").toInt
    execMutable(df1("A"), x => x *= df1("B")) shouldBe df1("A").toInt * df1("B").toDouble
    execMutable(df2("A"), x => x *= df2("B")) shouldBe df2("A").toInt * df2("B").toDouble
    execMutable(df3("B"), x => x *= df2("A")) shouldBe df3("B").toDouble * df2("A").toInt
    execMutable(df1("A"), x => x /= df1("B")) shouldBe df1("A").toInt / df1("B").toDouble
    execMutable(df2("A"), x => x /= df2("B")) shouldBe df2("A").toInt / df2("B").toDouble
    execMutable(df3("B"), x => x /= df2("A")) shouldBe df3("B").toDouble / df2("A").toInt
  }

  test("sortValues: Unit") {
    val m1 = df1.A.mutable
    m1.sortValues
    m1 shouldBe df1.A.sortValues
    val m2 = df2.B.mutable
    m2.sortValues
    m2 shouldBe df2.B.sortValues
    val m3 = df3.C.mutable
    m3.sortValues
    m3 shouldBe df3.C.sortValues
  }

  test("sortValues(order: Order): Unit") {
    val m1 = df1.A.mutable
    m1.sortValues(Order.desc)
    m1 shouldBe df1.A.sortValues(Order.desc)
    val m2 = df2.B.mutable
    m2.sortValues(Order.ascNullsFirst)
    m2 shouldBe df2.B.sortValues(Order.ascNullsFirst)
    val m3 = df3.C.mutable
    m3.sortValues(Order.descNullsFirst)
    m3 shouldBe df3.C.sortValues(Order.descNullsFirst)
  }
