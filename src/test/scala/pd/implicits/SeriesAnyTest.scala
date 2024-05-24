/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.BaseTest

class SeriesAnyTest extends BaseTest:
  test("assign") {
    df1("A") + df1("B") shouldBe df1("A").toInt + df1("B").toDouble
    df2("A") + df2("B") shouldBe df2("A").toInt + df2("B").toDouble
    df3("B") + df2("A") shouldBe df3("B").toDouble + df2("A").toInt
    df1("A") - df1("B") shouldBe df1("A").toInt - df1("B").toDouble
    df2("A") - df2("B") shouldBe df2("A").toInt - df2("B").toDouble
    df3("B") - df2("A") shouldBe df3("B").toDouble - df2("A").toInt
    df1("A") * df1("B") shouldBe df1("A").toInt * df1("B").toDouble
    df2("A") * df2("B") shouldBe df2("A").toInt * df2("B").toDouble
    df3("B") * df2("A") shouldBe df3("B").toDouble * df2("A").toInt
    df1("A") / df1("B") shouldBe df1("A").toInt / df1("B").toDouble
    df2("A") / df2("B") shouldBe df2("A").toInt / df2("B").toDouble
    df3("B") / df2("A") shouldBe df3("B").toDouble / df2("A").toInt
  }
