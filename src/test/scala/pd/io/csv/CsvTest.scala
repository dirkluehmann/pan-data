/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io.csv

import pd.{BaseTest, DataFrame}

import java.net.URL

class CsvTest extends BaseTest:

  test("Read and write plain CSV") {
    {
      val df = df1(Seq("A", "B", "C"))
      df.io.csv.write("df1.csv")
      DataFrame.io.csv().read("df1.csv") shouldBe df
    }
    {
      val df = df1(Seq("A", "C", "B"))
      df.io.csv(nullEncoding = "null").write("df1.csv")
      DataFrame.io.csv(nullEncodingEnabled = false).read("df1.csv") shouldBe df
    }
  }

  test("Read and write CSV with undefined values") {
    {
      val df = df2(Seq("A", "B", "C"))
      df.io.csv.write("df2.csv")
      DataFrame.io.csv.read("df2.csv") shouldBe df
    }
    {
      val df = df2(Seq("A", "B", "C"))
      df.io.csv.write("df2.csv")
      DataFrame.io.csv(nullEncodingEnabled = false).read("df2.csv") should not be df
    }
    {
      val df = df2(Seq("A", "B", "C"))
      df.io.csv(nullEncoding = "null").write("df2.csv")
      DataFrame.io.csv(nullEncoding = "null").read("df2.csv") shouldBe df
    }
    {
      val df = df2(Seq("A", "B", "C"))
      df.io.csv(nullEncoding = "XXX").write("df2.csv")
      DataFrame.io.csv(nullEncoding = "XXX").read("df2.csv") shouldBe df
    }
    {
      val df = df3(Seq("A", "B", "C"))
      df.io.csv.write("df3.csv")
      DataFrame.io.csv.read("df3.csv") should not be df
      DataFrame.io.csv.read("df3.csv") shouldBe df.resetIndex
    }
  }

  test("Read and write RFC4180 compliant CSV") {
    val df = DataFrame.io.csv.readResource("RFC4180Example.csv")
    df.requires
      .hasNumRows(5)
      .hasNumCols(5)
      .isType[Int]("Year")
      .isType[String]("Make", "Description")
      .isTypeAll[String]("Model")
      .isTypeAll[Double]("Price")
    df.io.csv.write("RFC4180Example.csv")
    DataFrame.io.csv.read("RFC4180Example.csv") shouldBe df
  }
