/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.math.*
import pd.{BaseTest, Series}

import scala.math

class SeriesIntTest extends BaseTest {

  val x: Series[Int] = Series("x")(3, 6, -1, 7)
  val y: Series[Int] = Series("y")(2, 3, 2, 7)
  val z: Series[Double] = Series("z")(1.4, 2.5, 3.7, 4.9)
  val xn: Series[Int] = Series("x")(3, null, -1, 7)
  val yn: Series[Int] = Series("y")(2, 3, 2, null)
  val zn: Series[Double] = Series("z")(1.4, 2.5, null, 4.9)

  Seq[(String, (Series[Int], Series[Int]) => Series[Int], (Int, Int) => Int)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
  ) foreach { op =>
    test(s"Series[Int] ${op._1} Series[Int]") {
      op._2(x, y) shouldBe Series(s"x${op._1}y")(op._3(3, 2), op._3(6, 3), op._3(-1, 2), op._3(7, 7))
      op._2(xn, yn) shouldBe Series(s"x${op._1}y")(op._3(3, 2), null, op._3(-1, 2), null)
    }
  }

  Seq[(String, (Series[Int], Series[Double]) => Series[Double], (Int, Double) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
  ) foreach { op =>
    test(s"Series[Int] ${op._1} Series[Double]") {
      op._2(x, z) shouldBe Series(s"x${op._1}z")(op._3(3, 1.4), op._3(6, 2.5), op._3(-1, 3.7), op._3(7, 4.9))
      op._2(xn, zn) shouldBe Series(s"x${op._1}z")(op._3(3, 1.4), null, null, op._3(7, 4.9))
    }
  }

  Seq[(String, (Series[Int], Int) => Series[Int], (Int, Int) => Int)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
    (":+", (x, y) => y + x, (x, y) => y + x),
    (":-", (x, y) => y - x, (x, y) => y - x),
    (":*", (x, y) => y * x, (x, y) => y * x),
    (":/", (x, y) => y / x, (x, y) => y / x),
  ) foreach { op =>
    test(s"Series[Int] ${op._1} Int") {
      op._2(x, 2) shouldBe Series("x")(op._3(3, 2), op._3(6, 2), op._3(-1, 2), op._3(7, 2))
      op._2(xn, 2) shouldBe Series("x")(op._3(3, 2), null, op._3(-1, 2), op._3(7, 2))
    }
  }

  Seq[(String, (Series[Int], Double) => Series[Double], (Int, Double) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
    (":+", (x, y) => y + x, (x, y) => y + x),
    (":-", (x, y) => y - x, (x, y) => y - x),
    (":*", (x, y) => y * x, (x, y) => y * x),
    (":/", (x, y) => y / x, (x, y) => y / x),
  ) foreach { op =>
    test(s"Series[Int] ${op._1} Double") {
      op._2(x, 2.7) shouldBe Series("x")(op._3(3, 2.7), op._3(6, 2.7), op._3(-1, 2.7), op._3(7, 2.7))
      op._2(xn, 2.7) shouldBe Series("x")(op._3(3, 2.7), null, op._3(-1, 2.7), op._3(7, 2.7))
    }
  }

  Seq[(String, Series[Int] => Series[Int], Int => Int)](
    ("abs", x => abs(x), x => math.abs(x))
  ) foreach { op =>
    test(s"${op._1}(Series[Int])") {
      op._2(x) shouldBe Series("x")(op._3(3), op._3(6), op._3(-1), op._3(7))
      op._2(xn) shouldBe Series("x")(op._3(3), null, op._3(-1), op._3(7))
    }
  }

}
