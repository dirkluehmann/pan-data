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

class SeriesDoubleTest extends BaseTest {

  val x: Series[Double] = Series("x")(3.1, 6.0, -1.5, 7.0)
  val y: Series[Double] = Series("y")(2.0, 3.5, 2.0, 7.0)
  val z: Series[Int] = Series("z").from(1 to 4)
  val xn: Series[Double] = Series("x")(3.1, null, -1.5, 7.0)
  val yn: Series[Double] = Series("y")(2.0, 3.5, 2.0, null)
  val zn: Series[Int] = Series("z")(1, 2, null, 4)
  val xMask = Seq(0, 2, 3)
  val xs: Series[Double] = x(xMask)
  val ys: Series[Double] = y(Seq(0, 1, 2))
  val zs: Series[Int] = z(Seq(0, 1, 3))

  Seq[(String, (Series[Double], Series[Double]) => Series[Double], (Double, Double) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
  ) foreach { op =>
    testMulti(s"Series[Double] ${op._1} Series[Double]") {
      op._2(x, y) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), op._3(6.0, 3.5), op._3(-1.5, 2.0), op._3(7.0, 7.0))
      op._2(xn, yn) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), null, op._3(-1.5, 2.0), null)
      op._2(xn, ys) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), null, op._3(-1.5, 2.0), null)
      op._2(xs, ys) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), null, op._3(-1.5, 2.0), null).apply(xMask)
      op._2(xs, y) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), null, op._3(-1.5, 2.0), op._3(7.0, 7.0)).apply(xMask)
      op._2(x, yn) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), op._3(6.0, 3.5), op._3(-1.5, 2.0), null)
      op._2(x, ys) shouldBe Series(s"x${op._1}y")(op._3(3.1, 2.0), op._3(6.0, 3.5), op._3(-1.5, 2.0), null)
    }
  }

  Seq[(String, (Series[Double], Series[Int]) => Series[Double], (Double, Int) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
  ) foreach { op =>
    testMulti(s"Series[Double] ${op._1} Series[Int]") {
      op._2(x, z) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), op._3(6.0, 2), op._3(-1.5, 3), op._3(7.0, 4))
      op._2(xn, zn) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), null, null, op._3(7.0, 4))
      op._2(xn, zs) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), null, null, op._3(7.0, 4))
      op._2(xs, zs) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), null, null, op._3(7.0, 4)).apply(xMask)
      op._2(xs, z) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), null, op._3(-1.5, 3), op._3(7.0, 4)).apply(xMask)
      op._2(x, zn) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), op._3(6.0, 2), null, op._3(7.0, 4))
      op._2(x, zs) shouldBe Series(s"x${op._1}z")(op._3(3.1, 1), op._3(6.0, 2), null, op._3(7.0, 4))
    }
  }

  Seq[(String, (Series[Double], Double) => Series[Double], (Double, Double) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
    (":+", (x, y) => y + x, (x, y) => y + x),
    (":-", (x, y) => y - x, (x, y) => y - x),
    (":*", (x, y) => y * x, (x, y) => y * x),
    (":/", (x, y) => y / x, (x, y) => y / x),
  ) foreach { op =>
    testMulti(s"Series[Double] ${op._1} Double") {
      op._2(x, 2.4) shouldBe Series("x")(op._3(3.1, 2.4), op._3(6.0, 2.4), op._3(-1.5, 2.4), op._3(7.0, 2.4))
      op._2(xn, 2.4) shouldBe Series("x")(op._3(3.1, 2.4), null, op._3(-1.5, 2.4), op._3(7.0, 2.4))
      op._2(xs, 2.4) shouldBe Series("x")(op._3(3.1, 2.4), null, op._3(-1.5, 2.4), op._3(7.0, 2.4)).apply(xMask)
    }
  }

  Seq[(String, (Series[Double], Int) => Series[Double], (Double, Int) => Double)](
    ("+", (x, y) => x + y, (x, y) => x + y),
    ("-", (x, y) => x - y, (x, y) => x - y),
    ("*", (x, y) => x * y, (x, y) => x * y),
    ("/", (x, y) => x / y, (x, y) => x / y),
    (":+", (x, y) => y + x, (x, y) => y + x),
    (":-", (x, y) => y - x, (x, y) => y - x),
    (":*", (x, y) => y * x, (x, y) => y * x),
    (":/", (x, y) => y / x, (x, y) => y / x),
  ) foreach { op =>
    testMulti(s"Series[Double] ${op._1} Int") {
      op._2(x, 2) shouldBe Series("x")(op._3(3.1, 2), op._3(6.0, 2), op._3(-1.5, 2), op._3(7.0, 2))
      op._2(xn, 2) shouldBe Series("x")(op._3(3.1, 2), null, op._3(-1.5, 2), op._3(7.0, 2))
      op._2(xs, 2) shouldBe Series("x")(op._3(3.1, 2), null, op._3(-1.5, 2), op._3(7.0, 2)).apply(xMask)
    }
  }

  Seq[(String, Series[Double] => Series[Double], Double => Double)](
    ("abs", x => abs(x), x => math.abs(x)),
    ("ceil", x => ceil(x), x => math.ceil(x)),
    ("floor", x => floor(x), x => math.floor(x)),
    ("rint", x => rint(x), x => math.rint(x)),
    ("sqrt", x => sqrt(abs(x)), x => math.sqrt(math.abs(x))),
  ) foreach { op =>
    testMulti(s"${op._1}(Series[Double])") {
      op._2(x) shouldBe Series("x")(op._3(3.1), op._3(6.0), op._3(-1.5), op._3(7.0))
      op._2(xn) shouldBe Series("x")(op._3(3.1), null, op._3(-1.5), op._3(7.0))
      op._2(xs) shouldBe Series("x")(op._3(3.1), null, op._3(-1.5), op._3(7.0)).apply(xMask)
    }
  }

}
