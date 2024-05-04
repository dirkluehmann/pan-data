/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.{BaseTest, Series}

class MutableSeriesIntTest extends BaseTest:

  test("mixed") {
    val s = s2ASliced
    val s2 = s3A
    val m = s.mutable
    val m2 = s2.mutable

    m + s2 shouldBe s + s2
    s + m2 shouldBe s + s2
    m + m shouldBe s + s
    {
      val x = m.copy.mutable
      x += s2
      x shouldBe s + s2
    }

    m - s2 shouldBe s - s2
    s - m2 shouldBe s - s2
    m - m shouldBe s - s
    {
      val x = m.copy.mutable
      x -= s2
      x shouldBe s - s2
    }

    m * s2 shouldBe s * s2
    s * m2 shouldBe s * s2
    m * m shouldBe s * s
    {
      val x = m.copy.mutable
      x *= s2
      x shouldBe s * s2
    }

    m / s2 shouldBe s / s2
    s / m2 shouldBe s / s2
    m / m shouldBe s / s
    {
      val x = m.copy.mutable
      x /= s2
      x shouldBe s / s2
    }
  }
