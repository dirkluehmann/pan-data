/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits.mutable

import pd.{BaseTest, Series}

class MutableSeriesBooleanTest extends BaseTest:

  test("mixed") {
    val s = Series(true, true, true, false, false, null)
    val s2 = Series(false, true, null, false, true, false)
    val m = s2.mutable
    val and = s && s2
    val or = s || s2

    m && s shouldBe and
    s && m shouldBe and
    m && s.mutable shouldBe and
    m && m shouldBe s2 && s2
    {
      val x = m.copy.mutable
      x &&= s
      x shouldBe and
    }

    m || s shouldBe or
    s || m shouldBe or
    m || s.mutable shouldBe or
    m || m shouldBe s2 || s2
    {
      val x = m.copy.mutable
      x ||= s
      x shouldBe or
    }
  }
