/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.utils

import pd.BaseTest
import pd.internal.utils.StringUtils.fixedString

class StringUtilsTest extends BaseTest {

  test("fixedString") {
    fixedString("Index", 9, centerAlign = true) shouldBe "  Index  "
    fixedString("1234", 9, centerAlign = true) shouldBe "   1234  "
    fixedString("int", 9, centerAlign = true) shouldBe "   int   "
    fixedString("Index", 10, centerAlign = true) shouldBe "   Index  "
    fixedString("1234", 10, centerAlign = true) shouldBe "   1234   "
    fixedString("int", 10, centerAlign = true) shouldBe "    int   "
  }

}
