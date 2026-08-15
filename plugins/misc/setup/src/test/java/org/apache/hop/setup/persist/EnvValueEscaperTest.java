/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.setup.HopSetupException;
import org.junit.jupiter.api.Test;

class EnvValueEscaperTest {

  @Test
  void shellQuotesSimplePath() throws Exception {
    assertEquals(
        "'/home/alice/.local/share/hop'",
        EnvValueEscaper.shellSingleQuoted("V", "/home/alice/.local/share/hop"));
  }

  @Test
  void shellRejectsQuoteAndNewline() {
    assertThrows(HopSetupException.class, () -> EnvValueEscaper.shellSingleQuoted("V", "o'reilly"));
    assertThrows(HopSetupException.class, () -> EnvValueEscaper.shellSingleQuoted("V", "a\nb"));
  }

  @Test
  void cmdRejectsMetacharacters() {
    assertThrows(HopSetupException.class, () -> EnvValueEscaper.cmdQuoted("V", "a%PATH%"));
    assertThrows(HopSetupException.class, () -> EnvValueEscaper.cmdQuoted("V", "a&b"));
  }

  @Test
  void powershellDoublesSingleQuotes() throws Exception {
    assertEquals("'o''reilly'", EnvValueEscaper.powershellSingleQuoted("V", "o'reilly"));
  }
}
