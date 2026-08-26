/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HelpUtilsTest {

  @Test
  void appendUtmParametersAddsQueryWhenMissing() {
    String tracked = HelpUtils.appendUtmParameters("https://hop.apache.org/manual/latest/x.html");
    assertTrue(tracked.startsWith("https://hop.apache.org/manual/latest/x.html?"));
    assertTrue(tracked.contains("mtm_campaign="));
    assertTrue(tracked.contains("mtm_source="));
    assertFalse(tracked.contains("?mtm_campaign") && tracked.contains("&mtm_campaign="));
  }

  @Test
  void appendUtmParametersUsesAmpersandWhenQueryExists() {
    String tracked =
        HelpUtils.appendUtmParameters("https://hop.apache.org/manual/latest/x.html?foo=bar");
    assertTrue(tracked.contains("?foo=bar&mtm_campaign="));
  }

  @Test
  void appendUtmParametersPassesThroughBlank() {
    assertEquals("", HelpUtils.appendUtmParameters(""));
    assertEquals(null, HelpUtils.appendUtmParameters(null));
  }
}
