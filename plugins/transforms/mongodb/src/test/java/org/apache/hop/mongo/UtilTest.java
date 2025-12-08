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

package org.apache.hop.mongo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UtilTest {

  @Test
  void testIsEmptyWithNull() {
    assertTrue(Util.isEmpty(null));
  }

  @Test
  void testIsEmptyWithEmptyString() {
    assertTrue(Util.isEmpty(""));
  }

  @Test
  void testIsEmptyWithWhitespace() {
    assertTrue(Util.isEmpty("   "));
    assertTrue(Util.isEmpty("\t"));
    assertTrue(Util.isEmpty("\n"));
    assertTrue(Util.isEmpty("  \t\n  "));
  }

  @Test
  void testIsEmptyWithNonEmptyString() {
    assertFalse(Util.isEmpty("hello"));
    assertFalse(Util.isEmpty("  hello  "));
    assertFalse(Util.isEmpty("a"));
  }

  @Test
  void testIsEmptyWithSpecialCharacters() {
    assertFalse(Util.isEmpty("@"));
    assertFalse(Util.isEmpty("$"));
    assertFalse(Util.isEmpty("."));
  }
}
