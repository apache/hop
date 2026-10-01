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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TextFindSupportTest {

  @Test
  void findNextCaseSensitive() {
    String text = "Select From from FROM dual";
    assertEquals(7, TextFindSupport.findNext(text, "From", 0, true));
    assertEquals(12, TextFindSupport.findNext(text, "from", 0, true));
    assertEquals(-1, TextFindSupport.findNext(text, "missing", 0, true));
  }

  @Test
  void findNextCaseInsensitive() {
    String text = "Select From dual";
    assertEquals(7, TextFindSupport.findNext(text, "FROM", 0, false));
    assertEquals(-1, TextFindSupport.findNext(text, "from", 8, false));
  }

  @Test
  void findPrevious() {
    String text = "aaa bbb aaa ccc";
    assertEquals(8, TextFindSupport.findPrevious(text, "aaa", 10, true));
    assertEquals(0, TextFindSupport.findPrevious(text, "aaa", 7, true));
    assertEquals(-1, TextFindSupport.findPrevious(text, "aaa", -1, true));
  }

  @Test
  void replaceAllCaseSensitive() {
    TextFindSupport.ReplaceAllResult result =
        TextFindSupport.replaceAll("aAa aAa aaa", "aAa", "X", true);
    assertEquals(2, result.count());
    assertEquals("X X aaa", result.text());
  }

  @Test
  void replaceAllCaseInsensitive() {
    TextFindSupport.ReplaceAllResult result =
        TextFindSupport.replaceAll("Hello hello HELLO", "hello", "Hi", false);
    assertEquals(3, result.count());
    assertEquals("Hi Hi Hi", result.text());
  }
}
