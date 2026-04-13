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

package org.apache.hop.core.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SearchQueryTest {

  @Test
  void emptySearchStringMatchesNonEmptyTargetOnly() {
    SearchQuery q = new SearchQuery("", false, false);
    assertTrue(q.matches("anything"));
    assertFalse(q.matches(""));
  }

  @Test
  void substringCaseInsensitive() {
    SearchQuery q = new SearchQuery("Foo", false, false);
    assertTrue(q.matches("barfooBaz"));
    assertFalse(q.matches("barf00Baz"));
  }

  @Test
  void substringCaseSensitive() {
    SearchQuery q = new SearchQuery("Foo", true, false);
    assertTrue(q.matches("xFooy"));
    assertFalse(q.matches("xfooy"));
  }

  @Test
  void regExCaseInsensitive() {
    SearchQuery q = new SearchQuery("^a.c$", false, true);
    assertTrue(q.matches("AbC"));
    assertFalse(q.matches("axxc"));
  }

  @Test
  void regExCaseSensitive() {
    SearchQuery q = new SearchQuery("^ABC$", true, true);
    assertTrue(q.matches("ABC"));
    assertFalse(q.matches("abc"));
  }

  @Test
  void gettersSetters() {
    SearchQuery q = new SearchQuery();
    q.setSearchString("x");
    q.setCaseSensitive(true);
    q.setRegEx(true);
    assertEquals("x", q.getSearchString());
    assertTrue(q.isCaseSensitive());
    assertTrue(q.isRegEx());
  }
}
