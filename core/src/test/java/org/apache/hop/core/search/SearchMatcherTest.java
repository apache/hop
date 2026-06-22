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

class SearchMatcherTest {

  private static SearchMatcher substring(String q) {
    return new SearchMatcher(q, false, false, false);
  }

  private static SearchMatcher fuzzy(String q) {
    return new SearchMatcher(q, false, false, true);
  }

  @Test
  void caseInsensitiveSubstring() {
    assertTrue(substring("load").matches("Load Customers"));
    assertTrue(substring("CUSTOMERS").matches("load customers"));
    assertFalse(substring("orders").matches("load customers"));
  }

  @Test
  void caseSensitiveRespectsCase() {
    SearchMatcher matcher = new SearchMatcher("Load", true, false, false);
    assertTrue(matcher.matches("Load Customers"));
    assertFalse(matcher.matches("load customers"));
  }

  @Test
  void multiTermIsAnd() {
    assertTrue(substring("load customer").matches("customer loader pipeline"));
    assertTrue(substring("load,customer").matches("customer loader pipeline"));
    assertFalse(substring("load order").matches("customer loader pipeline"));
  }

  @Test
  void emptyQueryMatchesNonEmpty() {
    assertTrue(substring("").matches("anything"));
    assertFalse(substring("").matches(""));
  }

  @Test
  void diacriticsAreIgnored() {
    assertTrue(substring("recuperer").matches("Récupérer les données"));
    assertTrue(substring("Récupérer").matches("recuperer les donnees"));
  }

  @Test
  void scoreRanksExactAboveSubstring() {
    SearchMatcher matcher = substring("customer");
    double exact = matcher.score("customer");
    double word = matcher.score("the customer table");
    double substring = matcher.score("customers");
    assertTrue(exact >= word, "exact should outrank a word match");
    assertTrue(word >= substring, "word match should outrank a mid-word substring");
    assertEquals(0.0, matcher.score("orders"));
  }

  @Test
  void fuzzyMatchesTyposOnlyWhenEnabled() {
    // "custmer" is a typo of "customer"
    assertFalse(substring("custmer").matches("customer pipeline"));
    assertTrue(fuzzy("custmer").matches("customer pipeline"));
    // A fuzzy match scores below a real substring match.
    assertTrue(fuzzy("custmer").score("customer") < fuzzy("customer").score("customer"));
  }

  @Test
  void fuzzyDoesNotMatchUnrelated() {
    assertFalse(fuzzy("customer").matches("orders warehouse"));
  }

  @Test
  void fuzzyRequiresSimilarLength() {
    // "tableinput" is just "table input" without the space -> should fuzzy match.
    assertTrue(fuzzy("tableinput").matches("Table input"));
    // ...but it must NOT latch onto the much shorter keyword "table" (the old leniency bug).
    assertFalse(fuzzy("tableinput").matches("table"));
  }

  @Test
  void regexIsPartialMatch() {
    SearchMatcher matcher = new SearchMatcher("cust.*er", false, true, false);
    assertTrue(matcher.matches("the customer table"));
    assertFalse(matcher.matches("the orders table"));
  }

  @Test
  void invalidRegexMatchesNothing() {
    SearchMatcher matcher = new SearchMatcher("cust(er", false, true, false);
    assertFalse(matcher.matches("customer"));
  }
}
