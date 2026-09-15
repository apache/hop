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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ComboFilterPopupTest {

  private static final List<String> TESTS =
      List.of(
          "merge-orders",
          "merge-customers",
          "load-customers",
          "validate-golden-customers",
          "orders-daily");

  @Test
  void emptyQueryKeepsOriginalOrder() {
    assertEquals(TESTS, ComboFilterPopup.filterItems(TESTS, ""));
    assertEquals(TESTS, ComboFilterPopup.filterItems(TESTS, null));
  }

  @Test
  void emptySourceIsEmpty() {
    assertTrue(ComboFilterPopup.filterItems(List.of(), "cust").isEmpty());
    assertTrue(ComboFilterPopup.filterItems(null, "cust").isEmpty());
  }

  @Test
  void substringIsCaseInsensitive() {
    List<String> matches = ComboFilterPopup.filterItems(TESTS, "CUSTOMER");
    assertEquals(
        List.of("load-customers", "merge-customers", "validate-golden-customers"), matches);
  }

  @Test
  void exactMatchRanksAboveSubstring() {
    List<String> names = List.of("customer-address", "customer", "the-customer-hub");
    List<String> matches = ComboFilterPopup.filterItems(names, "customer");
    assertEquals("customer", matches.get(0));
    assertTrue(matches.contains("customer-address"));
    assertTrue(matches.contains("the-customer-hub"));
  }

  @Test
  void noMatchIsEmpty() {
    assertTrue(ComboFilterPopup.filterItems(TESTS, "does-not-exist").isEmpty());
  }

  @Test
  void multiTermRequiresEveryTerm() {
    List<String> matches = ComboFilterPopup.filterItems(TESTS, "merge customer");
    assertEquals(List.of("merge-customers"), matches);
    assertTrue(ComboFilterPopup.filterItems(TESTS, "merge orders extra").isEmpty());
  }

  @Test
  void blankItemsAreIgnored() {
    List<String> matches = ComboFilterPopup.filterItems(List.of("", "keep-me", "  "), "keep");
    assertEquals(List.of("keep-me"), matches);
  }
}
