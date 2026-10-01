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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.database.Catalog;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.database.Schema;
import org.apache.hop.core.search.SearchMatcher;
import org.junit.jupiter.api.Test;

class DatabaseWorkbenchFilterTest {

  @Test
  void matchesFilterReturnsTrueWhenFilterIsEmpty() {
    SearchMatcher matcher = new SearchMatcher("", false, false, false);
    assertTrue(DatabaseWorkbench.matchesFilter(matcher, "", "customers", "public"));
    assertTrue(DatabaseWorkbench.matchesFilter(matcher, null, "customers", "public"));
  }

  @Test
  void matchesFilterMatchesTableName() {
    SearchMatcher matcher = new SearchMatcher("cust", false, false, false);
    assertTrue(DatabaseWorkbench.matchesFilter(matcher, "cust", "customers", "public"));
    assertTrue(DatabaseWorkbench.matchesFilter(matcher, "cust", "CUSTOMER_ORDER", "public"));
    assertFalse(DatabaseWorkbench.matchesFilter(matcher, "cust", "orders", "public"));
  }

  @Test
  void matchesFilterMatchesSchemaName() {
    SearchMatcher matcher = new SearchMatcher("pub", false, false, false);
    assertTrue(DatabaseWorkbench.matchesFilter(matcher, "pub", "orders", "public"));
    assertFalse(DatabaseWorkbench.matchesFilter(matcher, "pub", "orders", "analytics"));
  }

  @Test
  void schemaOrChildMatchesWithEmptyFilter() {
    SearchMatcher matcher = new SearchMatcher("", false, false, false);
    Schema schema = new Schema("public", new String[] {"customers"});
    assertTrue(DatabaseWorkbench.schemaOrChildMatches(matcher, "", schema, null));
  }

  @Test
  void schemaOrChildMatchesSchemaName() {
    SearchMatcher matcher = new SearchMatcher("pub", false, false, false);
    Schema schema = new Schema("public", new String[] {"orders"});
    assertTrue(DatabaseWorkbench.schemaOrChildMatches(matcher, "pub", schema, null));
  }

  @Test
  void schemaOrChildMatchesTableName() {
    SearchMatcher matcher = new SearchMatcher("cust", false, false, false);
    Schema schema = new Schema("sales", new String[] {"customers", "orders"});
    assertTrue(DatabaseWorkbench.schemaOrChildMatches(matcher, "cust", schema, null));

    Schema other = new Schema("sales", new String[] {"orders", "products"});
    assertFalse(DatabaseWorkbench.schemaOrChildMatches(matcher, "cust", other, null));
  }

  @Test
  void schemaOrChildMatchesViewsAndSynonyms() {
    SearchMatcher matcher = new SearchMatcher("view_order", false, false, false);
    Schema schema = new Schema("sales", new String[] {"customers"});

    DatabaseMetaInformation info = mock(DatabaseMetaInformation.class);
    Map<String, Collection<String>> viewMap = Map.of("sales", List.of("view_orders"));
    when(info.getViewMap()).thenReturn(viewMap);

    assertTrue(DatabaseWorkbench.schemaOrChildMatches(matcher, "view_order", schema, info));
  }

  @Test
  void catalogOrChildMatchesCatalogNameOrItems() {
    SearchMatcher emptyMatcher = new SearchMatcher("", false, false, false);
    Catalog catalog = new Catalog("cat1", new String[] {"orders"});
    assertTrue(DatabaseWorkbench.catalogOrChildMatches(emptyMatcher, "", catalog, null));

    SearchMatcher nameMatcher = new SearchMatcher("cat1", false, false, false);
    assertTrue(DatabaseWorkbench.catalogOrChildMatches(nameMatcher, "cat1", catalog, null));

    SearchMatcher itemMatcher = new SearchMatcher("ord", false, false, false);
    assertTrue(DatabaseWorkbench.catalogOrChildMatches(itemMatcher, "ord", catalog, null));

    SearchMatcher noMatcher = new SearchMatcher("cust", false, false, false);
    assertFalse(DatabaseWorkbench.catalogOrChildMatches(noMatcher, "cust", catalog, null));
  }
}
