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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableViewColumnViewsTest {

  @Test
  void resolvesNamesInViewOrder() {
    String[] available = {"id", "name", "email", "amount"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(available, List.of("email", "id"));

    assertEquals(List.of(2, 0), indices);
  }

  @Test
  void skipsMissingNames() {
    String[] available = {"id", "name", "email"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(
            available, List.of("email", "missing", "id", "also_missing"));

    assertEquals(List.of(2, 0), indices);
  }

  @Test
  void usesFirstUnusedMatchForDuplicateAvailableNames() {
    String[] available = {"id", "name", "id"};
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(available, List.of("id", "id"));

    assertEquals(List.of(0, 2), indices);
  }

  @Test
  void emptyViewOrTableYieldsEmptyResult() {
    assertTrue(TableViewColumnViews.resolveColumnIndices(new String[0], List.of("id")).isEmpty());
    assertTrue(
        TableViewColumnViews.resolveColumnIndices(new String[] {"id"}, Collections.emptyList())
            .isEmpty());
    assertTrue(TableViewColumnViews.resolveColumnIndices(null, List.of("id")).isEmpty());
    assertTrue(TableViewColumnViews.resolveColumnIndices(new String[] {"id"}, null).isEmpty());
  }

  @Test
  void zeroMatchesIsEmpty() {
    List<Integer> indices =
        TableViewColumnViews.resolveColumnIndices(
            new String[] {"id", "name"}, Arrays.asList("other", null));

    assertTrue(indices.isEmpty());
  }
}
