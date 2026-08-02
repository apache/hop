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

package org.apache.hop.ui.hopgui.search.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SearchLimitsTest {

  @Test
  void defaultsMatchProductSafetyLimits() {
    SearchLimits limits = SearchLimits.defaults();
    assertEquals(3, limits.getMinContentQueryLength());
    assertEquals(500, limits.getMaxResults());
    assertEquals(20, limits.getMaxMatchesPerFile());
    assertEquals(1L * 1024 * 1024, limits.getMaxTextFileSizeBytes());
    assertTrue(limits.isIncludeProjectTextFiles());
    assertTrue(limits.isSearchAsYouType());
    assertEquals(300, limits.getDebounceMs());
  }

  @Test
  void contentSearchRequiresMinLength() {
    SearchLimits limits = SearchLimits.defaults();
    assertFalse(limits.allowsContentSearch("a"));
    assertFalse(limits.allowsContentSearch("ad"));
    assertTrue(limits.allowsContentSearch("add"));
    assertTrue(limits.allowsContentSearch("address"));
  }

  @Test
  void customConfigIsParsed() {
    SearchConfig config = new SearchConfig();
    config.setMinContentQueryLength("5");
    config.setMaxResults("100");
    config.setMaxMatchesPerFile("7");
    config.setMaxTextFileSizeMb("2");
    config.setIncludeProjectTextFiles(false);
    config.setSearchAsYouType(false);
    config.setDebounceMs("400");

    SearchLimits limits = SearchLimits.fromConfig(config);
    assertEquals(5, limits.getMinContentQueryLength());
    assertEquals(100, limits.getMaxResults());
    assertEquals(7, limits.getMaxMatchesPerFile());
    assertEquals(2L * 1024 * 1024, limits.getMaxTextFileSizeBytes());
    assertFalse(limits.isIncludeProjectTextFiles());
    assertFalse(limits.isSearchAsYouType());
    assertEquals(400, limits.getDebounceMs());
    assertFalse(limits.allowsContentSearch("addr"));
    assertTrue(limits.allowsContentSearch("addre"));
  }
}
