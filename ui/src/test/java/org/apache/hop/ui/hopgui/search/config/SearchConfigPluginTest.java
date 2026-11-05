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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** The search checkboxes are only changed by hop-conf when their option is given (#9007). */
class SearchConfigPluginTest {

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  void inMemory() {
    HopConfig.setInMemoryMode(true);
  }

  @AfterEach
  void restore() {
    SearchConfig config = SearchConfigSingleton.getConfig();
    config.setSearchAsYouType(null);
    config.setIncludeProjectTextFiles(null);
    config.setMaxResults(null);
    HopConfig.setInMemoryMode(false);
  }

  @Test
  void checkboxOptionsAreUnsetUnlessGiven() {
    SearchConfigPlugin plugin = parse("--search-max-results=10");
    assertNull(plugin.getSearchAsYouType());
    assertNull(plugin.getIncludeProjectTextFiles());
  }

  @Test
  void negatedOptionsDisable() {
    SearchConfigPlugin plugin =
        parse("--no-search-as-you-type", "--no-search-include-project-text-files");
    assertFalse(plugin.getSearchAsYouType());
    assertFalse(plugin.getIncludeProjectTextFiles());
  }

  @Test
  void optionsEnable() {
    SearchConfigPlugin plugin =
        parse("--search-as-you-type", "--search-include-project-text-files");
    assertTrue(plugin.getSearchAsYouType());
    assertTrue(plugin.getIncludeProjectTextFiles());
  }

  @Test
  void otherOptionKeepsTheCheckboxSettings() throws Exception {
    SearchConfig config = SearchConfigSingleton.getConfig();
    config.setSearchAsYouType(false);
    config.setIncludeProjectTextFiles(false);

    SearchConfigPlugin plugin = parse("--search-max-results=10");
    assertTrue(plugin.handleOption(LogChannel.GENERAL, null, new Variables()));

    assertEquals("10", config.getMaxResults());
    assertFalse(config.getSearchAsYouType());
    assertFalse(config.getIncludeProjectTextFiles());
  }

  private static SearchConfigPlugin parse(String... args) {
    SearchConfigPlugin plugin = new SearchConfigPlugin();
    new CommandLine(plugin).parseArgs(args);
    return plugin;
  }
}
