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

package org.apache.hop.ui.hopgui.perspective.explorer.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

/** The explorer checkboxes are only changed by hop-conf when their option is given (#9007). */
class ExplorerPerspectiveConfigPluginTest {

  private String originalLazyLoadingDepth;
  private Boolean originalVisibleByDefault;
  private Boolean originalActiveFileSelection;

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  @BeforeEach
  void inMemory() {
    HopConfig.setInMemoryMode(true);
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    originalLazyLoadingDepth = config.getLazyLoadingDepth();
    originalVisibleByDefault = config.getFileExplorerVisibleByDefault();
    originalActiveFileSelection = config.getActiveFileSelection();
  }

  @AfterEach
  void restore() {
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    config.setLazyLoadingDepth(originalLazyLoadingDepth);
    config.setFileExplorerVisibleByDefault(originalVisibleByDefault);
    config.setActiveFileSelection(originalActiveFileSelection);
    HopConfig.setInMemoryMode(false);
  }

  @Test
  void otherOptionKeepsTheCheckboxSettings() throws Exception {
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    config.setFileExplorerVisibleByDefault(false);
    config.setActiveFileSelection(false);

    assertTrue(handle("--explorer-lazy-loading-initial-depth=2"));

    assertEquals("2", config.getLazyLoadingDepth());
    assertFalse(config.getFileExplorerVisibleByDefault());
    assertFalse(config.getActiveFileSelection());
  }

  @Test
  void flagsWithoutAValueEnable() throws Exception {
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    config.setFileExplorerVisibleByDefault(false);
    config.setActiveFileSelection(false);

    assertTrue(handle("-exv", "-exafs"));

    assertTrue(config.getFileExplorerVisibleByDefault());
    assertTrue(config.getActiveFileSelection());
  }

  @Test
  void flagsWithFalseDisable() throws Exception {
    ExplorerPerspectiveConfig config = ExplorerPerspectiveConfigSingleton.getConfig();
    config.setFileExplorerVisibleByDefault(true);
    config.setActiveFileSelection(true);

    assertTrue(handle("-exv=false", "-exafs=false"));

    assertFalse(config.getFileExplorerVisibleByDefault());
    assertFalse(config.getActiveFileSelection());
  }

  private static boolean handle(String... args) throws Exception {
    ExplorerPerspectiveConfigPlugin plugin = new ExplorerPerspectiveConfigPlugin();
    new CommandLine(plugin).parseArgs(args);
    return plugin.handleOption(LogChannel.GENERAL, null, new Variables());
  }
}
