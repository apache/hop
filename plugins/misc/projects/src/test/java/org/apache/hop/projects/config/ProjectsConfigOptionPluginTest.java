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

package org.apache.hop.projects.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class ProjectsConfigOptionPluginTest {

  @BeforeAll
  static void beforeAll() {
    HopLogStore.init();
  }

  /**
   * The restrict-environments option has no effect since 2.17 (#5382), but existing hop-conf
   * scripts still pass it: it must keep parsing instead of failing with "Unknown option".
   */
  @Test
  void deprecatedEnvironmentsForActiveProjectOptionIsStillAccepted() {
    ProjectsConfigOptionPlugin plugin = new ProjectsConfigOptionPlugin();
    CommandLine cmd = new CommandLine(plugin);

    cmd.parseArgs("-eap");
    assertEquals(Boolean.TRUE, plugin.getEnvironmentsForActiveProject());

    plugin = new ProjectsConfigOptionPlugin();
    new CommandLine(plugin).parseArgs("--environments-for-active-project=false");
    assertEquals(Boolean.FALSE, plugin.getEnvironmentsForActiveProject());
  }

  /**
   * On its own the option still counts as handled, or hop-conf would print its usage as if the
   * option were not understood. Nothing is saved for it.
   */
  @Test
  void deprecatedEnvironmentsForActiveProjectOptionIsHandledWithoutBeingSaved() throws Exception {
    HopConfig.setInMemoryMode(true);
    try {
      ProjectsConfigOptionPlugin plugin = new ProjectsConfigOptionPlugin();
      new CommandLine(plugin).parseArgs("-eap");

      assertTrue(plugin.handleOption(LogChannel.GENERAL, null, new Variables()));

      String saved = HopJson.newMapper().writeValueAsString(ProjectsConfigSingleton.getConfig());
      assertFalse(saved.contains("environmentsForActiveProject"), saved);
    } finally {
      HopConfig.setInMemoryMode(false);
    }
  }

  /** The options after it in the same invocation are still applied. */
  @Test
  void deprecatedEnvironmentsForActiveProjectOptionDoesNotStopTheOthers() throws Exception {
    HopConfig.setInMemoryMode(true);
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    boolean clearing = config.isClearingDbCacheWhenSwitching();
    try {
      ProjectsConfigOptionPlugin plugin = new ProjectsConfigOptionPlugin();
      new CommandLine(plugin).parseArgs("-eap", "-cdb=" + !clearing);

      assertTrue(plugin.handleOption(LogChannel.GENERAL, null, new Variables()));
      assertEquals(!clearing, config.isClearingDbCacheWhenSwitching());
    } finally {
      config.setClearingDbCacheWhenSwitching(clearing);
      HopConfig.setInMemoryMode(false);
    }
  }

  @Test
  void deprecatedEnvironmentsForActiveProjectOptionIsHiddenFromUsage() {
    String usage = new CommandLine(new ProjectsConfigOptionPlugin()).getUsageMessage();
    assertFalse(usage.contains("environments-for-active-project"), usage);
    assertTrue(usage.contains("--projects-enabled"), usage);
  }

  @Test
  void legacyConfigWithEnvironmentsForActiveProjectStillLoads() throws Exception {
    String json =
        "{\"enabled\":true,\"environmentsForActiveProject\":true,\"defaultProject\":\"default\"}";
    ProjectsConfig config = HopJson.newMapper().readValue(json, ProjectsConfig.class);
    assertTrue(config.isEnabled());
    assertEquals("default", config.getDefaultProject());

    String written = HopJson.newMapper().writeValueAsString(config);
    assertFalse(written.contains("environmentsForActiveProject"), written);
  }
}
