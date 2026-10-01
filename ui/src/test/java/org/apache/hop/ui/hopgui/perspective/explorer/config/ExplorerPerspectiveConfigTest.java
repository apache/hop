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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.ui.util.HelpOpenMode;
import org.junit.jupiter.api.Test;

class ExplorerPerspectiveConfigTest {

  private final ObjectMapper mapper = HopJson.newMapper();

  @Test
  void defaultModeIsBrowser() {
    assertEquals(HelpOpenMode.BROWSER, new ExplorerPerspectiveConfig().getHelpOpenMode());
  }

  @Test
  void migratesLegacyOpeningHelpFilesTrueToTab() throws Exception {
    ExplorerPerspectiveConfig config =
        mapper.readValue(
            "{\"lazyLoadingDepth\":\"0\",\"openingHelpFiles\":true}",
            ExplorerPerspectiveConfig.class);
    assertEquals(HelpOpenMode.TAB, config.getHelpOpenMode());
  }

  @Test
  void migratesLegacyOpeningHelpFilesFalseToBrowser() throws Exception {
    ExplorerPerspectiveConfig config =
        mapper.readValue("{\"openingHelpFiles\":false}", ExplorerPerspectiveConfig.class);
    assertEquals(HelpOpenMode.BROWSER, config.getHelpOpenMode());
  }

  @Test
  void missingLegacyFlagDefaultsToBrowser() throws Exception {
    ExplorerPerspectiveConfig config =
        mapper.readValue("{\"lazyLoadingDepth\":\"2\"}", ExplorerPerspectiveConfig.class);
    assertEquals(HelpOpenMode.BROWSER, config.getHelpOpenMode());
  }

  @Test
  void newHelpOpenModeWinsOverLegacyBoolean() throws Exception {
    ExplorerPerspectiveConfig config =
        mapper.readValue(
            "{\"helpOpenMode\":\"DIALOG\",\"openingHelpFiles\":true}",
            ExplorerPerspectiveConfig.class);
    assertEquals(HelpOpenMode.DIALOG, config.getHelpOpenMode());
  }

  @Test
  void serializesHelpOpenModeAndOmitsLegacyBoolean() throws Exception {
    ExplorerPerspectiveConfig config = new ExplorerPerspectiveConfig();
    config.setHelpOpenMode(HelpOpenMode.DIALOG);
    String json = mapper.writeValueAsString(config);
    assertEquals(
        HelpOpenMode.DIALOG,
        mapper.readValue(json, ExplorerPerspectiveConfig.class).getHelpOpenMode());
    assertFalse(json.contains("openingHelpFiles"));
    assertFalse(json.contains("\"helpOpenMode\":\"true\""));
  }

  @Test
  void copyConstructorCopiesHelpOpenMode() {
    ExplorerPerspectiveConfig original = new ExplorerPerspectiveConfig();
    original.setHelpOpenMode(HelpOpenMode.TAB);
    assertEquals(HelpOpenMode.TAB, new ExplorerPerspectiveConfig(original).getHelpOpenMode());
  }
}
