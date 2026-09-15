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

package org.apache.hop.ui.hopgui.palette;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.config.HopConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphPaletteTest {

  private String saved;

  @BeforeEach
  void setUp() {
    saved = HopConfig.getGuiProperty(GraphPalette.CONFIG_KEY);
    HopConfig.readGuiProperties().remove(GraphPalette.CONFIG_KEY);
  }

  @AfterEach
  void tearDown() {
    if (saved == null) {
      HopConfig.readGuiProperties().remove(GraphPalette.CONFIG_KEY);
    } else {
      HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, saved);
    }
  }

  @Test
  void hiddenByDefault() {
    assertFalse(GraphPalette.isVisible());
  }

  @Test
  void setVisibleUpdatesTheFlagWithoutRequiringYOnMissingKey() {
    HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, "Y");
    assertTrue(GraphPalette.isVisible());
    HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, "N");
    assertFalse(GraphPalette.isVisible());
  }
}
