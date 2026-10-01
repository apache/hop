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
package org.apache.hop.ui.hopgui.file.workflow.context;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.ui.hopgui.palette.GraphPalette;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The empty-canvas context dialog only lists actions to create while the palette tree is hidden.
 */
class HopGuiWorkflowContextTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

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

  private static boolean hasCreateActions() {
    List<GuiAction> actions =
        new HopGuiWorkflowContext(new WorkflowMeta(), null, new Point(0, 0)).getSupportedActions();
    return actions.stream().anyMatch(a -> a.getType() == GuiActionType.Create);
  }

  @Test
  void paletteHiddenListsActionsToCreate() {
    HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, "N");
    assertTrue(hasCreateActions());
  }

  @Test
  void paletteShownLeavesActionCreationToThePalette() {
    HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, "Y");
    assertFalse(hasCreateActions());
  }
}
