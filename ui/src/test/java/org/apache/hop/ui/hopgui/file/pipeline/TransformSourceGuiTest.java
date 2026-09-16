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
package org.apache.hop.ui.hopgui.file.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSourceSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TransformSourceGuiTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUp() {
    TransformSourceSupport.clearCache();
  }

  @Test
  void dummyCreateActionIsNotLabeledAsPipelineSource() {
    IPlugin plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, "Dummy");
    assertNotNull(plugin);

    GuiAction action =
        new GuiAction(
            "pipeline-graph-create-transform-Dummy",
            GuiActionType.Create,
            plugin.getName(),
            plugin.getDescription(),
            plugin.getImageFile(),
            (shift, control, t) -> {});
    TransformSourceGui.labelCreateAction(action, plugin);

    assertFalse(action.getKeywords().contains(TransformSourceSupport.SEARCH_KEYWORD));
    assertTrue(
        action.getTooltip() == null
            || !action.getTooltip().contains("Can start without incoming hops"));
  }

  @Test
  void dummyPaletteKeywordsDoNotIncludePipelineSource() {
    IPlugin plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, "Dummy");
    assertNotNull(plugin);
    List<String> keywords = new ArrayList<>();
    TransformSourceGui.addSearchKeywords(keywords, plugin);
    assertTrue(keywords.isEmpty());
  }
}
