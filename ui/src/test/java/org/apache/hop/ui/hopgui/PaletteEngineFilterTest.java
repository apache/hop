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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.plugins.IPlugin;
import org.junit.jupiter.api.Test;

/**
 * Covers the unified "Hop" engine group behind the canvas "Design for:" combo — in particular that
 * the local single-threaded pipeline engine ("LocalSingle") is part of it, and that it does not
 * leak into the workflow side where no such engine exists.
 */
class PaletteEngineFilterTest {

  private static IPlugin plugin(String[] supported, String[] excluded) {
    IPlugin plugin = mock(IPlugin.class);
    when(plugin.getSupportedEngines()).thenReturn(supported);
    when(plugin.getExcludedEngines()).thenReturn(excluded);
    return plugin;
  }

  @Test
  void singleThreadedEngineIsLabelledAsTheHopGroup() {
    assertEquals(
        PaletteEngineFilter.HOP_ENGINE_GROUP_ID,
        PaletteEngineFilter.getPipelineEngineLabelForId("LocalSingle"));
    assertEquals(
        PaletteEngineFilter.HOP_ENGINE_GROUP_ID,
        PaletteEngineFilter.getPipelineEngineLabelForId("Local"));
    assertEquals(
        PaletteEngineFilter.HOP_ENGINE_GROUP_ID,
        PaletteEngineFilter.getPipelineEngineLabelForId("Remote"));
    assertEquals(
        PaletteEngineFilter.HOP_ENGINE_GROUP_ID,
        PaletteEngineFilter.getPipelineEngineLabelForId("LoadBalancing"));
    assertEquals(
        PaletteEngineFilter.NO_FILTER_LABEL, PaletteEngineFilter.getPipelineEngineLabelForId(""));
  }

  @Test
  void hopGroupAllowsTransformsDeclaredForAnyOfItsEngines() {
    PaletteEngineFilter filter =
        PaletteEngineFilter.forPipelineEngineId(PaletteEngineFilter.HOP_ENGINE_GROUP_ID);

    assertTrue(filter.isActive());
    assertTrue(filter.isPluginAllowed(plugin(new String[] {"Local"}, new String[0])));
    assertTrue(filter.isPluginAllowed(plugin(new String[] {"LocalSingle"}, new String[0])));
    assertTrue(filter.isPluginAllowed(plugin(new String[0], new String[0])));
  }

  @Test
  void hopGroupHidesTransformsRefusedByEveryMemberEngine() {
    PaletteEngineFilter filter =
        PaletteEngineFilter.forPipelineEngineId(PaletteEngineFilter.HOP_ENGINE_GROUP_ID);

    assertFalse(filter.isPluginAllowed(plugin(new String[] {"Beam*"}, new String[0])));
    assertFalse(
        filter.isPluginAllowed(
            plugin(
                new String[0], new String[] {"Local", "Remote", "LoadBalancing", "LocalSingle"})));
  }

  /** A stored single-member id from an older release still filters as the whole group. */
  @Test
  void storedGroupMemberIdExpandsToTheGroup() {
    PaletteEngineFilter filter = PaletteEngineFilter.forPipelineEngineId("LocalSingle");

    assertTrue(filter.isPluginAllowed(plugin(new String[] {"Local"}, new String[0])));
    assertFalse(filter.isPluginAllowed(plugin(new String[] {"Beam*"}, new String[0])));
  }

  /**
   * There is no single-threaded workflow engine, so "LocalSingle" must not be probed on the
   * workflow side — an id without an engine always resolves UNKNOWN, and since the group is
   * permissive that would make every action visible and silently disable the filter.
   */
  @Test
  void workflowGroupDoesNotIncludeTheSingleThreadedEngine() {
    PaletteEngineFilter filter =
        PaletteEngineFilter.forWorkflowEngineId(PaletteEngineFilter.HOP_ENGINE_GROUP_ID);

    assertFalse(
        filter.isPluginAllowed(
            plugin(new String[0], new String[] {"Local", "Remote", "LoadBalancing"})),
        "action excluded on every Hop workflow engine must stay hidden");
  }
}
