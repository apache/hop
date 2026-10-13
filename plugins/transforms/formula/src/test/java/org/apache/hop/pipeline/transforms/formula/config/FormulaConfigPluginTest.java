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

package org.apache.hop.pipeline.transforms.formula.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FormulaConfigPluginTest {

  private final ILogChannel log = mock(ILogChannel.class);

  private Map<String, Object> savedConfig;
  private boolean savedInMemory;

  /** An unconfigured Hop that never writes the real hop-config.json. */
  @BeforeEach
  void unconfiguredHop() {
    savedInMemory = HopConfig.isInMemoryMode();
    HopConfig.setInMemoryMode(true);
    savedConfig = new HashMap<>(HopConfig.getInstance().getConfigMap());
    HopConfig.getInstance().getConfigMap().clear();
  }

  @AfterEach
  void restoreConfiguration() {
    HopConfig.getInstance().getConfigMap().clear();
    HopConfig.getInstance().getConfigMap().putAll(savedConfig);
    HopConfig.setInMemoryMode(savedInMemory);
  }

  /** The configuration reference documents the annotated default, so it has to be the real one. */
  @Test
  void theDocumentedDefaultIsTheRealDefault() throws Exception {
    GuiWidgetElement widget =
        FormulaConfigPlugin.class
            .getDeclaredField("fastPathEnabled")
            .getAnnotation(GuiWidgetElement.class);

    assertEquals(
        Boolean.parseBoolean(widget.defaultValue()),
        FormulaConfigPlugin.getInstance().getFastPathEnabled());
    assertTrue(FormulaConfig.isFastPathEnabled());
  }

  @Test
  void hopConfSwitchesTheFastPathOffAndOnAgain() throws Exception {
    FormulaConfigPlugin plugin = new FormulaConfigPlugin();

    plugin.setFastPathEnabled(false);
    assertTrue(plugin.handleOption(log, null, null));
    assertFalse(FormulaConfig.isFastPathEnabled());
    assertFalse(FormulaConfigPlugin.getInstance().getFastPathEnabled());

    plugin.setFastPathEnabled(true);
    assertTrue(plugin.handleOption(log, null, null));
    assertTrue(FormulaConfig.isFastPathEnabled());
  }

  @Test
  void withoutTheOptionHopConfLeavesItAlone() throws Exception {
    assertFalse(new FormulaConfigPlugin().handleOption(log, null, null));
    assertFalse(
        HopConfig.getInstance().getConfigMap().containsKey(FormulaConfig.OPTION_FAST_PATH_ENABLED));
  }
}
