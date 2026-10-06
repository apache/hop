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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ConfigPluginOptionsTabTest {

  /**
   * Filling the widgets fires no events, so a plugin only learns they are filled from
   * widgetsPopulated. Without it, widgets that depend on others kept the wrong enabled state until
   * the first change.
   */
  @Test
  void aListeningPluginIsToldItsWidgetsAreFilled() {
    GuiCompositeWidgets widgets = mock(GuiCompositeWidgets.class);
    IGuiPluginCompositeWidgetsListener plugin = mock(IGuiPluginCompositeWidgetsListener.class);

    ConfigPluginOptionsTab.attachWidgetsListener(widgets, plugin);

    InOrder order = inOrder(widgets, plugin);
    order.verify(widgets).setWidgetsListener(plugin);
    order.verify(plugin).widgetsPopulated(widgets);
  }

  @Test
  void aPluginThatDoesNotListenIsLeftAlone() {
    GuiCompositeWidgets widgets = mock(GuiCompositeWidgets.class);

    ConfigPluginOptionsTab.attachWidgetsListener(widgets, new Object());

    verify(widgets, never()).setWidgetsListener(any());
  }
}
