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

package org.apache.hop.ui.hopgui.context;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.Test;

/**
 * A filter that cannot be created used to throw once per action. Resolving it once keeps every
 * action visible and logs the failure a single time.
 */
class BaseGuiContextHandlerFilterTest {

  private static final String PREPARE_ONCE = "BaseGuiContextHandlerFilterTest-prepare-once";

  @Test
  void brokenFilterIsPreparedOnceForEveryAction() throws Exception {
    HopLogStore.init();
    registerPrepareOnceActions();
    CountingPlugin.lookups = 0;

    List<String> actionIds = new ArrayList<>();
    for (Object action : new PrepareOnceContext().getPluginActions(true)) {
      actionIds.add(((GuiAction) action).getId());
    }

    assertEquals(1, CountingPlugin.lookups, "a broken filter is created once, not once per action");
    assertEquals(List.of("prepare-once-a", "prepare-once-b"), actionIds);
  }

  private static void registerPrepareOnceActions() throws Exception {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.getGuiContextActions(PREPARE_ONCE) != null) {
      return;
    }
    Class<?> pluginClass = CountingPlugin.class;
    ClassLoader classLoader = pluginClass.getClassLoader();
    for (String methodName : List.of("actionA", "actionB")) {
      Method method = pluginClass.getDeclaredMethod(methodName, PrepareOnceContext.class);
      registry.addGuiContextAction(
          pluginClass.getName(), method, method.getAnnotation(GuiContextAction.class), classLoader);
    }
    Method filterMethod =
        pluginClass.getDeclaredMethod("filterActions", String.class, PrepareOnceContext.class);
    registry.addGuiActionFilter(
        pluginClass.getName(),
        filterMethod,
        filterMethod.getAnnotation(GuiContextActionFilter.class),
        classLoader);
  }

  /**
   * {@code getInstance()} returns null and there is no zero-arg constructor. Preparing the menu
   * calls that lookup once.
   */
  public static final class CountingPlugin {
    static int lookups;

    @SuppressWarnings("unused")
    private final int marker;

    private CountingPlugin(int marker) {
      this.marker = marker;
    }

    public static CountingPlugin getInstance() {
      lookups++;
      return null;
    }

    @GuiContextAction(
        id = "prepare-once-b",
        parentId = PREPARE_ONCE,
        type = GuiActionType.Modify,
        name = "B",
        tooltip = "B",
        image = "b.svg")
    public void actionB(PrepareOnceContext context) {}

    @GuiContextAction(
        id = "prepare-once-a",
        parentId = PREPARE_ONCE,
        type = GuiActionType.Modify,
        name = "A",
        tooltip = "A",
        image = "a.svg")
    public void actionA(PrepareOnceContext context) {}

    @GuiContextActionFilter(parentId = PREPARE_ONCE)
    public boolean filterActions(String contextActionId, PrepareOnceContext context) {
      return false;
    }
  }

  public static final class PrepareOnceContext extends BaseGuiContextHandler {
    @Override
    public String getContextId() {
      return PREPARE_ONCE;
    }
  }
}
