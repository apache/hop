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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionFilter;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.Test;

/**
 * Context-menu filters used to call {@code HopGuiPipelineGraph.getInstance()}. That returns the
 * active tab only, so a canvas that is not the active file (the SWTBot click tests) logged a null
 * plugin for every action.
 */
class BaseGuiContextHandlerFilterTest {

  private static final String PREPARE_ONCE = "BaseGuiContextHandlerFilterTest-prepare-once";

  @Test
  void filterUsesTheGraphOnTheContextWhenGetInstanceReturnsNull() throws Exception {
    GraphPlugin.active = null;
    GraphPlugin clicked = new GraphPlugin();
    GraphContext context = new GraphContext(clicked);

    assertTrue(context.evaluateActionFilter(action("show"), filter(GraphPlugin.class)));
    assertFalse(context.evaluateActionFilter(action("hide"), filter(GraphPlugin.class)));
    assertTrue(clicked.used, "the filter runs on the clicked graph, not on getInstance()");
  }

  @Test
  void filterPrefersTheClickedGraphOverAnotherActiveTab() throws Exception {
    GraphPlugin.active = new GraphPlugin();
    GraphPlugin clicked = new GraphPlugin();
    GraphContext context = new GraphContext(clicked);

    assertTrue(context.evaluateActionFilter(action("show"), filter(GraphPlugin.class)));
    assertFalse(GraphPlugin.active.used, "the active tab is not asked to filter this context");
    assertTrue(clicked.used);
  }

  @Test
  void filterUsesGetInstanceWhenTheContextDoesNotHoldThePlugin() throws Exception {
    SingletonPlugin.instance.used = false;
    EmptyContext context = new EmptyContext();

    assertTrue(context.evaluateActionFilter(action("show"), filter(SingletonPlugin.class)));
    assertTrue(SingletonPlugin.instance.used);
  }

  @Test
  void nullGetInstanceWithoutAContextPluginFailsClearly() {
    EmptyContext context = new EmptyContext();

    HopException exception =
        assertThrows(
            HopException.class,
            () -> context.evaluateActionFilter(action("show"), filter(NullPlugin.class)));
    assertTrue(exception.getMessage().contains(NullPlugin.class.getName()));
    assertFalse(exception.getMessage().contains("java.lang.String"));
  }

  @Test
  void filterUsesTheWorkflowGraphWhenGetInstanceReturnsNull() throws Exception {
    WorkflowPlugin.active = null;
    WorkflowPlugin clicked = new WorkflowPlugin();
    WorkflowContext context = new WorkflowContext(clicked);

    assertTrue(context.evaluateActionFilter(action("show"), filter(WorkflowPlugin.class)));
    assertTrue(clicked.used, "the filter runs on the clicked workflow graph");
  }

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

  private static GuiAction action(String id) {
    return new GuiAction(id, GuiActionType.Modify, id, id, null, null, null);
  }

  private static GuiActionFilter filter(Class<?> pluginClass) {
    return new GuiActionFilter(
        pluginClass.getName() + ".filterActions",
        pluginClass.getName(),
        "filterActions",
        pluginClass.getClassLoader());
  }

  /** Stands in for the pipeline or workflow graph that owns the hop and transform filters. */
  public static final class GraphPlugin {
    static GraphPlugin active;
    boolean used;

    public static GraphPlugin getInstance() {
      return active;
    }

    public boolean filterActions(String contextActionId, GraphContext context) {
      used = true;
      return this == context.graph && "show".equals(contextActionId);
    }
  }

  public static final class GraphContext extends BaseGuiContextHandler {
    final GraphPlugin graph;

    GraphContext(GraphPlugin graph) {
      this.graph = graph;
    }

    @Override
    public String getContextId() {
      return "graph";
    }

    public GraphPlugin getPipelineGraph() {
      return graph;
    }
  }

  public static final class SingletonPlugin {
    static final SingletonPlugin instance = new SingletonPlugin();
    boolean used;

    public static SingletonPlugin getInstance() {
      return instance;
    }

    public boolean filterActions(String contextActionId, EmptyContext context) {
      used = true;
      return "show".equals(contextActionId);
    }
  }

  /**
   * {@code getInstance()} returns null and this plugin has no zero-arg constructor, so the filter
   * cannot be created.
   */
  public static final class NullPlugin {
    @SuppressWarnings("unused")
    private final int marker;

    private NullPlugin(int marker) {
      this.marker = marker;
    }

    public static NullPlugin getInstance() {
      return null;
    }

    public boolean filterActions(String contextActionId, EmptyContext context) {
      return true;
    }
  }

  public static final class EmptyContext extends BaseGuiContextHandler {
    @Override
    public String getContextId() {
      return "empty";
    }
  }

  /** Same shape as a workflow graph: {@code getInstance()} is the active tab only. */
  public static final class WorkflowPlugin {
    static WorkflowPlugin active;
    boolean used;

    public static WorkflowPlugin getInstance() {
      return active;
    }

    public boolean filterActions(String contextActionId, WorkflowContext context) {
      used = true;
      return this == context.graph && "show".equals(contextActionId);
    }
  }

  public static final class WorkflowContext extends BaseGuiContextHandler {
    final WorkflowPlugin graph;

    WorkflowContext(WorkflowPlugin graph) {
      this.graph = graph;
    }

    @Override
    public String getContextId() {
      return "workflow";
    }

    public WorkflowPlugin getWorkflowGraph() {
      return graph;
    }
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
