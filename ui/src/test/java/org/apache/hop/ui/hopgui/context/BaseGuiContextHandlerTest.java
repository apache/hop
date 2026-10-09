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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionFilter;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.junit.jupiter.api.Test;

class BaseGuiContextHandlerTest {

  public static class SamplePlugin {
    public boolean filterAction(String actionId, ContextWithGetter context) {
      return !"blocked-action".equals(actionId);
    }

    public boolean filterSuperclass(String actionId, BaseGuiContextHandler<?> context) {
      return true;
    }
  }

  public static class SingletonPlugin {
    private static SingletonPlugin instance;

    public static SingletonPlugin getInstance() {
      return instance;
    }

    public boolean filterAction(String actionId, ContextWithoutGetter context) {
      return true;
    }
  }

  public static class PluginWithNullGetInstance {
    public static PluginWithNullGetInstance getInstance() {
      return null;
    }

    public boolean filterAction(String actionId, ContextWithoutGetter context) {
      return true;
    }
  }

  public static class ContextWithGetter extends BaseGuiContextHandler<ContextWithGetter>
      implements IGuiContextHandler {
    private final SamplePlugin samplePlugin;

    public ContextWithGetter(SamplePlugin samplePlugin) {
      this.samplePlugin = samplePlugin;
    }

    public SamplePlugin getSamplePlugin() {
      return samplePlugin;
    }

    @Override
    public List<GuiAction> getSupportedActions() {
      return Collections.emptyList();
    }

    @Override
    public String getContextId() {
      return "ContextWithGetter";
    }
  }

  public static class ContextWithFieldOnly extends BaseGuiContextHandler<ContextWithFieldOnly>
      implements IGuiContextHandler {
    @SuppressWarnings("unused")
    private final SamplePlugin samplePlugin;

    public ContextWithFieldOnly(SamplePlugin samplePlugin) {
      this.samplePlugin = samplePlugin;
    }

    @Override
    public List<GuiAction> getSupportedActions() {
      return Collections.emptyList();
    }

    @Override
    public String getContextId() {
      return "ContextWithFieldOnly";
    }
  }

  public static class ContextWithoutGetter extends BaseGuiContextHandler<ContextWithoutGetter>
      implements IGuiContextHandler {
    @Override
    public List<GuiAction> getSupportedActions() {
      return Collections.emptyList();
    }

    @Override
    public String getContextId() {
      return "ContextWithoutGetter";
    }
  }

  public static class ContextAsPlugin extends BaseGuiContextHandler<ContextAsPlugin>
      implements IGuiContextHandler {
    public boolean filterSelf(String actionId, ContextAsPlugin context) {
      return true;
    }

    @Override
    public List<GuiAction> getSupportedActions() {
      return Collections.emptyList();
    }

    @Override
    public String getContextId() {
      return "ContextAsPlugin";
    }
  }

  private GuiActionFilter createFilter(Class<?> pluginClass, String methodName) {
    GuiActionFilter filter = new GuiActionFilter();
    filter.setGuiPluginClassName(pluginClass.getName());
    filter.setGuiPluginMethodName(methodName);
    filter.setId(pluginClass.getName() + "." + methodName);
    filter.setClassLoader(getClass().getClassLoader());
    return filter;
  }

  @Test
  void getFilterObject_ResolvesFromContextGetter() throws Exception {
    SamplePlugin plugin = new SamplePlugin();
    ContextWithGetter context = new ContextWithGetter(plugin);

    GuiActionFilter filter = createFilter(SamplePlugin.class, "filterAction");
    Object resolved = context.getFilterObject(filter);

    assertSame(plugin, resolved);
  }

  @Test
  void getFilterObject_ResolvesFromContextField() throws Exception {
    SamplePlugin plugin = new SamplePlugin();
    ContextWithFieldOnly context = new ContextWithFieldOnly(plugin);

    GuiActionFilter filter = createFilter(SamplePlugin.class, "filterAction");
    Object resolved = context.getFilterObject(filter);

    assertSame(plugin, resolved);
  }

  @Test
  void getFilterObject_ResolvesFromContextItself() throws Exception {
    ContextAsPlugin context = new ContextAsPlugin();

    GuiActionFilter filter = createFilter(ContextAsPlugin.class, "filterSelf");
    Object resolved = context.getFilterObject(filter);

    assertSame(context, resolved);
  }

  @Test
  void getFilterObject_ResolvesFromGetInstance() throws Exception {
    SingletonPlugin plugin = new SingletonPlugin();
    SingletonPlugin.instance = plugin;
    try {
      ContextWithoutGetter context = new ContextWithoutGetter();
      GuiActionFilter filter = createFilter(SingletonPlugin.class, "filterAction");
      Object resolved = context.getFilterObject(filter);

      assertSame(plugin, resolved);
    } finally {
      SingletonPlugin.instance = null;
    }
  }

  @Test
  void getFilterObject_FallsBackToConstructorWhenGetInstanceReturnsNull() throws Exception {
    ContextWithoutGetter context = new ContextWithoutGetter();
    GuiActionFilter filter = createFilter(PluginWithNullGetInstance.class, "filterAction");
    Object resolved = context.getFilterObject(filter);

    assertNotNull(resolved);
    assertTrue(resolved instanceof PluginWithNullGetInstance);
  }

  @Test
  void getFilterMethod_MatchesSuperclassParameter() throws Exception {
    ContextWithGetter context = new ContextWithGetter(new SamplePlugin());
    GuiActionFilter filter = createFilter(SamplePlugin.class, "filterSuperclass");

    Method method = context.getFilterMethod(SamplePlugin.class, filter);
    assertNotNull(method);
    assertEquals("filterSuperclass", method.getName());
  }

  @Test
  void evaluateActionFilter_EvaluatesCorrectly() throws Exception {
    SamplePlugin plugin = new SamplePlugin();
    ContextWithGetter context = new ContextWithGetter(plugin);

    GuiActionFilter filter = createFilter(SamplePlugin.class, "filterAction");

    GuiAction allowedAction =
        new GuiAction(
            "allowed-action", GuiActionType.Info, "Allowed", "Tooltip", "image.svg", null);
    GuiAction blockedAction =
        new GuiAction(
            "blocked-action", GuiActionType.Info, "Blocked", "Tooltip", "image.svg", null);

    assertTrue(context.evaluateActionFilter(allowedAction, filter));
    assertFalse(context.evaluateActionFilter(blockedAction, filter));
  }
}
