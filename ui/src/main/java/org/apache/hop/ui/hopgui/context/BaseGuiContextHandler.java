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

package org.apache.hop.ui.hopgui.context;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionFilter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;

public abstract class BaseGuiContextHandler<T extends IGuiContextHandler> {

  public static final String CONTEXT_ID = "HopGuiPipelineTransformContext";

  public BaseGuiContextHandler() {}

  public abstract String getContextId();

  /**
   * Create a list of supported actions from the plugin GUI registry. If this is indicated as such
   * the actions will be sorted by ID to deliver a consistent user experience. The actions are
   * picked up from GuiContextAction annotations in the GuiPlugin classes.
   *
   * @param sortActionsById true if the actions need to be sorted by ID
   * @return The list of supported actions
   */
  protected List<GuiAction> getPluginActions(boolean sortActionsById) {

    // Get the actions from the plugins...
    //
    List<GuiAction> actions = GuiRegistry.getInstance().getGuiContextActions(getContextId());

    if (Utils.isEmpty(actions)) {
      return Collections.emptyList();
    }

    // Get the list of filters for the parent context ID...
    //
    List<GuiActionFilter> actionFilters =
        GuiRegistry.getInstance().getGuiContextActionFilters(getContextId());

    // A filter that cannot be created (for example getInstance() returned null) used to throw
    // once per action and print a full stack trace every time. Resolve each filter once. A
    // failure leaves every action visible, which is what the per-action catch did before.
    //
    List<ResolvedActionFilter> resolvedFilters = resolveActionFilters(actionFilters);

    List<GuiAction> filteredActions = new ArrayList<>();
    for (GuiAction action : actions) {
      if (isActionRetained(action, resolvedFilters)) {
        filteredActions.add(action);
      }
    }
    actions = filteredActions;

    if (sortActionsById) {
      Collections.sort(actions, Comparator.comparing(GuiAction::getId));
    }

    return actions;
  }

  private List<ResolvedActionFilter> resolveActionFilters(List<GuiActionFilter> actionFilters) {
    if (actionFilters == null) {
      return Collections.emptyList();
    }
    List<ResolvedActionFilter> resolvedFilters = new ArrayList<>();
    for (GuiActionFilter actionFilter : actionFilters) {
      try {
        Object guiPlugin = getFilterObject(actionFilter);
        Method method = getFilterMethod(guiPlugin.getClass(), actionFilter);
        resolvedFilters.add(new ResolvedActionFilter(actionFilter, guiPlugin, method));
      } catch (HopException e) {
        LogChannel.UI.logError(
            "Error preparing action filter "
                + actionFilter.getId()
                + ". Actions stay visible because this filter could not be created.",
            e);
      }
    }
    return resolvedFilters;
  }

  private boolean isActionRetained(GuiAction action, List<ResolvedActionFilter> resolvedFilters) {
    for (ResolvedActionFilter resolvedFilter : resolvedFilters) {
      try {
        boolean retainAction =
            (boolean) resolvedFilter.method.invoke(resolvedFilter.guiPlugin, action.getId(), this);
        if (!retainAction) {
          return false;
        }
      } catch (Exception e) {
        LogChannel.UI.logError(
            "Error filtering out action "
                + action.getId()
                + " with filter "
                + resolvedFilter.actionFilter.getId(),
            e);
      }
    }
    return true;
  }

  public ClassLoader findClassLoader(GuiActionFilter actionFilter) {
    if (actionFilter.getClassLoader() != null) {
      return actionFilter.getClassLoader();
    }
    return getClass().getClassLoader();
  }

  public Object getFilterObject(GuiActionFilter actionFilter) throws HopException {
    try {
      ClassLoader classLoader = findClassLoader(actionFilter);

      // Find the class that contains the filter method...
      //
      Class<?> filterClass = classLoader.loadClass(actionFilter.getGuiPluginClassName());
      if (filterClass == null) {
        throw new HopException(
            "Couldn't load class "
                + actionFilter.getGuiPluginClassName()
                + " for action filter "
                + actionFilter.getId());
      }

      // The handler can be the plugin itself.
      //
      if (filterClass.isInstance(this)) {
        return this;
      }

      // The context already holds the graph that was clicked. getInstance() only returns the
      // active tab, and that is null when a test (or another caller) drives a graph that is not
      // the active file. Prefer that graph over any other getter and over getInstance().
      //
      Object guiPlugin = pluginInstanceFromContext(filterClass);
      if (guiPlugin == null) {
        guiPlugin = pluginFromMethod(filterClass);
      }
      if (guiPlugin == null) {
        guiPlugin = pluginFromField(filterClass);
      }
      if (guiPlugin == null) {
        guiPlugin = newFilterInstance(filterClass);
      }

      if (guiPlugin == null) {
        throw new HopException(
            "Couldn't find, load or create object for action filter " + actionFilter.getId());
      }

      return guiPlugin;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Error finding, loading or creating object for action filter " + actionFilter.getId(), e);
    }
  }

  /**
   * The pipeline or workflow graph this context was opened on, when that graph owns the filter.
   * Other plugins (unit tests, drill-down, and so on) are not held here and still use {@code
   * getInstance()}.
   */
  protected Object pluginInstanceFromContext(Class<?> pluginClass) {
    Object pipelineGraph = graphFromGetter("getPipelineGraph");
    if (pluginClass.isInstance(pipelineGraph)) {
      return pipelineGraph;
    }
    Object workflowGraph = graphFromGetter("getWorkflowGraph");
    if (pluginClass.isInstance(workflowGraph)) {
      return workflowGraph;
    }
    return null;
  }

  private Object graphFromGetter(String getterName) {
    try {
      return getClass().getMethod(getterName).invoke(this);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  /** A no-arg method on this handler whose return type is the filter class. */
  private Object pluginFromMethod(Class<?> filterClass) {
    for (Method method : getClass().getMethods()) {
      if (method.getParameterCount() == 0 && filterClass.isAssignableFrom(method.getReturnType())) {
        try {
          Object candidate = method.invoke(this);
          if (candidate != null) {
            return candidate;
          }
        } catch (Exception ignored) {
          // Continue searching
        }
      }
    }
    return null;
  }

  /** A field on this handler whose type is the filter class. */
  private Object pluginFromField(Class<?> filterClass) {
    Class<?> clazz = getClass();
    while (clazz != null && clazz != Object.class) {
      for (Field field : clazz.getDeclaredFields()) {
        if (filterClass.isAssignableFrom(field.getType())) {
          try {
            field.setAccessible(true);
            Object candidate = field.get(this);
            if (candidate != null) {
              return candidate;
            }
          } catch (Exception ignored) {
            // Continue searching
          }
        }
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  /**
   * {@code getInstance()} when it returns a plugin, otherwise a new instance. A null singleton is
   * not a plugin, so the constructor is still tried.
   */
  private Object newFilterInstance(Class<?> filterClass) {
    Object guiPlugin = null;
    try {
      Method getInstanceMethod = filterClass.getDeclaredMethod("getInstance");
      guiPlugin = getInstanceMethod.invoke(null, (Object[]) null);
    } catch (Exception ignored) {
      // No singleton. A new instance is just as usable for these filters.
    }
    if (guiPlugin == null) {
      try {
        Constructor<?> constructor = filterClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        guiPlugin = constructor.newInstance();
      } catch (Exception ignored) {
        // No instance available.
      }
    }
    return guiPlugin;
  }

  public Method getFilterMethod(Class<?> filterClass, GuiActionFilter actionFilter)
      throws HopException {
    try {
      try {
        return filterClass.getMethod(
            actionFilter.getGuiPluginMethodName(), String.class, getClass());
      } catch (NoSuchMethodException nsme) {
        for (Method method : filterClass.getMethods()) {
          if (method.getName().equals(actionFilter.getGuiPluginMethodName())
              && method.getParameterCount() == 2
              && method.getParameterTypes()[0].isAssignableFrom(String.class)
              && method.getParameterTypes()[1].isAssignableFrom(getClass())) {
            return method;
          }
        }
        throw nsme;
      }
    } catch (Exception e) {
      throw new HopException("Error finding action filter method " + actionFilter.getId(), e);
    }
  }

  public boolean evaluateActionFilter(GuiAction action, GuiActionFilter actionFilter)
      throws HopException {

    try {
      Object guiPlugin = getFilterObject(actionFilter);
      if (guiPlugin == null) {
        throw new HopException(
            "Couldn't find, load or create object for action filter " + actionFilter.getId());
      }
      Method method = getFilterMethod(guiPlugin.getClass(), actionFilter);

      // Invoke the action filter method...
      //
      return (boolean) method.invoke(guiPlugin, action.getId(), this);

    } catch (Exception e) {
      throw new HopException(
          "Error filtering out action with ID "
              + action.getId()
              + " against filter "
              + actionFilter.getId(),
          e);
    }
  }

  /** A filter whose plugin instance and method were loaded once for this context. */
  private static final class ResolvedActionFilter {
    private final GuiActionFilter actionFilter;
    private final Object guiPlugin;
    private final Method method;

    private ResolvedActionFilter(GuiActionFilter actionFilter, Object guiPlugin, Method method) {
      this.actionFilter = actionFilter;
      this.guiPlugin = guiPlugin;
      this.method = method;
    }
  }
}
