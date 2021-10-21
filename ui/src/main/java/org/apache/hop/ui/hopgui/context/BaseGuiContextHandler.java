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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionFilter;
import org.apache.hop.core.logging.LogChannel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

    if (actions == null || actions.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the list of filters for the parent context ID...
    //
    List<GuiActionFilter> actionFilters =
        GuiRegistry.getInstance().getGuiContextActionFilters(getContextId());

    // Evaluate all the actions and see if any of the filters remove it from the list...
    //
    List<GuiAction> filteredActions = new ArrayList<>();
    for (GuiAction action : actions) {
      boolean retain = true;
      if (actionFilters != null) {
        for (GuiActionFilter actionFilter : actionFilters) {
          boolean retainAction = false;
          try {
            retainAction = evaluateActionFilter(action, actionFilter);
            if (!retainAction) {
              retain = false;
              break;
            }
          } catch (HopException e) {
            LogChannel.UI.logError(
                "Error filtering out action "
                    + action.getId()
                    + " with filter "
                    + actionFilter.getId(),
                e);
          }
        }
      }
      if (retain) {
        filteredActions.add(action);
      }
    }
    actions = filteredActions;

    if (sortActionsById) {
      Collections.sort(actions, Comparator.comparing(GuiAction::getId));
    }

    return actions;
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

      // Get (or create) the instance of that class...
      //
      Object guiPlugin;
      try {
        Method getInstanceMethod = filterClass.getDeclaredMethod("getInstance");
        guiPlugin = getInstanceMethod.invoke(null, null);
      } catch (Exception nsme) {
        // On the rebound we'll try to simply construct a new instance...
        // This makes the plugins even simpler.
        //
        try {
          guiPlugin = filterClass.newInstance();
        } catch (Exception e) {
          throw nsme;
        }
      }

      return guiPlugin;
    } catch (Exception e) {
      throw new HopException(
          "Error finding, loading or creating object for action filter " + actionFilter.getId(), e);
    }
  }

  public Method getFilterMethod(Class<?> filterClass, GuiActionFilter actionFilter)
      throws HopException {
    try {

      Method method =
          filterClass.getMethod(actionFilter.getGuiPluginMethodName(), String.class, getClass());
      if (method == null) {
        throw new HopException(
            "Couldn't find method "
                + actionFilter.getGuiPluginMethodName()
                + " class "
                + actionFilter.getGuiPluginClassName()
                + " for action filter "
                + actionFilter.getId());
      }
      return method;
    } catch (Exception e) {
      throw new HopException("Error finding action filter method " + actionFilter.getId(), e);
    }
  }

  public boolean evaluateActionFilter(GuiAction action, GuiActionFilter actionFilter)
      throws HopException {

    try {
      Object guiPlugin = getFilterObject(actionFilter);
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
}
