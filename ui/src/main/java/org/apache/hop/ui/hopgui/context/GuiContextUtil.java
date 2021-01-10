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

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.ISingletonProvider;
import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GuiContextUtil {

  private final Map<String, ContextDialog> shellDialogMap = new HashMap<>();

  private static final ISingletonProvider PROVIDER;
  static {
    PROVIDER = (ISingletonProvider) ImplementationLoader.newInstance( GuiContextUtil.class );
  }
  public static final GuiContextUtil getInstance() {
    return (GuiContextUtil) PROVIDER.getInstanceInternal();
  }

  public final List<GuiAction> getContextActions( IActionContextHandlersProvider provider, GuiActionType actionType, String contextId ) {
    return filterHandlerActions( provider.getContextHandlers(), actionType, contextId );
  }

  /**
   * Filter out the actions with the given type, return a new list.
   *
   * @param guiActions The list of actions to filter
   * @param actionType The type to filter out
   * @return A new list with only the actions of the specified type
   */
  public final List<GuiAction> filterActions( List<GuiAction> guiActions, GuiActionType actionType ) {
    List<GuiAction> filtered = new ArrayList<>();
    for ( GuiAction guiAction : guiActions ) {
      if ( guiAction.getType().equals( actionType ) ) {
        filtered.add( guiAction );
      }
    }
    return filtered;
  }

  /**
   * Ask for all the actions from the list of context handlers. Then filter out the actions of a particular type.
   *
   * @param handlers
   * @param actionType
   * @return
   */
  public final List<GuiAction> filterHandlerActions( List<IGuiContextHandler> handlers, GuiActionType actionType, String contextId ) {
    List<GuiAction> filtered = new ArrayList<>();
    for ( IGuiContextHandler handler : handlers ) {
      filtered.addAll( filterActions( handler.getSupportedActions(), actionType ) );
    }
    return filtered;
  }

  public final void handleActionSelection( Shell parent, String message, IActionContextHandlersProvider provider, GuiActionType actionType, String contextId ) {
    handleActionSelection( parent, message, null, provider, actionType, contextId );
  }

  public final void handleActionSelection( Shell parent, String message, Point clickLocation, IActionContextHandlersProvider provider, GuiActionType actionType, String contextId ) {
    handleActionSelection( parent, message, clickLocation, provider, actionType, contextId, false );
  }

  public final void handleActionSelection( Shell parent, String message, Point clickLocation, IActionContextHandlersProvider provider, GuiActionType actionType, String contextId, boolean sortByName ) {
    // Get the list of create actions in the Hop UI context...
    //
    List<GuiAction> actions = getContextActions( provider, actionType, contextId );
    if ( actions.isEmpty() ) {
      return;
    }
    if ( sortByName ) {
      Collections.sort( actions, Comparator.comparing( GuiAction::getName ) );
    }

    handleActionSelection( parent, message, clickLocation, new GuiContextHandler( contextId, actions ) );
  }

  public boolean handleActionSelection( Shell parent, String message, IGuiContextHandler contextHandler ) {
    return handleActionSelection( parent, message, null, contextHandler );
  }

  /**
   * @param parent
   * @param message
   * @param clickLocation
   * @param contextHandler
   * @return true if the action dialog lost focus
   */
  public synchronized boolean handleActionSelection( Shell parent, String message, Point clickLocation, IGuiContextHandler contextHandler ) {
    List<GuiAction> actions = contextHandler.getSupportedActions();
    if ( actions.isEmpty() ) {
      return false;
    }

    try {

      synchronized ( parent ) {
        ContextDialog contextDialog = shellDialogMap.get( parent.getText() );
        if ( contextDialog != null ) {
          if ( !contextDialog.isDisposed() ) {
            contextDialog.dispose();
          }
          shellDialogMap.remove( parent.getText() );
          return true;
        }

        List<String> fileTypes = new ArrayList<>();
        for ( GuiAction action : actions ) {
          fileTypes.add( action.getType().name() + " - " + action.getName() + " : " + action.getTooltip() );
        }

        contextDialog = new ContextDialog( parent, message, clickLocation, actions, contextHandler.getContextId() );
        shellDialogMap.put( parent.getText(), contextDialog );
        GuiAction selectedAction = contextDialog.open();
        shellDialogMap.remove( parent.getText() );
        if ( selectedAction != null ) {
          IGuiActionLambda<?> actionLambda = selectedAction.getActionLambda();
          actionLambda.executeAction( contextDialog.isShiftClicked(), contextDialog.isCtrlClicked() );
        } else {
          return contextDialog.isFocusLost();
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( parent, "Error", "An error occurred executing action", e );
    }
    return false;
  }
}
