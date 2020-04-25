/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GuiContextUtil {

  public static final List<GuiAction> getContextActions( IActionContextHandlersProvider provider, GuiActionType actionType ) {
    return GuiContextUtil.filterHandlerActions( provider.getContextHandlers(), actionType );
  }

  /**
   * Filter out the actions with the given type, return a new list.
   *
   * @param guiActions The list of actions to filter
   * @param actionType The type to filter out
   * @return A new list with only the actions of the specified type
   */
  public static final List<GuiAction> filterActions( List<GuiAction> guiActions, GuiActionType actionType ) {
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
  public static final List<GuiAction> filterHandlerActions( List<IGuiContextHandler> handlers, GuiActionType actionType ) {
    List<GuiAction> filtered = new ArrayList<>();
    for ( IGuiContextHandler handler : handlers ) {
      filtered.addAll( filterActions( handler.getSupportedActions(), actionType ) );
    }
    return filtered;
  }

  public static final void handleActionSelection( Shell parent, String message, IActionContextHandlersProvider provider, GuiActionType actionType ) {
    handleActionSelection( parent, message, null, provider, actionType );
  }

  public static final void handleActionSelection( Shell parent, String message, Point clickLocation, IActionContextHandlersProvider provider, GuiActionType actionType ) {
    handleActionSelection( parent, message, clickLocation, provider, actionType, false );
  }

    public static final void handleActionSelection( Shell parent, String message, Point clickLocation, IActionContextHandlersProvider provider, GuiActionType actionType, boolean sortByName ) {
    // Get the list of create actions in the Hop UI context...
    //
    List<GuiAction> actions = GuiContextUtil.getContextActions( provider, actionType );
    if ( actions.isEmpty() ) {
      return;
    }
    if (sortByName) {
      Collections.sort( actions, Comparator.comparing( GuiAction::getName ) );
    }

    handleActionSelection( parent, message, clickLocation, actions );
  }

  public static boolean handleActionSelection( Shell parent, String message, List<GuiAction> actions ) {
    return handleActionSelection( parent, message, null, actions );
  }

    /**
     * @param parent
     * @param message
     * @param clickLocation
     * @param actions
     * @return true if the action dialog lost focus
     */
  public static boolean handleActionSelection( Shell parent, String message, Point clickLocation, List<GuiAction> actions ) {
    if ( actions.isEmpty() ) {
      return false;
    }

    try {

      List<String> fileTypes = new ArrayList<>();
      for ( GuiAction action : actions ) {
        fileTypes.add( action.getType().name() + " - " + action.getName() + " : " + action.getTooltip() );
      }

      ContextDialog contextDialog = new ContextDialog( parent, message, clickLocation, actions );
      GuiAction selectedAction = contextDialog.open();
      if ( selectedAction != null ) {
        IGuiActionLambda<?> actionLambda = selectedAction.getActionLambda();
        actionLambda.executeAction( contextDialog.isShiftClicked(), contextDialog.isCtrlClicked() );
      } else {
        return contextDialog.isFocusLost();
      }
    } catch ( Exception e ) {
      new ErrorDialog( parent, "Error", "An error occurred executing action", e );
    }
    return false;
  }
}
