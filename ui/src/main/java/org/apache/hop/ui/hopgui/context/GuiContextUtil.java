package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
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

  public static final void handleActionSelection( Shell parent, String message, Point clickLocation, IActionContextHandlersProvider provider, GuiActionType actionType ) {
    // Get the list of create actions in the Hop UI context...
    //
    List<GuiAction> actions = GuiContextUtil.getContextActions( provider, actionType );
    if ( actions.isEmpty() ) {
      return;
    }

    handleActionSelection( parent, message, clickLocation, actions );
  }

  public static void handleActionSelection( Shell parent, String message, Point clickLocation, List<GuiAction> actions ) {
    if ( actions.isEmpty() ) {
      return;
    }

    try {

      List<String> fileTypes = new ArrayList<>();
      for ( GuiAction action : actions ) {
        fileTypes.add( action.getType().name() + " - " + action.getName() + " : " + action.getTooltip() );
      }

      ContextDialog contextDialog = new ContextDialog( parent, message, clickLocation, actions );
      GuiAction selectedAction = contextDialog.open();
      if ( selectedAction != null ) {
        selectedAction.getActionLambda().executeAction(contextDialog.isShiftClicked(), contextDialog.isCtrlClicked());
      }
    } catch ( Exception e ) {
      new ErrorDialog( parent, "Error", "An error occurred executing action", e );
    }

  }
}
