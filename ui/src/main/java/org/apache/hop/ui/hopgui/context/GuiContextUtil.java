package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiAction;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.List;

public class GuiContextUtil {

  public static final List<IGuiAction> getContextActions(IActionContextHandlersProvider provider, GuiActionType actionType) {
    return GuiContextUtil.filterHandlerActions(provider.getContextHandlers(), actionType );
  }

  /**
   * Filter out the actions with the given type, return a new list.
   * @param guiActions The list of actions to filter
   * @param actionType The type to filter out
   * @return A new list with only the actions of the specified type
   */
  public static final List<IGuiAction> filterActions( List<IGuiAction> guiActions, GuiActionType actionType) {
    List<IGuiAction> filtered = new ArrayList<>( );
    for (IGuiAction guiAction : guiActions) {
      if (guiAction.getType().equals( actionType )) {
        filtered.add( guiAction );
      }
    }
    return filtered;
  }

  /**
   * Ask for all the actions from the list of context handlers. Then filter out the actions of a particular type.
   * @param handlers
   * @param actionType
   * @return
   */
  public static final List<IGuiAction> filterHandlerActions(List<IGuiContextHandler> handlers, GuiActionType actionType) {
    List<IGuiAction> filtered = new ArrayList<>( );
    for (IGuiContextHandler handler : handlers) {
      filtered.addAll( filterActions(handler.getSupportedActions(), actionType) );
    }
    return filtered;
  }

  public static final void handleActionSelection( Shell parent, IActionContextHandlersProvider provider, GuiActionType actionType) {
    try {
      // Get the list of create actions in the Hop UI context...
      //
      List<IGuiAction> actions = GuiContextUtil.getContextActions( provider, actionType );
      if (actions.isEmpty()) {
        return;
      }

      List<String> fileTypes = new ArrayList<>(  );
      for (IGuiAction action : actions) {
        fileTypes.add(action.getTooltip());
      }

      EnterSelectionDialog dialog = new EnterSelectionDialog( parent, fileTypes.toArray( new String[ 0 ] ), "Action: "+actionType.name(), "What do you want to do?" );
      String selection = dialog.open();
      if ( selection != null ) {
        IGuiAction action = actions.get(dialog.getSelectionNr());

        System.out.println("Selected action: "+action.getType().name()+" " + action.getName()+" : "+action.getTooltip());

        action.getActionLambda().executeAction();
      }
    } catch(Exception e) {
      new ErrorDialog( parent, "Error", "Error handling action type "+actionType.name(), e );
    }
  }
}
