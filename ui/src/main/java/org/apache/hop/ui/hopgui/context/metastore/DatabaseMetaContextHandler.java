package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class DatabaseMetaContextHandler implements IGuiContextHandler {

  private final HopGui hopGui;

  public DatabaseMetaContextHandler( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();
    GuiAction newAction = new GuiAction( "create-database-meta", GuiActionType.Create, "Create Database Connection", "Create a new database connection", "ui/images/CNC.svg",
      ( parameters ) -> {
        hopGui.databaseMetaManager.newMetadata();
      } );
    actions.add( newAction );

    GuiAction editAction = new GuiAction( "edit-database-meta", GuiActionType.Modify, "Edit Database Connection", "Edit a database connection", "ui/images/CNC.svg",
      (IGuiActionLambda<String>) parameters -> {
        if ( parameters.length < 1 ) {
          throw new RuntimeException( "You need to give the name of the database connection to edit" );
        }
        String name = parameters[ 0 ];
        hopGui.databaseMetaManager.editMetadata( name );
      } );
    actions.add( editAction );
    return actions;
  }
}
