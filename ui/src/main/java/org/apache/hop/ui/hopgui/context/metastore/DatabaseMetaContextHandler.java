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

package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
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
      ( shiftClicked, controlClicked, parameters ) -> hopGui.databaseMetaManager.newMetadata() );
    actions.add( newAction );

    GuiAction editAction = new GuiAction( "edit-database-meta", GuiActionType.Modify, "Edit Database Connection", "Edit a database connection", "ui/images/CNC.svg",
      (IGuiActionLambda<String>) (shiftClicked, controlClicked, parameters) -> {
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
