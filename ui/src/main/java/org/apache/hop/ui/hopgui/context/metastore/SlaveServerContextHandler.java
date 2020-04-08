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

import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class SlaveServerContextHandler implements IGuiContextHandler {

  private final HopGui hopGui;

  public SlaveServerContextHandler( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();
    GuiAction newAction = new GuiAction( "create-slave-server", GuiActionType.Create, "Create Slave Server", "Create a new slave server", "ui/images/slave.svg",
      (IGuiActionLambda<Object>) (shiftClicked, controlClicked, params) -> hopGui.slaveServerManager.newMetadata()
    );
    actions.add( newAction );

    GuiAction editAction = new GuiAction( "edit-slave-server", GuiActionType.Modify, "Edit Slave Server", "Edit a slave server", "ui/images/slave.svg",
      (IGuiActionLambda<String>) (shiftClicked, controlClicked, names ) -> {
        if ( names.length < 1 ) {
          throw new RuntimeException( "You need to give the name of the slave server to edit" );
        }
        hopGui.slaveServerManager.editMetadata( names[ 0 ] );
      }
    );
    actions.add( editAction );
    return actions;
  }
}
