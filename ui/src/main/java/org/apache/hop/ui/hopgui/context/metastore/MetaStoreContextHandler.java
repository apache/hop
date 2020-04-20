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
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.ui.core.metastore.MetaStoreManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreContextHandler implements IGuiContextHandler {

  private HopGui hopGui;
  private IMetaStore metaStore;
  private MetaStoreFactory<IHopMetaStoreElement> factory;
  private Class<? extends IHopMetaStoreElement> metaStoreElementClass;
  private GuiMetaStoreElement guiMetaStoreElement;
  private MetaStoreManager<? extends IHopMetaStoreElement> metaStoreManager;

  public MetaStoreContextHandler( HopGui hopGui, IMetaStore metaStore, MetaStoreFactory<IHopMetaStoreElement> factory,
                                  Class<? extends IHopMetaStoreElement> metaStoreElementClass, GuiMetaStoreElement guiMetaStoreElement ) {
    this.hopGui = hopGui;
    this.metaStore = metaStore;
    this.factory = factory;
    this.metaStoreElementClass = metaStoreElementClass;
    this.guiMetaStoreElement = guiMetaStoreElement;
    this.metaStoreManager = new MetaStoreManager<>( hopGui.getVariables(), metaStore, metaStoreElementClass );


  }

  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    GuiAction newAction = new GuiAction(
      "Create: "+guiMetaStoreElement.name(),
      GuiActionType.Create,
      "Create "+guiMetaStoreElement.name(),
      "Create a new: "+guiMetaStoreElement.description(),
      guiMetaStoreElement.iconImage(),
      ( shiftClicked, controlClicked, parameters ) -> metaStoreManager.newMetadata() );
    newAction.setClassLoader( metaStoreElementClass.getClassLoader() );
    actions.add( newAction );

    GuiAction editAction = new GuiAction(
      "Edit: "+guiMetaStoreElement.name(),
      GuiActionType.Modify,
      guiMetaStoreElement.name(),
      guiMetaStoreElement.description(),
      guiMetaStoreElement.iconImage(),
      (shiftClicked, controlClicked, parameters) -> metaStoreManager.editMetadata() );
    editAction.setClassLoader( metaStoreElementClass.getClassLoader() );
    actions.add( editAction );

    GuiAction deleteAction = new GuiAction(
      "Delete "+guiMetaStoreElement.name(),
      GuiActionType.Delete,
      guiMetaStoreElement.name(),
      guiMetaStoreElement.description(),
      guiMetaStoreElement.iconImage(),
       (shiftClicked, controlClicked, parameters) -> metaStoreManager.editMetadata() );
    deleteAction.setClassLoader( metaStoreElementClass.getClassLoader() );
    actions.add( deleteAction );

    return actions;
  }
}
