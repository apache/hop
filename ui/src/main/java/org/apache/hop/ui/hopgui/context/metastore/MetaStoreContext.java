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

import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.metastore.HopMetaStoreGuiPluginDetails;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MetaStoreContext implements IActionContextHandlersProvider {

  private HopGui hopGui;
  private IMetaStore metaStore;

  public MetaStoreContext( HopGui hopGui, IMetaStore metaStore ) {
    this.hopGui = hopGui;
    this.metaStore = metaStore;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    Set<Class<? extends IHopMetaStoreElement>> metaClasses = guiRegistry.getMetaStoreTypeMap().keySet();
    for (Class<? extends IHopMetaStoreElement> metaClass : metaClasses) {
      HopMetaStoreGuiPluginDetails details = GuiRegistry.getInstance().getMetaStoreTypeMap().get( metaClass );
      try {
        IHopMetaStoreElement storeElement = metaClass.newInstance();
        MetaStoreFactory<IHopMetaStoreElement> factory = storeElement.getFactory( metaStore );

        handlers.add( new MetaStoreContextHandler( hopGui, metaStore, factory, metaClass, details ));

      } catch(Exception e) {
        hopGui.getLog().logError( "Error getting metastore context information from class: "+metaClass.getName(), e );
      }

    }

    return handlers;
  }
}
