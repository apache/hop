package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiRegistry;
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
      GuiMetaStoreElement guiMetaStoreElement = GuiRegistry.getInstance().getMetaStoreTypeMap().get( metaClass );
      try {
        IHopMetaStoreElement storeElement = metaClass.newInstance();
        MetaStoreFactory<IHopMetaStoreElement> factory = storeElement.getFactory( metaStore );

        handlers.add( new MetaStoreContextHandler( hopGui, metaStore, factory, metaClass, guiMetaStoreElement ));

      } catch(Exception e) {
        hopGui.getLog().logError( "Error getting metastore context information from class: "+metaClass.getName(), e );
      }

    }

    return handlers;
  }
}
