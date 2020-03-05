package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreContext implements IActionContextHandlersProvider {

  private HopGui hopGui;
  private IMetaStore metaStore;

  public MetaStoreContext( HopGui hopGui, IMetaStore metaStore ) {
    this.hopGui = hopGui;
    this.metaStore = metaStore;
  }

  // TODO: give back a context handler for every element type
  // TODO: somehow find and register every metastore type through a plugin system somehow.
  //
  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>(  );

    // Register a few defaults, manually for now.
    //  - DatabaseMeta
    //  - SlaveServer
    //
    handlers.add( new DatabaseMetaContextHandler( hopGui ) );
    handlers.add( new SlaveServerContextHandler( hopGui ) );

    return handlers;
  }
}
