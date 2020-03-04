package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreContext implements IActionContextHandlersProvider {

  private IMetaStore metaStore;

  public MetaStoreContext( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  // TODO: give back a context handler for every element type
  // TODO: somehow find and register every metastore type through a plugin system somehow.
  //
  @Override public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>(  );
  }
}
