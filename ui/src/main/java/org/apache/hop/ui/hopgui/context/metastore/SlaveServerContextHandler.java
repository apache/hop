package org.apache.hop.ui.hopgui.context.metastore;

import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiAction;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class SlaveServerContextHandler implements IGuiContextHandler {

  private final HopGui hopGui;

  public SlaveServerContextHandler( HopGui hopGui) {
    this.hopGui = hopGui;
  }

  @Override public List<IGuiAction> getSupportedActions() {
    List<IGuiAction> actions = new ArrayList<>();
    IGuiAction newAction = new GuiAction( "create-slave-server", GuiActionType.Create, "Create Slave Server", "Create a new slave server", "ui/images/slave.svg",
      (IGuiActionLambda<Object>) params -> hopGui.slaveServerManager.newMetadata()
    );
    actions.add(newAction);

    IGuiAction editAction = new GuiAction( "edit-slave-server", GuiActionType.Modify, "Edit Slave Server", "Edit a slave server", "ui/images/slave.svg",
      (IGuiActionLambda<String>) names -> {
        if (names.length<1) {
          throw new RuntimeException( "You need to give the name of the slave server to edit" );
        }
        hopGui.slaveServerManager.editMetadata(names[0]);
      }
    );
    actions.add(editAction);
    return actions;
  }
}
