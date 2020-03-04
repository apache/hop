package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.AddUndoPositionInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiAction;
import org.apache.hop.job.JobMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.context.metastore.MetaStoreContext;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;

import java.util.ArrayList;
import java.util.List;

public class HopGuiNewDelegate {
  private HopGui hopUi;

  public HopGuiNewDelegate( HopGui hopGui ) {
    this.hopUi = hopGui;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopGui to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

  /**
   * Create a new file, ask which type of file or object you want created

   */
  public void fileNew() {
    GuiContextUtil.handleActionSelection( hopUi.getShell(), hopUi, GuiActionType.Create );
  }
}
