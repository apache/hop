package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;

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
    // Click : shell location + 50, 50
    //
    int x = 50 + hopUi.getShell().getLocation().x;
    int y = 50 + hopUi.getShell().getLocation().y;

    GuiContextUtil.handleActionSelection( hopUi.getShell(), "Select the action...", new Point(x, y), hopUi, GuiActionType.Create );
  }
}
