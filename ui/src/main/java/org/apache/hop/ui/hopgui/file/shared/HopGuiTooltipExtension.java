package org.apache.hop.ui.hopgui.file.shared;

import org.apache.hop.core.gui.AreaOwner;
import org.eclipse.swt.graphics.Image;

public class HopGuiTooltipExtension {
  public int x;
  public int y;
  public int screenX;
  public int screenY;

  public AreaOwner areaOwner;

  public Image tooltipImage;
  public StringBuilder tip;

  public HopGuiTooltipExtension( int x, int y, int screenX, int screenY, AreaOwner areaOwner, StringBuilder tip ) {
    this.x = x;
    this.y = y;
    this.screenX = screenX;
    this.screenY = screenY;
    this.areaOwner = areaOwner;
    this.tip = tip;
  }
}
