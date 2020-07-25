package org.apache.hop.ui.core.gui;

import org.apache.hop.ui.hopgui.ISingletonProvider;

public class GuiResourceImpl implements ISingletonProvider {
  private static GuiResource instance;
  public Object getInstanceInternal() {
    if ( instance == null ) {
      instance = new GuiResource();
    }
    return instance;
  }
}
