package org.apache.hop.ui.hopgui;

public class HopGuiImpl implements ISingletonProvider {
  private static HopGui instance;
  public Object getInstanceInternal() {
    if ( instance == null ) {
      instance = new HopGui();
    }
    return instance;
  }
}
