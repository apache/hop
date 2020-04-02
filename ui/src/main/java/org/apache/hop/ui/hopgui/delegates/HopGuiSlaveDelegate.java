package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

public class HopGuiSlaveDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGui hopUi;
  private IHopFileTypeHandler handler;

  public HopGuiSlaveDelegate( HopGui hopGui, IHopFileTypeHandler handler ) {
    this.hopUi = hopGui;
    this.handler = handler;
  }
}
