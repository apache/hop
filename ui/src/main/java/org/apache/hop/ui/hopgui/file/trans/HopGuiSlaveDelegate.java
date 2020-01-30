package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopui.HopUi;

public class HopGuiSlaveDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  private HopGui hopUi;
  private HopFileTypeHandlerInterface handler;

  public HopGuiSlaveDelegate( HopGui hopGui, HopFileTypeHandlerInterface handler ) {
    this.hopUi = hopGui;
    this.handler = handler;
  }
}
