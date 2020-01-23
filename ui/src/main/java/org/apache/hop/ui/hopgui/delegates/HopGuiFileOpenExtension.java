package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.FileDialog;

import java.util.concurrent.atomic.AtomicBoolean;

public class HopGuiFileOpenExtension {
  public AtomicBoolean doIt;
  public FileDialog fileDialog;
  public HopGui hopGui;

  public HopGuiFileOpenExtension( AtomicBoolean doIt, FileDialog fileDialog, HopGui hopGui ) {
    this.doIt = doIt;
    this.fileDialog = fileDialog;
    this.hopGui = hopGui;
  }

  /**
   * Gets doIt
   *
   * @return value of doIt
   */
  public AtomicBoolean getDoIt() {
    return doIt;
  }

  /**
   * @param doIt The doIt to set
   */
  public void setDoIt( AtomicBoolean doIt ) {
    this.doIt = doIt;
  }

  /**
   * Gets fileDialog
   *
   * @return value of fileDialog
   */
  public FileDialog getFileDialog() {
    return fileDialog;
  }

  /**
   * @param fileDialog The fileDialog to set
   */
  public void setFileDialog( FileDialog fileDialog ) {
    this.fileDialog = fileDialog;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }
}
