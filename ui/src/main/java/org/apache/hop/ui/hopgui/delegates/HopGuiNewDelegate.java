package org.apache.hop.ui.hopgui.delegates;

import org.apache.hop.core.AddUndoPositionInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.job.JobMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;

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
    try {
      HopFileTypeRegistry registry = HopFileTypeRegistry.getInstance();
      List<String> fileTypes = HopFileTypeRegistry.getInstance().getFileTypeNames();
      if ( fileTypes.isEmpty() ) {
        return; // Nothing to see here, move along.
      }
      EnterSelectionDialog dialog = new EnterSelectionDialog( hopUi.getShell(), fileTypes.toArray( new String[ 0 ] ), "New!", "What do you want to create?" );
      String selection = dialog.open();
      if ( selection != null ) {
        HopFileTypeInterface fileType = registry.getFileTypeByName( selection );
        fileType.newFile( hopUi, hopUi.getVariableSpace() );
        hopUi.handleFileCapabilities( fileType );
      }
    } catch(Exception e) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error creating new file", e );
    }
  }
}
