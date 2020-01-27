package org.apache.hop.ui.hopgui.delegates;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;

import java.util.concurrent.atomic.AtomicBoolean;

public class HopGuiFileDelegate {

  private HopGui hopGui;

  public HopGuiFileDelegate( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    IHopPerspective perspective = hopGui.getActivePerspective();
    if ( perspective == null ) {
      return null; // should happen rarely if ever
    }
    HopFileTypeHandlerInterface typeHandler = perspective.getActiveFileTypeHandler();
    return typeHandler;
  }

  public void fileOpen() {
    try {
      // Ask for the file name
      // Check in the registry for extensions and names...
      //
      HopFileTypeRegistry fileRegistry = HopFileTypeRegistry.getInstance();

      FileDialog fileDialog = new FileDialog( hopGui.getShell(), SWT.OPEN | SWT.OK | SWT.CANCEL );
      fileDialog.setText( "Open file..." );  // TODO i18n
      fileDialog.setFilterNames( fileRegistry.getFilterNames() );
      fileDialog.setFilterExtensions( fileRegistry.getFilterExtensions() );

      AtomicBoolean doIt = new AtomicBoolean( true );
      try {
        HopGuiFileOpenExtension openExtension = new HopGuiFileOpenExtension( doIt, fileDialog, hopGui );
        ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), HopGuiExtensionPoint.HopGuiFileOpenDialog.id, openExtension );
      } catch ( Exception e ) {
        throw new HopException( "Error calling extension point on the file dialog", e );
      }
      if ( !doIt.get() ) {
        return;
      }

      String filename = fileDialog.open();
      if ( filename == null ) {
        return;
      }

      HopFileTypeInterface hopFile = fileRegistry.findHopFileType( filename );
      if (hopFile==null) {
        throw new HopException( "We looked at "+fileRegistry.getFileTypes().size()+" different Hop GUI file types but none know how to open file '"+filename+"'" );
      }

      HopFileTypeHandlerInterface hopFileTypeHandlerInterface = hopFile.openFile( hopGui, filename, hopGui.getVariableSpace() );
      hopGui.handleFileCapabilities(hopFile);

    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error opening file", e );
    }
  }

  /**
   * We need to figure out which file is open at the given time so we can save it.
   * To do this we see which is the active perspective.
   * Then we ask the perspective for the shown/active file.
   * We then know the filter extension and name so we can show a dialog.
   * We can then also have the {@link HopFileTypeInterface to save the file.
   *
   */
  public void fileSaveAs() {
    try {
      HopFileTypeHandlerInterface typeHandler = getActiveFileTypeHandler();
      if ( typeHandler == null ) {
        return; // Nothing to save?
      }
      HopFileTypeInterface fileType = typeHandler.getFileType();

      FileDialog fileDialog = new FileDialog( hopGui.getShell(), SWT.SAVE | SWT.OK | SWT.CANCEL );
      fileDialog.setText( "Save as ..." ); // TODO i18n
      fileDialog.setFilterNames( fileType.getFilterNames() );
      fileDialog.setFilterExtensions( fileType.getFilterExtensions() );

      AtomicBoolean doIt = new AtomicBoolean( true );
      try {
        HopGuiFileOpenExtension openExtension = new HopGuiFileOpenExtension( doIt, fileDialog, hopGui );
        ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), HopExtensionPoint.TransBeforeOpen.id, openExtension );
      } catch ( Exception e ) {
        throw new HopException( "Error calling extension point on the file dialog", e );
      }
      if ( !doIt.get() ) {
        return;
      }

      String filename = fileDialog.open();
      if ( filename == null ) {
        return;
      }

      typeHandler.saveAs( filename );
    } catch(Exception e) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving file", e );
    }
  }

  public void fileSave() {
    try {
      HopFileTypeHandlerInterface typeHandler = getActiveFileTypeHandler();
      if (typeHandler!=null) {

        if ( StringUtils.isEmpty(typeHandler.getFilename())) {
          // Ask for the filename: saveAs
          //
          fileSaveAs();
        } else {
          typeHandler.save();
        }
      }
    } catch(Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error saving file", e);
    }
  }

  public boolean fileClose() {
    try {
      IHopPerspective perspective = hopGui.getActivePerspective();
      if (perspective==null) {
        return false; // Not sure this is a possible scenario
      }
      HopFileTypeHandlerInterface typeHandler = getActiveFileTypeHandler();
      if (typeHandler!=null) {
        typeHandler.isCloseable();
        return true;
      }
    } catch(Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error saving/closing file", e);
    }
    return false;
  }
}
