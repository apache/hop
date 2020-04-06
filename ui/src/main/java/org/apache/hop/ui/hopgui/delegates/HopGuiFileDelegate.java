package org.apache.hop.ui.hopgui.delegates;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HopGuiFileDelegate {

  private HopGui hopGui;

  public HopGuiFileDelegate( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  public IHopFileTypeHandler getActiveFileTypeHandler() {
    IHopFileTypeHandler typeHandler = hopGui.getActivePerspective().getActiveFileTypeHandler();
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

      fileOpen( filename );
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error opening file", e );
    }
  }

  public void fileOpen( String filename ) throws Exception {
    HopFileTypeRegistry fileRegistry = HopFileTypeRegistry.getInstance();

    IHopFileType hopFile = fileRegistry.findHopFileType( filename );
    if ( hopFile == null ) {
      throw new HopException( "We looked at " + fileRegistry.getFileTypes().size() + " different Hop GUI file types but none know how to open file '" + filename + "'" );
    }

    hopFile.openFile( hopGui, filename, hopGui.getVariables() );
    hopGui.handleFileCapabilities( hopFile );

    // Keep track of this...
    //
    hopGui.auditDelegate.registerEvent( "file", filename, "open" );
  }

  /**
   * We need to figure out which file is open at the given time so we can save it.
   * To do this we see which is the active perspective.
   * Then we ask the perspective for the shown/active file.
   * We then know the filter extension and name so we can show a dialog.
   * We can then also have the {@link IHopFileType to save the file.
   */
  public void fileSaveAs() {
    try {
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      if ( !fileType.hasCapability( IHopFileType.CAPABILITY_SAVE ) ) {
        return;
      }

      FileDialog fileDialog = new FileDialog( hopGui.getShell(), SWT.SAVE | SWT.OK | SWT.CANCEL );
      fileDialog.setText( "Save as ..." ); // TODO i18n
      fileDialog.setFilterNames( fileType.getFilterNames() );
      fileDialog.setFilterExtensions( fileType.getFilterExtensions() );

      AtomicBoolean doIt = new AtomicBoolean( true );
      try {
        HopGuiFileOpenExtension openExtension = new HopGuiFileOpenExtension( doIt, fileDialog, hopGui );
        ExtensionPointHandler.callExtensionPoint( hopGui.getLog(), HopExtensionPoint.PipelineBeforeOpen.id, openExtension );
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

      hopGui.auditDelegate.registerEvent( "file", filename, "save" );
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving file", e );
    }
  }

  public void fileSave() {
    try {
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      if ( fileType.hasCapability( IHopFileType.CAPABILITY_SAVE ) ) {
        if ( StringUtils.isEmpty( typeHandler.getFilename() ) ) {
          // Ask for the filename: saveAs
          //
          fileSaveAs();
        } else {
          typeHandler.save();
          hopGui.auditDelegate.registerEvent( "file", typeHandler.getFilename(), "save" );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving file", e );
    }
  }

  public boolean fileClose() {
    try {
      IHopPerspective perspective = hopGui.getActivePerspective();
      IHopFileTypeHandler typeHandler = getActiveFileTypeHandler();
      IHopFileType fileType = typeHandler.getFileType();
      if ( fileType.hasCapability( IHopFileType.CAPABILITY_CLOSE ) ) {
        perspective.remove( typeHandler );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving/closing file", e );
    }
    return false;
  }

  /**
   * When the app exits we need to see if all open files are saved in all perspectives...
   */
  public boolean fileExit() {
    for ( IHopPerspective perspective : hopGui.getPerspectiveManager().getPerspectives() ) {
      List<TabItemHandler> tabItemHandlers = perspective.getItems();
      if ( tabItemHandlers != null ) {
        for ( TabItemHandler tabItemHandler : tabItemHandlers ) {
          IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();
          if ( !typeHandler.isCloseable() ) {
            return false;
          }
        }
      }
    }
    // Also save all the open files in a list
    //
    hopGui.auditDelegate.writeLastOpenFiles();

    hopGui.getProps().saveProps();

    return true;
  }

  /**
   * Show all the recent files in a new dialog...
   */
  public void fileOpenRecent() {
    // Get the recent files for the active perspective...
    //
    IHopPerspective perspective = hopGui.getActivePerspective();
    try {
      // Let's limit ourselves to 100 operations...
      //
      List<AuditEvent> events = hopGui.auditDelegate.findEvents( "file", "open", 100 );
      Set<String> filenames = new HashSet<>();
      List<RowMetaAndData> rows = new ArrayList<>();
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "filename" ) );
      rowMeta.addValueMeta( new ValueMetaString( "operation" ) );
      rowMeta.addValueMeta( new ValueMetaString( "date" ) );

      for ( AuditEvent event : events ) {
        String filename = event.getName();
        if (!filenames.contains( filename )) {
          filenames.add( filename );
          String operation = event.getOperation();
          String dateString = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( event.getDate() );
          rows.add( new RowMetaAndData( rowMeta, new Object[] { filename, operation, dateString } ) );
        }
      }

      SelectRowDialog rowDialog = new SelectRowDialog( hopGui.getShell(), hopGui.getVariables(), SWT.NONE, rows );
      rowDialog.setTitle( "Select the file to open" );
      RowMetaAndData row = rowDialog.open();
      if ( row != null ) {
        String filename = row.getString( "filename", null );
        hopGui.fileDelegate.fileOpen( filename );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error getting list of recently opened files", e );
    }
  }
}
