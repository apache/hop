/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.delegates;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.eclipse.swt.SWT;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

      String filename = BaseDialog.presentFileDialog(hopGui.getShell(), fileRegistry.getFilterExtensions(), fileRegistry.getFilterNames(), true);
      if ( filename == null ) {
        return;
      }
      fileOpen( hopGui.getVariables().environmentSubstitute( filename ) );
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error opening file", e );
    }
  }

  public IHopFileTypeHandler fileOpen( String filename ) throws Exception {
    HopFileTypeRegistry fileRegistry = HopFileTypeRegistry.getInstance();

    IHopFileType hopFile = fileRegistry.findHopFileType( filename );
    if ( hopFile == null ) {
      throw new HopException( "We looked at " + fileRegistry.getFileTypes().size() + " different Hop GUI file types but none know how to open file '" + filename + "'" );
    }

    IHopFileTypeHandler fileTypeHandler = hopFile.openFile( hopGui, filename, hopGui.getVariables() );
    hopGui.handleFileCapabilities( hopFile, false, false );

    // Keep track of this...
    //
    AuditManager.registerEvent( HopNamespace.getNamespace(), "file", filename, "open" );

    return fileTypeHandler;
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

      String filename = BaseDialog.presentFileDialog( true, hopGui.getShell(), fileType.getFilterExtensions(), fileType.getFilterNames(), true );
      if ( filename == null ) {
        return;
      }

      typeHandler.saveAs( hopGui.getVariables().environmentSubstitute( filename ) );

      AuditManager.registerEvent( HopNamespace.getNamespace(), "file", filename, "save" );
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
          AuditManager.registerEvent( HopNamespace.getNamespace(), "file", typeHandler.getFilename(), "save" );
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
   * Go over all files and ask to save the ones who have changed.
   *
   *  @return True if all files are saveguarded (or changes are ignored)
   */
  public boolean saveGuardAllFiles() {
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
    return true;
  }

  public void closeAllFiles() {
    for ( IHopPerspective perspective : hopGui.getPerspectiveManager().getPerspectives() ) {
      List<TabItemHandler> tabItemHandlers = perspective.getItems();
      if ( tabItemHandlers != null ) {
        // Copy the list to avoid changing the list we're editing (closing items)
        //
        List<TabItemHandler> handlers = new ArrayList<>( tabItemHandlers );
        for ( TabItemHandler tabItemHandler : handlers ) {
          IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();
          typeHandler.close();
        }
      }
    }
  }


  /**
   * When the app exits we need to see if all open files are saved in all perspectives...
   */
  public boolean fileExit() {

    if (!saveGuardAllFiles()) {
      return false;
    }
    // Also save all the open files in a list
    //
    hopGui.auditDelegate.writeLastOpenFiles();

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
      List<AuditEvent> events = AuditManager.findEvents( HopNamespace.getNamespace(), "file", "open", 100 );
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
