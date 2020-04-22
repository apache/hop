package org.apache.hop.env.session;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopGuiPerspectiveManager;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;

import java.util.ArrayList;
import java.util.List;

public class EnvironmentSessionUtil {

  /**
   * We store a list of files per perspective using the audit manager
   */
  public static final void storeHopGuiSession() throws HopException {
    HopGui hopGui = HopGui.getInstance();
    IAuditManager auditManager = hopGui.getAuditManager();

    // The name of the environment is stored in the Hop GUI namespace
    //
    String environmentName = hopGui.getNamespace();

    // Ask all perspectives for their currently loaded files
    //
    HopGuiPerspectiveManager perspectiveManager = hopGui.getPerspectiveManager();
    List<IHopPerspective> perspectives = perspectiveManager.getPerspectives();
    for ( IHopPerspective perspective : perspectives ) {
      // These are the currently loaded tab items for this perspective
      //
      String perspectiveId = perspective.getId();

      List<String> filenames = new ArrayList<>();

      List<TabItemHandler> items = perspective.getItems();
      for ( TabItemHandler item : items ) {
        IHopFileTypeHandler typeHandler = item.getTypeHandler();
        String filename = typeHandler.getFilename();
        filenames.add( filename );
      }

      AuditList auditList = new AuditList(calculateGroup(environmentName), perspectiveId, filenames );
      auditManager.storeList( auditList );
    }
  }

  public static String calculateGroup( String environmentName ) {
    // TODO sanitize env-name, perhaps checksum?
    return "environment-"+environmentName;
  }

  public static final void restoreHopGuiSession() throws HopException {
    HopGui hopGui = HopGui.getInstance();
    IAuditManager auditManager = hopGui.getAuditManager();

    // The name of the environment is stored in the Hop GUI namespace
    //
    String environmentName = hopGui.getNamespace();

    // First close all files in all perspectives
    //
    HopGuiPerspectiveManager perspectiveManager = hopGui.getPerspectiveManager();
    List<IHopPerspective> perspectives = perspectiveManager.getPerspectives();
    for ( IHopPerspective perspective : perspectives ) {
      List<TabItemHandler> items = perspective.getItems();
      for ( TabItemHandler item : items ) {
        String perspectiveId = perspective.getId();
        AuditList auditList = auditManager.retrieveList( calculateGroup( environmentName ), perspectiveId );
        for (String filename : auditList.getNames()) {
          try {
            hopGui.fileDelegate.fileOpen( filename );
          } catch(Exception e) {
            new ErrorDialog( hopGui.getShell(), "Error", "Error restoring file from session : "+filename, e );
          }
        }
      }
    }
  }
}
