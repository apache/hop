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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditList;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HopGuiAuditDelegate {

  private HopGui hopGui;

  public HopGuiAuditDelegate( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  public void registerEvent( String type, String name, String operation ) throws HopException {
    AuditEvent event = new AuditEvent( hopGui.getNamespace(), type, name, operation, new Date() );
    hopGui.getAuditManager().storeEvent( event );
  }

  public List<AuditEvent> findEvents( String type, String operation, int maxNrEvents ) throws HopException {
    List<AuditEvent> events = hopGui.getAuditManager().findEvents( hopGui.getNamespace(), type );

    if ( operation == null ) {
      return events;
    }
    // Filter out the specified operation only (File open)
    //
    List<AuditEvent> operationEvents = new ArrayList<>();
    for ( AuditEvent event : events ) {
      if ( event.getOperation().equalsIgnoreCase( operation ) ) {
        operationEvents.add( event );

        if ( maxNrEvents > 0 && operationEvents.size() >= maxNrEvents ) {
          break;
        }
      }
    }
    return operationEvents;
  }

  public void openLastFiles() {
    if ( !hopGui.getProps().openLastFile() ) {
      return;
    }

    // Open the last files for each perspective...
    //
    List<IHopPerspective> perspectives = hopGui.getPerspectiveManager().getPerspectives();
    for ( IHopPerspective perspective : perspectives ) {
      List<TabItemHandler> tabItems = perspective.getItems();
      if ( tabItems != null ) {
        // This perspective has the ability to handle multiple files.
        // Lets's load the files in the previously saved order...
        //
        AuditList auditList;
        try {
          auditList = hopGui.getAuditManager().retrieveList( hopGui.getNamespace(), perspective.getId() );
        } catch ( Exception e ) {
          new ErrorDialog(hopGui.getShell(), "Error", "Error reading audit list of perspective " + perspective.getId(), e);
          break;
        }

        for (String filename : auditList.getNames()) {
          try {
            if (StringUtils.isNotEmpty( filename )) {
              hopGui.fileDelegate.fileOpen( filename );
            }
          } catch(Exception e) {
            new ErrorDialog(hopGui.getShell(), "Error", "Error opening file '"+filename+"'", e);
          }
        }
      }
    }
  }

  /**
   * Remember all the open files per perspective
   */
  public void writeLastOpenFiles() {
    if ( !hopGui.getProps().openLastFile() ) {
      return;
    }

    List<IHopPerspective> perspectives = hopGui.getPerspectiveManager().getPerspectives();
    for ( IHopPerspective perspective : perspectives ) {
      List<TabItemHandler> tabItems = perspective.getItems();
      if ( tabItems != null ) {
        // This perspective has the ability to handle multiple files.
        // Lets's save the files in the given order...
        //
        List<String> files = new ArrayList<>();
        for ( TabItemHandler tabItem : tabItems ) {
          if (StringUtils.isNotEmpty(tabItem.getTypeHandler().getFilename())) {
            files.add( tabItem.getTypeHandler().getFilename() );
          }
        }
        AuditList auditList = new AuditList( hopGui.getNamespace(), perspective.getId(), files );
        try {
          hopGui.getAuditManager().storeList( auditList );
        } catch ( Exception e ) {
          hopGui.getLog().logError( "Error writing audit list of perspective " + perspective.getId(), e );
        }
      }
    }
  }
}
