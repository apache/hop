/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui.delegates;

import java.util.List;

import org.apache.hop.ui.hopui.HopUiSlave;
import org.eclipse.swt.SWT;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.HasSlaveServersInterface;
import org.apache.hop.ui.cluster.dialog.SlaveServerDialog;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.TabMapEntry;
import org.apache.hop.ui.hopui.TabMapEntry.ObjectType;
import org.apache.hop.ui.hopui.tree.provider.SlavesFolderProvider;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;

public class HopUiSlaveDelegate extends HopUiSharedObjectDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  public HopUiSlaveDelegate( HopUi hopUi ) {
    super( hopUi );
  }

  public void addSpoonSlave( SlaveServer slaveServer ) {
    TabSet tabfolder = hopUi.tabfolder;

    // See if there is a HopUiSlave for this slaveServer...
    String tabName = hopUi.delegates.tabs.makeSlaveTabName( slaveServer );
    TabMapEntry tabMapEntry = hopUi.delegates.tabs.findTabMapEntry( tabName, ObjectType.SLAVE_SERVER );
    if ( tabMapEntry == null ) {
      HopUiSlave hopUiSlave = new HopUiSlave( tabfolder.getSwtTabset(), SWT.NONE, hopUi, slaveServer );
      PropsUI props = PropsUI.getInstance();
      TabItem tabItem = new TabItem( tabfolder, tabName, tabName, props.getSashWeights() );
      tabItem.setToolTipText( "Status of slave server : "
        + slaveServer.getName() + " : " + slaveServer.getServerAndPort() );
      tabItem.setControl( hopUiSlave );

      tabMapEntry = new TabMapEntry( tabItem, null, tabName, null, null, hopUiSlave, ObjectType.SLAVE_SERVER );
      hopUi.delegates.tabs.addTab( tabMapEntry );
    }
    int idx = tabfolder.indexOf( tabMapEntry.getTabItem() );
    tabfolder.setSelected( idx );
  }

  public void delSlaveServer( HasSlaveServersInterface hasSlaveServersInterface, SlaveServer slaveServer )
    throws HopException {

    Repository rep = hopUi.getRepository();
    if ( rep != null && slaveServer.getObjectId() != null ) {
      // remove the slave server from the repository too...
      rep.deleteSlave( slaveServer.getObjectId() );
      if ( sharedObjectSyncUtil != null ) {
        sharedObjectSyncUtil.deleteSlaveServer( slaveServer );
      }
    }
    hasSlaveServersInterface.getSlaveServers().remove( slaveServer );
    refreshTree();

  }


  public void newSlaveServer( HasSlaveServersInterface hasSlaveServersInterface ) {
    SlaveServer slaveServer = new SlaveServer();

    SlaveServerDialog dialog =
        new SlaveServerDialog( hopUi.getShell(), slaveServer, hasSlaveServersInterface.getSlaveServers() );
    if ( dialog.open() ) {
      slaveServer.verifyAndModifySlaveServerName( hasSlaveServersInterface.getSlaveServers(), null );
      hasSlaveServersInterface.getSlaveServers().add( slaveServer );
      if ( hopUi.rep != null ) {
        try {
          if ( !hopUi.rep.getSecurityProvider().isReadOnly() ) {
            hopUi.rep.save( slaveServer, Const.VERSION_COMMENT_INITIAL_VERSION, null );
            // repository objects are "global"
            if ( sharedObjectSyncUtil != null ) {
              sharedObjectSyncUtil.reloadJobRepositoryObjects( false );
              sharedObjectSyncUtil.reloadTransformationRepositoryObjects( false );
            }
          } else {
            showSaveErrorDialog( slaveServer,
                new HopException( BaseMessages.getString( PKG, "Spoon.Dialog.Exception.ReadOnlyRepositoryUser" ) ) );
          }
        } catch ( HopException e ) {
          showSaveErrorDialog( slaveServer, e );
        }
      }

      refreshTree();
    }
  }

  public boolean edit( SlaveServer slaveServer, List<SlaveServer> existingServers ) {
    String originalName = slaveServer.getName();
    SlaveServerDialog dialog = new SlaveServerDialog( hopUi.getShell(), slaveServer, existingServers );
    if ( dialog.open() ) {
      if ( hopUi.rep != null ) {
        try {
          saveSharedObjectToRepository( slaveServer, null );
        } catch ( HopException e ) {
          showSaveErrorDialog( slaveServer, e );
        }
      }
      if ( sharedObjectSyncUtil != null ) {
        sharedObjectSyncUtil.synchronizeSlaveServers( slaveServer, originalName );
      }
      refreshTree();
      hopUi.refreshGraph();
      return true;
    }
    return false;
  }

  private void showSaveErrorDialog( SlaveServer slaveServer, HopException e ) {
    new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingSlave.Title" ),
        BaseMessages.getString( PKG, "Spoon.Dialog.ErrorSavingSlave.Message", slaveServer.getName() ), e );
  }

  private void refreshTree() {
    hopUi.refreshTree( SlavesFolderProvider.STRING_SLAVES );
  }
}
