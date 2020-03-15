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

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.HasSlaveServersInterface;
import org.apache.hop.ui.cluster.SlaveServerDialog;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.HopUiSlave;
import org.apache.hop.ui.hopui.TabMapEntry;
import org.apache.hop.ui.hopui.TabMapEntry.ObjectType;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;
import org.eclipse.swt.SWT;

public class HopUiSlaveDelegate  {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!
  private final HopUi hopUi;

  public HopUiSlaveDelegate( HopUi hopUi ) {
    this.hopUi = hopUi;
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

      tabMapEntry = new TabMapEntry( tabItem, null, tabName, hopUiSlave, ObjectType.SLAVE_SERVER );
      hopUi.delegates.tabs.addTab( tabMapEntry );
    }
    int idx = tabfolder.indexOf( tabMapEntry.getTabItem() );
    tabfolder.setSelected( idx );
  }

  public void delSlaveServer( HasSlaveServersInterface hasSlaveServersInterface, SlaveServer slaveServer )
    throws HopException {

    hasSlaveServersInterface.getSlaveServers().remove( slaveServer );
    refreshTree();

  }


  public void newSlaveServer( HasSlaveServersInterface hasSlaveServersInterface ) {
    SlaveServer slaveServer = new SlaveServer();

    SlaveServerDialog dialog =
      new SlaveServerDialog( hopUi.getShell(), hopUi.getMetaStore(), slaveServer );
    if ( dialog.open()!=null ) {
      slaveServer.verifyAndModifySlaveServerName( hasSlaveServersInterface.getSlaveServers(), null );
      hasSlaveServersInterface.getSlaveServers().add( slaveServer );

      refreshTree();
    }
  }

  public boolean edit( SlaveServer slaveServer ) {
    String originalName = slaveServer.getName();
    SlaveServerDialog dialog = new SlaveServerDialog( hopUi.getShell(), hopUi.getMetaStore(), slaveServer );
    if ( dialog.open()!=null ) {

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
    hopUi.refreshTree();
  }
}
