/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui.tree.provider;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.tree.TreeNode;
import org.apache.hop.ui.hopui.DatabasesCollector;
import org.apache.hop.ui.hopui.HopUi;

/**
 * Created by bmorrise on 6/28/18.
 */
public class DBConnectionFolderProvider extends AutomaticTreeFolderProvider {

  private static Class<?> PKG = HopUi.class;
  public static final String STRING_CONNECTIONS = BaseMessages.getString( PKG, "Spoon.STRING_CONNECTIONS" );

  private GUIResource guiResource;
  private HopUi hopUi;

  public DBConnectionFolderProvider( GUIResource guiResource, HopUi hopUi ) {
    this.guiResource = guiResource;
    this.hopUi = hopUi;
  }

  public DBConnectionFolderProvider() {
    this( GUIResource.getInstance(), HopUi.getInstance() );
  }

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
    DatabasesCollector collector = new DatabasesCollector( meta );
    try {
      collector.collectDatabases();
    } catch ( HopException e ) {
      new ErrorDialog( HopUi.getInstance().getShell(),
              BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
              BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.DbConnections" ),
              e
      );
    }

    for ( String dbName : collector.getDatabaseNames() ) {
      if ( !filterMatch( dbName, filter ) ) {
        continue;
      }
      DatabaseMeta databaseMeta = collector.getMetaFor( dbName );

      TreeNode childTreeNode = createTreeNode( treeNode, databaseMeta.getName(), guiResource.getImageConnectionTree() );
      if ( databaseMeta.isShared() ) {
        childTreeNode.setFont( guiResource.getFontBold() );
      }
    }
  }

  @Override
  public String getTitle() {
    return STRING_CONNECTIONS;
  }

  @Override
  public Class getType() {
    return DatabaseMeta.class;
  }
}
