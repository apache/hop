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

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.cluster.dialog.ClusterSchemaDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.tree.provider.ClustersFolderProvider;

public class HopUiClustersDelegate extends HopUiSharedObjectDelegate {

  public HopUiClustersDelegate( HopUi hopUi ) {
    super( hopUi );
  }

  public void newClusteringSchema( TransMeta transMeta ) {
    ClusterSchema clusterSchema = new ClusterSchema();

    ClusterSchemaDialog dialog =
      new ClusterSchemaDialog(
          hopUi.getShell(), clusterSchema, transMeta.getClusterSchemas(), transMeta.getSlaveServers() );

    if ( dialog.open() ) {
      List<ClusterSchema> clusterSchemas = transMeta.getClusterSchemas();
      if ( isDuplicate( clusterSchemas, clusterSchema ) ) {
        new ErrorDialog(
          hopUi.getShell(), getMessage( "Spoon.Dialog.ErrorSavingCluster.Title" ), getMessage(
          "Spoon.Dialog.ErrorSavingCluster.Message", clusterSchema.getName() ),
          new HopException( getMessage( "Spoon.Dialog.ErrorSavingCluster.NotUnique" ) ) );
        return;
      }

      clusterSchemas.add( clusterSchema );

      if ( hopUi.rep != null ) {
        try {
          if ( !hopUi.rep.getSecurityProvider().isReadOnly() ) {
            hopUi.rep.save( clusterSchema, Const.VERSION_COMMENT_INITIAL_VERSION, null );
            if ( sharedObjectSyncUtil != null ) {
              sharedObjectSyncUtil.reloadTransformationRepositoryObjects( false );
            }
          } else {
            throw new HopException( BaseMessages.getString(
              PKG, "Spoon.Dialog.Exception.ReadOnlyRepositoryUser" ) );
          }
        } catch ( HopException e ) {
          showSaveError( clusterSchema, e );
        }
      }

      refreshTree();
    }
  }

  private void showSaveError( ClusterSchema clusterSchema, HopException e ) {
    new ErrorDialog(
      hopUi.getShell(), getMessage( "Spoon.Dialog.ErrorSavingCluster.Title" ),
      getMessage( "Spoon.Dialog.ErrorSavingCluster.Message", clusterSchema.getName() ), e );
  }

  public void editClusterSchema( TransMeta transMeta, ClusterSchema clusterSchema ) {
    ClusterSchemaDialog dialog =
        new ClusterSchemaDialog( hopUi.getShell(), clusterSchema, transMeta.getClusterSchemas(), transMeta.getSlaveServers() );
    if ( dialog.open() ) {
      if ( hopUi.rep != null && clusterSchema.getObjectId() != null ) {
        try {
          saveSharedObjectToRepository( clusterSchema, null );
        } catch ( HopException e ) {
          showSaveError( clusterSchema, e );
        }
      }
      sharedObjectSyncUtil.synchronizeClusterSchemas( clusterSchema );
      refreshTree();
    }
  }

  public void delClusterSchema( TransMeta transMeta, ClusterSchema clusterSchema ) {
    try {

      int idx = transMeta.getClusterSchemas().indexOf( clusterSchema );
      transMeta.getClusterSchemas().remove( idx );

      if ( hopUi.rep != null && clusterSchema.getObjectId() != null ) {
        // remove the partition schema from the repository too...
        hopUi.rep.deleteClusterSchema( clusterSchema.getObjectId() );
        if ( sharedObjectSyncUtil != null ) {
          sharedObjectSyncUtil.deleteClusterSchema( clusterSchema );
        }
      }

      refreshTree();
    } catch ( HopException e ) {
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ErrorDeletingPartitionSchema.Title" ), BaseMessages
          .getString( PKG, "Spoon.Dialog.ErrorDeletingPartitionSchema.Message" ), e );
    }
  }

  private void refreshTree() {
    hopUi.refreshTree( ClustersFolderProvider.STRING_CLUSTERS );
  }
}
