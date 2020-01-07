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

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.cluster.dialog.ClusterSchemaDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.tree.provider.ClustersFolderProvider;

import java.util.List;

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
      sharedObjectSyncUtil.synchronizeClusterSchemas( clusterSchema );
      refreshTree();
    }
  }

  public void delClusterSchema( TransMeta transMeta, ClusterSchema clusterSchema ) {
    int idx = transMeta.getClusterSchemas().indexOf( clusterSchema );
    transMeta.getClusterSchemas().remove( idx );

    refreshTree();
  }

  private void refreshTree() {
    hopUi.refreshTree( ClustersFolderProvider.STRING_CLUSTERS );
  }
}
