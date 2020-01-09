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
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.cluster.dialog.ClusterSchemaDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;

public class HopUiClustersDelegate  {

  private final HopUi hopUi;

  public HopUiClustersDelegate( HopUi hopUi ) {
    this.hopUi = hopUi;
  }

  public void newClusteringSchema( TransMeta transMeta ) {
    try {
      ClusterSchema clusterSchema = new ClusterSchema();

      ClusterSchemaDialog dialog = new ClusterSchemaDialog( hopUi.getShell(), hopUi.getMetaStore(), clusterSchema );
      if ( dialog.open() ) {
        ClusterSchema.createFactory( hopUi.getMetaStore() ).saveElement( clusterSchema );
        refreshTree();
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error creating new cluster schema", e );
    }
  }

  public void editClusterSchema( TransMeta transMeta, ClusterSchema clusterSchema ) {
    try {
      ClusterSchemaDialog dialog = new ClusterSchemaDialog( hopUi.getShell(), hopUi.getMetaStore(), clusterSchema );
      if ( dialog.open() ) {
        ClusterSchema.createFactory( hopUi.getMetaStore() ).saveElement( clusterSchema );
        refreshTree();
      }
    } catch(Exception e) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error editing/saving cluster schema", e );
    }
  }

  public void delClusterSchema( TransMeta transMeta, ClusterSchema clusterSchema ) {
    try {
      ClusterSchema.createFactory( hopUi.getMetaStore() ).deleteElement( clusterSchema.getName() );
      refreshTree();
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error deleting cluster schema", e );
    }
  }

  private void refreshTree() {
    hopUi.refreshTree();
  }
}
