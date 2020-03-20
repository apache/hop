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

import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.partition.dialog.PartitionSchemaDialog;

public class HopUiPartitionsDelegate {
  private final HopUi hopUi;

  public HopUiPartitionsDelegate( HopUi hopUi ) {
    this.hopUi = hopUi;
  }

  public void newPartitioningSchema( TransMeta transMeta ) {
    PartitionSchema partitionSchema = new PartitionSchema();

    PartitionSchemaDialog dialog = new PartitionSchemaDialog( hopUi.getShell(), hopUi.getMetaStore(), partitionSchema, transMeta );
    if ( dialog.open() ) {

      try {
        PartitionSchema.createFactory( hopUi.getMetaStore() ).saveElement( partitionSchema );
      } catch ( MetaStoreException e ) {
        new ErrorDialog( hopUi.getShell(), "Error", "Error adding partition schema to the metastore", e );
      }
      refreshTree();
    }
  }

  public void editPartitionSchema( TransMeta transMeta, PartitionSchema partitionSchema ) {
    String originalName = partitionSchema.getName();
    PartitionSchemaDialog dialog = new PartitionSchemaDialog( hopUi.getShell(), hopUi.getMetaStore(), partitionSchema, transMeta );
    if ( dialog.open() ) {
      try {
        PartitionSchema.createFactory( hopUi.getMetaStore() ).saveElement( partitionSchema );
      } catch ( MetaStoreException e ) {
        new ErrorDialog( hopUi.getShell(), "Error", "Error saving partition schema in the metastore", e );
      }
      refreshTree();
    }
  }

  public void delPartitionSchema( TransMeta transMeta, PartitionSchema partitionSchema ) {
    try {
      PartitionSchema.createFactory( hopUi.getMetaStore() ).deleteElement( partitionSchema.getName() );
    } catch ( MetaStoreException e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error deleting partition schema from the metastore", e );
    }

    refreshTree();
  }

  private void refreshTree() {
    hopUi.refreshTree();
  }
}
