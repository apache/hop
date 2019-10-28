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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.tree.TreeNode;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.tree.TreeFolderProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bmorrise on 6/28/18.
 */
public class PartitionsFolderProvider extends TreeFolderProvider {

  private static Class<?> PKG = HopUi.class;
  public static final String STRING_PARTITIONS = BaseMessages.getString( PKG, "Spoon.STRING_PARTITIONS" );
  private GUIResource guiResource;
  private HopUi hopUi;

  public PartitionsFolderProvider( GUIResource guiResource, HopUi hopUi ) {
    this.guiResource = guiResource;
    this.hopUi = hopUi;
  }

  public PartitionsFolderProvider() {
    this( GUIResource.getInstance(), HopUi.getInstance() );
  }

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {

    TransMeta transMeta = (TransMeta) meta;

    List<PartitionSchema> partitionSchemas;
    try {
      partitionSchemas = pickupPartitionSchemas( transMeta );
    } catch ( HopException e ) {
      new ErrorDialog( HopUi.getInstance().getShell(),
              BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
              BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.PartitioningSchemas" ),
              e
      );

      return;
    }

    // Put the steps below it.
    for ( PartitionSchema partitionSchema : partitionSchemas ) {
      if ( !filterMatch( partitionSchema.getName(), filter ) ) {
        continue;
      }
      TreeNode childTreeNode = createTreeNode( treeNode, partitionSchema.getName(), guiResource
              .getImagePartitionSchema() );
      if ( partitionSchema.isShared() ) {
        childTreeNode.setFont( guiResource.getFontBold() );
      }
    }
  }

  private List<PartitionSchema> pickupPartitionSchemas( TransMeta transMeta ) throws HopException {
    Repository rep = hopUi.getRepository();
    if ( rep != null ) {
      ObjectId[] ids = rep.getPartitionSchemaIDs( false );
      List<PartitionSchema> result = new ArrayList<>( ids.length );
      for ( ObjectId id : ids ) {
        PartitionSchema schema = rep.loadPartitionSchema( id, null );
        result.add( schema );
      }
      return result;
    }

    return transMeta.getPartitionSchemas();
  }

  @Override
  public String getTitle() {
    return STRING_PARTITIONS;
  }

  @Override
  public Class getType() {
    return PartitionSchema.class;
  }
}
