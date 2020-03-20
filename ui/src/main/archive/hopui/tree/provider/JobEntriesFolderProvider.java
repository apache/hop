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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.tree.TreeNode;
import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.graphics.Image;

/**
 * Created by bmorrise on 7/2/18.
 */
public class JobEntriesFolderProvider extends AutomaticTreeFolderProvider {

  private static Class<?> PKG = HopUi.class;
  public static final String STRING_JOB_ENTRIES = BaseMessages.getString( PKG, "HopGui.STRING_JOB_ENTRIES" );

  @Override
  public String getTitle() {
    return STRING_JOB_ENTRIES;
  }

  @Override
  public void refresh( AbstractMeta meta, TreeNode treeNode, String filter ) {
    JobMeta jobMeta = (JobMeta) meta;
    for ( int i = 0; i < jobMeta.nrJobEntries(); i++ ) {
      JobEntryCopy jobEntry = jobMeta.getJobEntry( i );

      if ( !filterMatch( jobEntry.getName(), filter ) && !filterMatch( jobEntry.getDescription(), filter ) ) {
        continue;
      }

      Image icon;
      if ( jobEntry.isStart() ) {
        icon = GUIResource.getInstance().getImageStartMedium();
      } else if ( jobEntry.isDummy() ) {
        icon = GUIResource.getInstance().getImageDummyMedium();
      } else {
        String key = jobEntry.getEntry().getPluginId();
        icon = GUIResource.getInstance().getImagesJobentriesSmall().get( key );
      }

      createTreeNode( treeNode, jobEntry.getName(), icon );
    }
  }

  @Override
  public Class getType() {
    return JobEntryCopy.class;
  }
}
