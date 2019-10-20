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

package org.apache.hop.ui.spoon;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.tree.TreeNode;
import org.apache.hop.ui.spoon.tree.provider.ClustersFolderProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
public class SpoonRefreshClustersSubtreeTest {

  private ClustersFolderProvider clustersFolderProvider;
  private TreeNode treeNode;

  @Before
  public void setUp() throws Exception {
    GUIResource guiResource = mock( GUIResource.class );
    clustersFolderProvider = new ClustersFolderProvider( guiResource );
    treeNode = new TreeNode();
  }

  private void callRefreshWith( TransMeta meta, String filter ) {
    clustersFolderProvider.refresh( meta, treeNode, filter );
  }

  private void verifyNumberOfNodesCreated( int times ) {
    Assert.assertEquals( times, treeNode.getChildren().size() );
  }

  @Test
  public void noClusters() {
    TransMeta meta = mock( TransMeta.class );
    when( meta.getClusterSchemas() ).thenReturn( Collections.<ClusterSchema>emptyList() );

    callRefreshWith( meta, null );
    verifyNumberOfNodesCreated( 0 );
  }

  @Test
  public void severalClustersExist() {
    TransMeta meta = prepareMeta();

    callRefreshWith( meta, null );
    verifyNumberOfNodesCreated( 3 );
  }

  @Test
  public void onlyOneMatchesFiltering() {
    TransMeta meta = prepareMeta();

    callRefreshWith( meta, "2" );
    verifyNumberOfNodesCreated( 1 );
  }


  private static TransMeta prepareMeta() {
    TransMeta meta = mock( TransMeta.class );
    List<ClusterSchema> schemas = Arrays.asList( createSchema( "1" ), createSchema( "2" ), createSchema( "3" ) );
    when( meta.getClusterSchemas() ).thenReturn( schemas );
    return meta;
  }

  private static ClusterSchema createSchema( String name ) {
    return new ClusterSchema( name, Collections.<SlaveServer>emptyList() );
  }
}
