/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.MySQLDatabaseMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class SynchronizeAfterMergeTest {

  private static final String TRANSFORM_NAME = "Sync";

  @Test
  public void initWithCommitSizeVariable() {
    TransformMeta transformMeta = mock( TransformMeta.class );
    doReturn( TRANSFORM_NAME ).when( transformMeta ).getName();
    doReturn( 1 ).when( transformMeta ).getCopies();

    SynchronizeAfterMergeMeta smi = mock( SynchronizeAfterMergeMeta.class );
    SynchronizeAfterMergeData sdi = mock( SynchronizeAfterMergeData.class );

    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    doReturn( mock( MySQLDatabaseMeta.class ) ).when( dbMeta ).getIDatabase();

    doReturn( dbMeta ).when( smi ).getDatabaseMeta();
    doReturn( "${commit.size}" ).when( smi ).getCommitSize();

    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    doReturn( "1" ).when( pipelineMeta ).getVariable( Const.INTERNAL_VARIABLE_SLAVE_SERVER_NUMBER );
    doReturn( "2" ).when( pipelineMeta ).getVariable( Const.INTERNAL_VARIABLE_CLUSTER_SIZE );
    doReturn( "Y" ).when( pipelineMeta ).getVariable( Const.INTERNAL_VARIABLE_CLUSTER_MASTER );
    doReturn( transformMeta ).when( pipelineMeta ).findTransform( TRANSFORM_NAME );

    SynchronizeAfterMerge transform = mock( SynchronizeAfterMerge.class );
    doCallRealMethod().when( transform ).setPipelineMeta( any( PipelineMeta.class ) );
    doCallRealMethod().when( transform ).setTransformMeta( any( TransformMeta.class ) );
    doCallRealMethod().when( transform ).init();
    doReturn( transformMeta ).when( transform ).getTransformMeta();
    doReturn( pipelineMeta ).when( transform ).getPipelineMeta();
    doReturn( "120" ).when( transform ).environmentSubstitute( "${commit.size}" );

    transform.setPipelineMeta( pipelineMeta );
    transform.setTransformMeta( transformMeta );
    transform.init();

    assertEquals( 120, sdi.commitSize );
  }
}
