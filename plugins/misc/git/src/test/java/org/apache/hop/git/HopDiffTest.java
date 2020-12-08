/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.apache.hop.git.PdiDiff.ADDED;
import static org.apache.hop.git.PdiDiff.ATTR_GIT;
import static org.apache.hop.git.PdiDiff.ATTR_STATUS;
import static org.apache.hop.git.PdiDiff.CHANGED;
import static org.apache.hop.git.PdiDiff.REMOVED;
import static org.apache.hop.git.PdiDiff.UNCHANGED;
import static org.apache.hop.git.PdiDiff.compareJobEntries;
import static org.apache.hop.git.PdiDiff.compareTransforms;
import static org.junit.Assert.assertEquals;

public class HopDiffTest {

  IHopMetadataProvider metadataProvider;

  @Before
  public void setUp() throws HopException {
    HopClientEnvironment.getInstance().setClient( HopClientEnvironment.ClientType.OTHER );
    HopEnvironment.init();
    metadataProvider = new MemoryMetadataProvider();
  }

  @Test
  public void diffPipelineTest() throws Exception {
    File file = new File( "src/test/resources/r1.hpl" );
    InputStream xmlStream = new FileInputStream( file );
    PipelineMeta pipelineMeta = new PipelineMeta( xmlStream, metadataProvider, true, Variables.getADefaultVariableSpace() );
    //pipelineMeta.sortTransforms();

    File file2 = new File( "src/test/resources/r2.hpl" );
    InputStream xmlStream2 = new FileInputStream( file2 );
    PipelineMeta pipelineMeta2 = new PipelineMeta( xmlStream2, metadataProvider, true, Variables.getADefaultVariableSpace() );
    //pipelineMeta2.sortTransforms();

    pipelineMeta = compareTransforms( pipelineMeta, pipelineMeta2, true );
    pipelineMeta2 = compareTransforms( pipelineMeta2, pipelineMeta, false );
    assertEquals( UNCHANGED, pipelineMeta.getTransform( 0 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( CHANGED, pipelineMeta.getTransform( 1 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( REMOVED, pipelineMeta.getTransform( 2 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( ADDED, pipelineMeta2.getTransform( 2 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
  }

  @Test
  public void diffWorkflowTest() throws Exception {
    File file = new File( "src/test/resources/r1.hwf" );
    InputStream xmlStream = new FileInputStream( file );
    WorkflowMeta jobMeta = new WorkflowMeta( xmlStream, metadataProvider, new Variables());

    File file2 = new File( "src/test/resources/r2.hwf" );
    InputStream xmlStream2 = new FileInputStream( file2 );
    WorkflowMeta jobMeta2 = new WorkflowMeta( xmlStream2, metadataProvider, new Variables());

    jobMeta = compareJobEntries( jobMeta, jobMeta2, true );
    jobMeta2 = compareJobEntries( jobMeta2, jobMeta, false );
    assertEquals( CHANGED, jobMeta.getAction( 0 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( UNCHANGED, jobMeta.getAction( 1 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( REMOVED, jobMeta.getAction( 2 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
    assertEquals( ADDED, jobMeta2.getAction( 2 ).getAttribute( ATTR_GIT, ATTR_STATUS ) );
  }
}
