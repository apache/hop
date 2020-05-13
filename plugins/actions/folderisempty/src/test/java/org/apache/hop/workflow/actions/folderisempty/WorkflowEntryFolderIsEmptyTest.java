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

package org.apache.hop.workflow.actions.folderisempty;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WorkflowEntryFolderIsEmptyTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionFolderIsEmpty entry;

  private String emptyDir;
  private String nonEmptyDir;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    workflow = new LocalWorkflowEngine( new WorkflowMeta() );
    entry = new ActionFolderIsEmpty();

    workflow.getWorkflowMeta().addAction( new ActionCopy( entry ) );
    entry.setParentWorkflow( workflow );
    WorkflowMeta mockWorkflowMeta = mock( WorkflowMeta.class );
    entry.setParentWorkflowMeta( mockWorkflowMeta );

    workflow.setStopped( false );

    File dir = Files.createTempDirectory( "dir", new FileAttribute<?>[ 0 ] ).toFile();
    dir.deleteOnExit();
    emptyDir = dir.getPath();

    dir = Files.createTempDirectory( "dir", new FileAttribute<?>[ 0 ] ).toFile();
    dir.deleteOnExit();
    nonEmptyDir = dir.getPath();

    File file = File.createTempFile( "existingFile", "ext", dir );
    file.deleteOnExit();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSetNrErrorsSuccess() throws Exception {
    entry.setFoldername( emptyDir );

    Result result = entry.execute( new Result(), 0 );

    assertTrue( "For empty folder result should be true", result.getResult() );
    assertEquals( "There should be no errors", 0, result.getNrErrors() );
  }

  @Test
  public void testSetNrErrorsNewBehaviorFail() throws Exception {
    entry.setFoldername( nonEmptyDir );

    Result result = entry.execute( new Result(), 0 );

    assertFalse( "For non-empty folder result should be false", result.getResult() );
    assertEquals( "There should be still no errors", 0, result.getNrErrors() );
  }

  @Test
  public void testSetNrErrorsOldBehaviorFail() throws Exception {
    entry.setFoldername( nonEmptyDir );

    entry.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "Y" );

    Result result = entry.execute( new Result(), 0 );

    assertFalse( "For non-empty folder result should be false", result.getResult() );
    assertEquals( "According to old behaviour there should be an error", 1, result.getNrErrors() );
  }
}
