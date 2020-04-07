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

package org.apache.hop.workflow.actions.filesexist;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WorkflowEntryFilesExistTest {
  private Workflow workflow;
  private ActionFilesExist entry;

  private String existingFile1;
  private String existingFile2;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() throws Exception {
    workflow = new Workflow( new WorkflowMeta() );
    entry = new ActionFilesExist();

    workflow.getWorkflowMeta().addAction( new ActionCopy( entry ) );
    entry.setParentWorkflow( workflow );
    WorkflowMeta mockWorkflowMeta = mock( WorkflowMeta.class );
    entry.setParentWorkflowMeta( mockWorkflowMeta );

    workflow.setStopped( false );

    existingFile1 = TestUtils.createRamFile( getClass().getSimpleName() + "/existingFile1.ext", entry );
    existingFile2 = TestUtils.createRamFile( getClass().getSimpleName() + "/existingFile2.ext", entry );
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSetNrErrorsNewBehaviorFalseResult() throws Exception {
    // this tests fix for PDI-10270
    entry.setArguments( new String[] { "nonExistingFile.ext" } );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Entry should fail", res.getResult() );
    assertEquals( "Files not found. Result is false. But... No of errors should be zero", 0, res.getNrErrors() );
  }

  @Test
  public void testSetNrErrorsOldBehaviorFalseResult() throws Exception {
    // this tests backward compatibility settings for PDI-10270
    entry.setArguments( new String[] { "nonExistingFile1.ext", "nonExistingFile2.ext" } );

    entry.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "Y" );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Entry should fail", res.getResult() );
    assertEquals(
      "Files not found. Result is false. And... Number of errors should be the same as number of not found files",
      entry.getArguments().length, res.getNrErrors() );
  }

  @Test
  public void testExecuteWithException() throws Exception {
    entry.setArguments( new String[] { null } );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Entry should fail", res.getResult() );
    assertEquals( "File with wrong name was specified. One error should be reported", 1, res.getNrErrors() );
  }

  @Test
  public void testExecuteSuccess() throws Exception {
    entry.setArguments( new String[] { existingFile1, existingFile2 } );

    Result res = entry.execute( new Result(), 0 );

    assertTrue( "Entry failed", res.getResult() );
  }

  @Test
  public void testExecuteFail() throws Exception {
    entry.setArguments(
      new String[] { existingFile1, existingFile2, "nonExistingFile1.ext", "nonExistingFile2.ext" } );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Entry should fail", res.getResult() );
  }
}
