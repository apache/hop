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

package org.apache.hop.workflow;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.BaseLogTable;
import org.apache.hop.core.logging.ActionLogTable;
import org.apache.hop.core.logging.WorkflowLogTable;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogStatus;
import org.apache.hop.core.logging.LogTableField;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.actions.special.ActionSpecial;
import org.apache.hop.metastore.api.IMetaStore;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WorkflowTest {
  private static final String STRING_DEFAULT = "<def>";
  private Workflow mockedWorkflow;
  private Database mockedDataBase;
  private IVariables mockedVariableSpace;
  private IMetaStore mockedMetaStore;
  private WorkflowMeta mockedWorkflowMeta;
  private ActionCopy mockedActionCopy;
  private ActionSpecial mockedJobEntrySpecial;
  private LogChannel mockedLogChannel;


  @Before
  public void init() {
    mockedDataBase = mock( Database.class );
    mockedWorkflow = mock( Workflow.class );
    mockedVariableSpace = mock( IVariables.class );
    mockedMetaStore = mock( IMetaStore.class );
    mockedWorkflowMeta = mock( WorkflowMeta.class );
    mockedActionCopy = mock( ActionCopy.class );
    mockedJobEntrySpecial = mock( ActionSpecial.class );
    mockedLogChannel = mock( LogChannel.class );

    when( mockedWorkflow.createDataBase( any( DatabaseMeta.class ) ) ).thenReturn( mockedDataBase );
  }

  @Test
  public void recordsCleanUpMethodIsCalled_JobEntryLogTable() throws Exception {

    ActionLogTable actionLogTable = ActionLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    setAllTableParamsDefault( actionLogTable );

    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setActionsLogTable( actionLogTable );

    when( mockedWorkflow.getWorkflowMeta() ).thenReturn( workflowMeta );
    doCallRealMethod().when( mockedWorkflow ).writeJobEntryLogInformation();

    mockedWorkflow.writeJobEntryLogInformation();

    verify( mockedDataBase ).cleanupLogRecords( actionLogTable );
  }

  @Test
  public void recordsCleanUpMethodIsCalled_JobLogTable() throws Exception {
    WorkflowLogTable workflowLogTable = WorkflowLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    setAllTableParamsDefault( workflowLogTable );

    doCallRealMethod().when( mockedWorkflow ).writeLogTableInformation( workflowLogTable, LogStatus.END );

    mockedWorkflow.writeLogTableInformation( workflowLogTable, LogStatus.END );

    verify( mockedDataBase ).cleanupLogRecords( workflowLogTable );
  }

  public void setAllTableParamsDefault( BaseLogTable table ) {
    table.setSchemaName( STRING_DEFAULT );
    table.setConnectionName( STRING_DEFAULT );
    table.setTimeoutInDays( STRING_DEFAULT );
    table.setTableName( STRING_DEFAULT );
    table.setFields( new ArrayList<LogTableField>() );
  }

  @Test
  public void testNewJobWithContainerObjectId() {
    WorkflowMeta meta = mock( WorkflowMeta.class );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerObjectId();

    Workflow workflow = new Workflow( meta );

    assertEquals( carteId, workflow.getContainerObjectId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17398.
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoWorkflowsGetSameLogChannelId() {
    WorkflowMeta meta = mock( WorkflowMeta.class );

    Workflow workflow1 = new Workflow( meta );
    Workflow workflow2 = new Workflow( meta );

    assertEquals( workflow1.getLogChannelId(), workflow2.getLogChannelId() );
  }

  /**
   * This test demonstrates the fix for PDI-17398.
   * Two schedules -> two HopServer object Ids -> two log channel Ids
   */
  @Test
  public void testTwoWorkflowsGetDifferentLogChannelIdWithDifferentCarteId() {
    WorkflowMeta meta1 = mock( WorkflowMeta.class );
    WorkflowMeta meta2 = mock( WorkflowMeta.class );

    String carteId1 = UUID.randomUUID().toString();
    String carteId2 = UUID.randomUUID().toString();

    doReturn( carteId1 ).when( meta1 ).getContainerObjectId();
    doReturn( carteId2 ).when( meta2 ).getContainerObjectId();

    Workflow workflow1 = new Workflow( meta1 );
    Workflow workflow2 = new Workflow( meta2 );

    assertNotEquals( workflow1.getContainerObjectId(), workflow2.getContainerObjectId() );
    assertNotEquals( workflow1.getLogChannelId(), workflow2.getLogChannelId() );
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    Workflow workflowTest = new Workflow();
    boolean hasFilename = true;
    workflowTest.copyVariablesFrom( null );
    workflowTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    workflowTest.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    workflowTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "file:///C:/SomeFilenameDirectory", workflowTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    Workflow workflowTest = new Workflow();
    workflowTest.copyVariablesFrom( null );
    boolean hasFilename = false;
    workflowTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    workflowTest.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    workflowTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "Original value defined at run execution", workflowTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }


}
