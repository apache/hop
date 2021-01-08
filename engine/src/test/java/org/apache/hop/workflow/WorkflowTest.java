/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class WorkflowTest {
  private static final String STRING_DEFAULT = "<def>";
  private IWorkflowEngine<WorkflowMeta> mockedWorkflow;
  private Database mockedDataBase;
  private IVariables mockedVariableSpace;
  private IHopMetadataProvider mockedMetadataProvider;
  private WorkflowMeta mockedWorkflowMeta;
  private ActionMeta mockedActionMeta;
  private ActionStart mockedActionStart;
  private LogChannel mockedLogChannel;


  @Before
  public void init() {
    mockedDataBase = mock( Database.class );
    mockedWorkflow = mock( Workflow.class );
    mockedVariableSpace = mock( IVariables.class );
    mockedMetadataProvider = mock( IHopMetadataProvider.class );
    mockedWorkflowMeta = mock( WorkflowMeta.class );
    mockedActionMeta = mock( ActionMeta.class );
    mockedActionStart = mock( ActionStart.class );
    mockedLogChannel = mock( LogChannel.class );
  }

  @Test
  public void testNewJobWithContainerObjectId() {
    WorkflowMeta meta = mock( WorkflowMeta.class );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerId();

    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine( meta );

    assertEquals( carteId, workflow.getContainerId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17398.
   * When a workflow is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoWorkflowsGetSameLogChannelId() {
    WorkflowMeta meta = mock( WorkflowMeta.class );

    IWorkflowEngine<WorkflowMeta> workflow1 = new LocalWorkflowEngine( meta );
    IWorkflowEngine<WorkflowMeta> workflow2 = new LocalWorkflowEngine( meta );

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

    doReturn( carteId1 ).when( meta1 ).getContainerId();
    doReturn( carteId2 ).when( meta2 ).getContainerId();

    IWorkflowEngine<WorkflowMeta> workflow1 = new LocalWorkflowEngine( meta1 );
    IWorkflowEngine<WorkflowMeta> workflow2 = new LocalWorkflowEngine( meta2 );

    assertNotEquals( workflow1.getContainerId(), workflow2.getContainerId() );
    assertNotEquals( workflow1.getLogChannelId(), workflow2.getLogChannelId() );
  }

}
