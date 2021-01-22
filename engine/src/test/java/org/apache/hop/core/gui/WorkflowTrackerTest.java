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

package org.apache.hop.core.gui;

import org.apache.hop.core.Const;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowTrackerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  // PDI-11389 Number of workflow trackers should be limited by HOP_MAX_JOB_TRACKER_SIZE
  public void testAddJobTracker() throws Exception {
    final String old = System.getProperty( Const.HOP_MAX_WORKFLOW_TRACKER_SIZE );

    final Integer maxTestSize = 30;
    try {
      System.setProperty( Const.HOP_MAX_WORKFLOW_TRACKER_SIZE, maxTestSize.toString() );

      WorkflowMeta workflowMeta = mock( WorkflowMeta.class );
      WorkflowTracker workflowTracker = new WorkflowTracker( workflowMeta );

      for ( int n = 0; n < maxTestSize * 2; n++ ) {
        workflowTracker.addWorkflowTracker( mock( WorkflowTracker.class ) );
      }

      assertTrue( "More JobTrackers than allowed were added", workflowTracker.getTotalNumberOfItems() <= maxTestSize );
    } finally {
      if ( old == null ) {
        System.clearProperty( Const.HOP_MAX_WORKFLOW_TRACKER_SIZE );
      } else {
        System.setProperty( Const.HOP_MAX_WORKFLOW_TRACKER_SIZE, old );
      }
    }
  }


  @Test
  public void findJobTracker_EntryNameIsNull() {
    WorkflowTracker workflowTracker = createTracker();
    workflowTracker.addWorkflowTracker( createTracker() );

    ActionMeta actionMeta = createActionMeta( null );

    assertNull( workflowTracker.findWorkflowTracker( actionMeta ) );
  }

  @Test
  public void findJobTracker_EntryNameNotFound() {
    WorkflowTracker workflowTracker = createTracker();
    for ( int i = 0; i < 3; i++ ) {
      workflowTracker.addWorkflowTracker( createTracker( Integer.toString( i ) ) );
    }

    ActionMeta copy = createActionMeta( "not match" );

    assertNull( workflowTracker.findWorkflowTracker( copy ) );
  }

  @Test
  public void findJobTracker_EntryNameFound() {
    WorkflowTracker workflowTracker = createTracker();
    WorkflowTracker[] children = new WorkflowTracker[] {
      createTracker( "0" ),
      createTracker( "1" ),
      createTracker( "2" )
    };
    for ( WorkflowTracker child : children ) {
      workflowTracker.addWorkflowTracker( child );
    }

    ActionMeta actionMeta = createActionMeta( "1" );

    assertEquals( children[ 1 ], workflowTracker.findWorkflowTracker( actionMeta ) );
  }


  private static WorkflowTracker createTracker() {
    return createTracker( null );
  }

  private static WorkflowTracker createTracker( String actionName ) {
    WorkflowMeta workflowMeta = mock( WorkflowMeta.class );
    WorkflowTracker workflowTracker = new WorkflowTracker( workflowMeta );
    if ( actionName != null ) {
      ActionResult result = mock( ActionResult.class );
      when( result.getActionName() ).thenReturn( actionName );
      workflowTracker.setActionResult( result );
    }
    return workflowTracker;
  }

  private static ActionMeta createActionMeta( String actionName ) {
    IAction action = mock( IAction.class );
    when( action.getName() ).thenReturn( actionName );

    ActionMeta actionMeta = new ActionMeta( action );
    return actionMeta;
  }

}
