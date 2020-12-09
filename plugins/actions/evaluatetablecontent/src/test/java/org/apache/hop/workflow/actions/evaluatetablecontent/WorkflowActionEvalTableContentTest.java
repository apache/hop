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

package org.apache.hop.workflow.actions.evaluatetablecontent;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/*
 * tests fix for PDI-1044
 * Action: Evaluate rows number in a table:
 * PDI Server logs with error from Quartz even though the workflow finishes successfully.
 */
public class WorkflowActionEvalTableContentTest {
  private static final Map<Class<?>, String> dbMap = new HashMap<>();
  private ActionEvalTableContent action;
  private static IPlugin mockDbPlugin;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static class DBMockIface extends BaseDatabaseMeta {

    @Override
    public Object clone() {
      return this;
    }

    @Override
    public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean useAutoIncrement,
                                      boolean addFieldName, boolean addCr ) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getDriverClass() {
      return MockDriver.class.getName();
    }

    @Override
    public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
      return "";
    }

    @Override
    public String getAddColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoIncrement,
                                         String pk, boolean semicolon ) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getModifyColumnStatement( String tableName, IValueMeta v, String tk,
                                            boolean useAutoIncrement, String pk, boolean semicolon ) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int[] getAccessTypeList() {
      // TODO Auto-generated method stub
      return null;
    }

  }

  // private static DBMockIface dbi = DBMockIface();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
    dbMap.put( IDatabase.class, DBMockIface.class.getName() );

    PluginRegistry preg = PluginRegistry.getInstance();

    mockDbPlugin = mock( IPlugin.class );
    when( mockDbPlugin.matches( anyString() ) ).thenReturn( true );
    when( mockDbPlugin.isNativePlugin() ).thenReturn( true );
    when( mockDbPlugin.getMainType() ).thenAnswer( (Answer<Class<?>>) invocation -> IDatabase.class );

    when( mockDbPlugin.getPluginType() ).thenAnswer( (Answer<Class<? extends IPluginType>>) invocation -> DatabasePluginType.class );

    when( mockDbPlugin.getIds() ).thenReturn( new String[] { "Oracle", "mock-db-id" } );
    when( mockDbPlugin.getName() ).thenReturn( "mock-db-name" );
    when( mockDbPlugin.getClassMap() ).thenReturn( dbMap );

    preg.registerPlugin( DatabasePluginType.class, mockDbPlugin );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    HopClientEnvironment.reset();
  }

  @Before
  public void setUp() throws Exception {
    MockDriver.registerInstance();
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine( new WorkflowMeta() );
    action = new ActionEvalTableContent();

    workflow.getWorkflowMeta().addAction( new ActionMeta( action ) );
    action.setParentWorkflow( workflow );

    workflow.setStopped( false );

    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setDatabaseType( "mock-db" );

    action.setDatabase( dbMeta );
    action.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "N" );
  }

  @After
  public void tearDown() throws Exception {
    MockDriver.deregeisterInstances();
  }

  @Test
  public void testNrErrorsFailureNewBehavior() throws Exception {
    action.setLimit( "1" );
    action.setSuccessCondition( ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    action.setTablename( "table" );

    Result res = action.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals(
      "No errors should be reported in result object accoding to the new behavior", res.getNrErrors(), 0 );
  }

  @Test
  public void testNrErrorsFailureOldBehavior() throws Exception {
    action.setLimit( "1" );
    action.setSuccessCondition( ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    action.setTablename( "table" );

    action.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "Y" );

    Result res = action.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals(
      "An error should be reported in result object accoding to the old behavior", res.getNrErrors(), 1 );
  }

  @Test
  public void testNrErrorsSuccess() throws Exception {
    action.setLimit( "5" );
    action.setSuccessCondition( ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    action.setTablename( "table" );

    Result res = action.execute( new Result(), 0 );

    assertTrue( "Eval number of rows should be suceeded", res.getResult() );
    assertEquals( "Apparently there should no error", res.getNrErrors(), 0 );

    // that should work regardless of old/new behavior flag
    action.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "Y" );

    res = action.execute( new Result(), 0 );

    assertTrue( "Eval number of rows should be suceeded", res.getResult() );
    assertEquals( "Apparently there should no error", res.getNrErrors(), 0 );
  }

  @Test
  public void testNrErrorsNoCustomSql() throws Exception {
    action.setLimit( "5" );
    action.setSuccessCondition( ActionEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    action.setUseCustomSql( true );
    action.setCustomSql( null );

    Result res = action.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals( "Apparently there should be an error", res.getNrErrors(), 1 );

    // that should work regardless of old/new behavior flag
    action.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "Y" );

    res = action.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals( "Apparently there should be an error", res.getNrErrors(), 1 );
  }
}
