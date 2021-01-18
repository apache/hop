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

package org.apache.hop.workflow.actions.columnsexist;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for column exist action.
 *
 * @author Tim Ryzhov
 * @since 21-03-2017
 */

public class WorkflowActionColumnsExistTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String TABLENAME = "TABLE";
  private static final String SCHEMANAME = "SCHEMA";
  private static final String[] COLUMNS = new String[] { "COLUMN1", "COLUMN2" };
  private ActionColumnsExist action;
  private Database db;


  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @AfterClass
  public static void tearDown() {
    HopEnvironment.reset();
  }

  @Before
  public void setUp() {
    IWorkflowEngine<WorkflowMeta> parentWorkflow = new LocalWorkflowEngine( new WorkflowMeta() );
    action = spy( new ActionColumnsExist( "" ) );
    parentWorkflow.getWorkflowMeta().addAction( new ActionMeta( action ) );
    parentWorkflow.setStopped( false );
    action.setParentWorkflow( parentWorkflow );
    parentWorkflow.setLogLevel( LogLevel.NOTHING );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    action.setDatabase( dbMeta );
    db = spy( new Database( action, action, dbMeta ) );
    action.setParentWorkflow( parentWorkflow );
    action.setTablename( TABLENAME );
    action.setArguments( COLUMNS );
    action.setSchemaname( SCHEMANAME );
  }

  @Test
  public void jobFail_tableNameIsEmpty() throws HopException {
    action.setTablename( null );
    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_columnsArrayIsEmpty() throws HopException {
    action.setArguments( null );
    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_connectionIsNull() throws HopException {
    action.setDatabase( null );
    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_tableNotExist() throws HopException {
    when( action.getNewDatabaseFromMeta() ).thenReturn( db );
    doNothing().when( db ).connect( anyString(), any() );
    doReturn( false ).when( db ).checkTableExists( anyString(), anyString() );

    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
    verify( db, atLeastOnce() ).disconnect();
  }

  @Test
  public void jobFail_columnNotExist() throws HopException {
    doReturn( db ).when( action ).getNewDatabaseFromMeta();
    doNothing().when( db ).connect( anyString(), anyString() );
    doReturn( true ).when( db ).checkTableExists( anyString(), anyString() );
    doReturn( false ).when( db ).checkColumnExists( anyString(), anyString(), anyString() );
    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be some errors", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
    verify( db, atLeastOnce() ).disconnect();
  }

  @Test
  public void jobSuccess() throws HopException {
    doReturn( db ).when( action ).getNewDatabaseFromMeta();
    doNothing().when( db ).connect( anyString(), anyString() );
    doReturn( true ).when( db ).checkColumnExists( anyString(), anyString(), anyString() );
    doReturn( true ).when( db ).checkTableExists( anyString(), anyString() );
    final Result result = action.execute( new Result(), 0 );
    assertEquals( "Should be no error", 0, result.getNrErrors() );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "Lines written", COLUMNS.length, result.getNrLinesWritten() );
    verify( db, atLeastOnce() ).disconnect();
  }
}
