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

package org.apache.hop.job.entries.columnsexist;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for column exist job entry.
 *
 * @author Tim Ryzhov
 * @since 21-03-2017
 *
 */

public class JobEntryColumnsExistTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String TABLENAME = "TABLE";
  private static final String SCHEMANAME = "SCHEMA";
  private static final String[] COLUMNS = new String[]{"COLUMN1", "COLUMN2"};
  private JobEntryColumnsExist jobEntry;
  private Database db;


  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init( false );
  }

  @AfterClass
  public static void tearDown() {
    HopEnvironment.reset();
  }

  @Before
  public void setUp() {
    Job parentJob = new Job( null, new JobMeta() );
    jobEntry = spy( new JobEntryColumnsExist( "" ) );
    parentJob.getJobMeta().addJobEntry( new JobEntryCopy( jobEntry ) );
    parentJob.setStopped( false );
    jobEntry.setParentJob( parentJob );
    parentJob.setLogLevel( LogLevel.NOTHING );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    jobEntry.setDatabase( dbMeta );
    db = spy( new Database( jobEntry, dbMeta ) );
    jobEntry.setParentJob( parentJob );
    jobEntry.setTablename( TABLENAME );
    jobEntry.setArguments( COLUMNS );
    jobEntry.setSchemaname( SCHEMANAME );
  }

  @Test
  public void jobFail_tableNameIsEmpty() throws HopException {
    jobEntry.setTablename( null );
    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_columnsArrayIsEmpty() throws HopException {
    jobEntry.setArguments( null );
    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_connectionIsNull() throws HopException {
    jobEntry.setDatabase( null );
    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
  }

  @Test
  public void jobFail_tableNotExist() throws HopException {
    when( jobEntry.getNewDatabaseFromMeta() ).thenReturn( db );
    doNothing().when( db ).connect( anyString(), any() );
    doReturn( false ).when( db ).checkTableExists( anyString(), anyString() );

    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be error", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
    verify( db, atLeastOnce() ).disconnect();
  }

  @Test
  public void jobFail_columnNotExist() throws HopException {
    doReturn( db ).when( jobEntry ).getNewDatabaseFromMeta();
    doNothing().when( db ).connect( anyString(), anyString() );
    doReturn( true ).when( db ).checkTableExists( anyString(), anyString() );
    doReturn( false ).when( db ).checkColumnExists( anyString(), anyString(), anyString() );
    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be some errors", 1, result.getNrErrors() );
    assertFalse( "Result should be false", result.getResult() );
    verify( db, atLeastOnce() ).disconnect();
  }

  @Test
  public void jobSuccess() throws HopException {
    doReturn( db ).when( jobEntry ).getNewDatabaseFromMeta();
    doNothing().when( db ).connect( anyString(), anyString() );
    doReturn( true ).when( db ).checkColumnExists( anyString(), anyString(), anyString() );
    doReturn( true ).when( db ).checkTableExists( anyString(), anyString() );
    final Result result = jobEntry.execute( new Result(), 0 );
    assertEquals( "Should be no error", 0, result.getNrErrors() );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "Lines written", COLUMNS.length, result.getNrLinesWritten() );
    verify( db, atLeastOnce() ).disconnect();
  }
}
