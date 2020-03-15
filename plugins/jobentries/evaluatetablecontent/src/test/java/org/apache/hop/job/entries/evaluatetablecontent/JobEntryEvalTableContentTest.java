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

package org.apache.hop.job.entries.evaluatetablecontent;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
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
 * Job entry: Evaluate rows number in a table:
 * PDI Server logs with error from Quartz even though the job finishes successfully.
 */
public class JobEntryEvalTableContentTest {
  private static final Map<Class<?>, String> dbMap = new HashMap<Class<?>, String>();
  JobEntryEvalTableContent entry;
  private static PluginInterface mockDbPlugin;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static class DBMockIface extends BaseDatabaseMeta {

    @Override
    public Object clone() {
      return this;
    }

    @Override
    public String getFieldDefinition( ValueMetaInterface v, String tk, String pk, boolean use_autoinc,
                                      boolean add_fieldname, boolean add_cr ) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getDriverClass() {
      return "org.apache.hop.job.entries.evaluatetablecontent.MockDriver";
    }

    @Override
    public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
      return "";
    }

    @Override
    public String getAddColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                         String pk, boolean semicolon ) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getModifyColumnStatement( String tablename, ValueMetaInterface v, String tk,
                                            boolean use_autoinc, String pk, boolean semicolon ) {
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
    dbMap.put( DatabaseInterface.class, DBMockIface.class.getName() );

    PluginRegistry preg = PluginRegistry.getInstance();

    mockDbPlugin = mock( PluginInterface.class );
    when( mockDbPlugin.matches( anyString() ) ).thenReturn( true );
    when( mockDbPlugin.isNativePlugin() ).thenReturn( true );
    when( mockDbPlugin.getMainType() ).thenAnswer( (Answer<Class<?>>) invocation -> DatabaseInterface.class );

    when( mockDbPlugin.getPluginType() ).thenAnswer(
      (Answer<Class<? extends PluginTypeInterface>>) invocation -> DatabasePluginType.class );

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
    Job job = new Job( new JobMeta() );
    entry = new JobEntryEvalTableContent();

    job.getJobMeta().addJobEntry( new JobEntryCopy( entry ) );
    entry.setParentJob( job );

    job.setStopped( false );

    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setDatabaseType( "mock-db" );

    entry.setDatabase( dbMeta );
  }

  @After
  public void tearDown() throws Exception {
    MockDriver.deregeisterInstances();
  }

  @Test
  public void testNrErrorsFailureNewBehavior() throws Exception {
    entry.setLimit( "1" );
    entry.setSuccessCondition( JobEntryEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    entry.setTablename( "table" );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals(
      "No errors should be reported in result object accoding to the new behavior", res.getNrErrors(), 0 );
  }

  @Test
  public void testNrErrorsFailureOldBehavior() throws Exception {
    entry.setLimit( "1" );
    entry.setSuccessCondition( JobEntryEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    entry.setTablename( "table" );

    entry.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_JOB_ENTRIES, "Y" );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals(
      "An error should be reported in result object accoding to the old behavior", res.getNrErrors(), 1 );
  }

  @Test
  public void testNrErrorsSuccess() throws Exception {
    entry.setLimit( "5" );
    entry.setSuccessCondition( JobEntryEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    entry.setTablename( "table" );

    Result res = entry.execute( new Result(), 0 );

    assertTrue( "Eval number of rows should be suceeded", res.getResult() );
    assertEquals( "Apparently there should no error", res.getNrErrors(), 0 );

    // that should work regardless of old/new behavior flag
    entry.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_JOB_ENTRIES, "Y" );

    res = entry.execute( new Result(), 0 );

    assertTrue( "Eval number of rows should be suceeded", res.getResult() );
    assertEquals( "Apparently there should no error", res.getNrErrors(), 0 );
  }

  @Test
  public void testNrErrorsNoCustomSql() throws Exception {
    entry.setLimit( "5" );
    entry.setSuccessCondition( JobEntryEvalTableContent.SUCCESS_CONDITION_ROWS_COUNT_EQUAL );
    entry.setUseCustomSQL( true );
    entry.setCustomSQL( null );

    Result res = entry.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals( "Apparently there should be an error", res.getNrErrors(), 1 );

    // that should work regardless of old/new behavior flag
    entry.setVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_JOB_ENTRIES, "Y" );

    res = entry.execute( new Result(), 0 );

    assertFalse( "Eval number of rows should fail", res.getResult() );
    assertEquals( "Apparently there should be an error", res.getNrErrors(), 1 );
  }
}
