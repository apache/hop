/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job;

import org.apache.hop.metastore.api.IMetaStore;
import org.junit.Before;
import org.junit.Test;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.BaseLogTable;
import org.apache.hop.core.logging.JobEntryLogTable;
import org.apache.hop.core.logging.JobLogTable;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogStatus;
import org.apache.hop.core.logging.LogTableField;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.job.entries.special.JobEntrySpecial;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.trans.HasDatabasesInterface;

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
import static org.powermock.reflect.Whitebox.setInternalState;

public class JobTest {
  private static final String STRING_DEFAULT = "<def>";
  private Job mockedJob;
  private Database mockedDataBase;
  private VariableSpace mockedVariableSpace;
  private IMetaStore mockedMetaStore;
  private JobMeta mockedJobMeta;
  private JobEntryCopy mockedJobEntryCopy;
  private JobEntrySpecial mockedJobEntrySpecial;
  private LogChannel mockedLogChannel;


  @Before
  public void init() {
    mockedDataBase = mock( Database.class );
    mockedJob = mock( Job.class );
    mockedVariableSpace = mock( VariableSpace.class );
    mockedMetaStore = mock( IMetaStore.class );
    mockedJobMeta = mock( JobMeta.class );
    mockedJobEntryCopy = mock( JobEntryCopy.class );
    mockedJobEntrySpecial = mock( JobEntrySpecial.class );
    mockedLogChannel = mock( LogChannel.class );

    when( mockedJob.createDataBase( any( DatabaseMeta.class ) ) ).thenReturn( mockedDataBase );
  }

  @Test
  public void recordsCleanUpMethodIsCalled_JobEntryLogTable() throws Exception {

    JobEntryLogTable jobEntryLogTable = JobEntryLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    setAllTableParamsDefault( jobEntryLogTable );

    JobMeta jobMeta = new JobMeta(  );
    jobMeta.setJobEntryLogTable( jobEntryLogTable );

    when( mockedJob.getJobMeta() ).thenReturn( jobMeta );
    doCallRealMethod().when( mockedJob ).writeJobEntryLogInformation();

    mockedJob.writeJobEntryLogInformation();

    verify( mockedDataBase ).cleanupLogRecords( jobEntryLogTable );
  }

  @Test
  public void recordsCleanUpMethodIsCalled_JobLogTable() throws Exception {
    JobLogTable jobLogTable = JobLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    setAllTableParamsDefault( jobLogTable );

    doCallRealMethod().when( mockedJob ).writeLogTableInformation( jobLogTable, LogStatus.END );

    mockedJob.writeLogTableInformation( jobLogTable, LogStatus.END );

    verify( mockedDataBase ).cleanupLogRecords( jobLogTable );
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
    JobMeta meta = mock( JobMeta.class );

    String carteId = UUID.randomUUID().toString();
    doReturn( carteId ).when( meta ).getContainerObjectId();

    Job job = new Job( meta );

    assertEquals( carteId, job.getContainerObjectId() );
  }

  /**
   * This test demonstrates the issue fixed in PDI-17398.
   * When a job is scheduled twice, it gets the same log channel Id and both logs get merged
   */
  @Test
  public void testTwoJobsGetSameLogChannelId() {
    JobMeta meta = mock( JobMeta.class );

    Job job1 = new Job( meta );
    Job job2 = new Job( meta );

    assertEquals( job1.getLogChannelId(), job2.getLogChannelId() );
  }

  /**
   * This test demonstrates the fix for PDI-17398.
   * Two schedules -> two HopServer object Ids -> two log channel Ids
   */
  @Test
  public void testTwoJobsGetDifferentLogChannelIdWithDifferentCarteId() {
    JobMeta meta1 = mock( JobMeta.class );
    JobMeta meta2 = mock( JobMeta.class );

    String carteId1 = UUID.randomUUID().toString();
    String carteId2 = UUID.randomUUID().toString();

    doReturn( carteId1 ).when( meta1 ).getContainerObjectId();
    doReturn( carteId2 ).when( meta2 ).getContainerObjectId();

    Job job1 = new Job( meta1 );
    Job job2 = new Job( meta2 );

    assertNotEquals( job1.getContainerObjectId(), job2.getContainerObjectId() );
    assertNotEquals( job1.getLogChannelId(), job2.getLogChannelId() );
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename( ) {
    Job jobTest = new Job(  );
    boolean hasFilename = true;
    boolean hasRepoDir = false;
    jobTest.copyVariablesFrom( null );
    jobTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "file:///C:/SomeFilenameDirectory", jobTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename( ) {
    Job jobTest = new Job(  );
    jobTest.copyVariablesFrom( null );
    boolean hasFilename = false;
    jobTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    jobTest.setVariable( Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    jobTest.setInternalEntryCurrentDirectory( hasFilename );

    assertEquals( "Original value defined at run execution", jobTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY )  );
  }


}
