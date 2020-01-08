/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.ftpsget;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.job.Job;
import org.apache.hop.job.entry.JobEntryBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class JobEntryFTPSGetIT {

  private static FtpsServer server;
  private static String ramDir;

  @BeforeClass
  public static void createServer() throws Exception {
    HopEnvironment.init();

    server = FtpsServer.createDefaultServer();
    server.start();

    ramDir = "ram://" + JobEntryFTPSGetIT.class.getSimpleName();
    HopVFS.getFileObject( ramDir ).createFolder();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    if ( server != null ) {
      server.stop();
      server = null;
    }

    HopVFS.getFileObject( ramDir ).delete();
  }


  @Test
  public void downloadFile_WhenDestinationIsSetViaVariable() throws Exception {
    final String myVar = "my-var";
    final String expectedDownloadedFilePath = ramDir + "/" + FtpsServer.SAMPLE_FILE;

    JobEntryFTPSGet job = createCommonJob();
    job.setVariable( myVar, ramDir );
    job.setTargetDirectory( String.format( "${%s}", myVar ) );

    FileObject downloaded = HopVFS.getFileObject( expectedDownloadedFilePath );
    assertFalse( downloaded.exists() );
    try {
      job.execute( new Result(), 1 );
      downloaded = HopVFS.getFileObject( expectedDownloadedFilePath );
      assertTrue( downloaded.exists() );
    } finally {
      downloaded.delete();
    }
  }

  @Test
  public void downloadFile_WhenDestinationIsSetDirectly() throws Exception {
    JobEntryFTPSGet job = createCommonJob();
    job.setTargetDirectory( ramDir );

    FileObject downloaded = HopVFS.getFileObject( ramDir + "/" + FtpsServer.SAMPLE_FILE );
    assertFalse( downloaded.exists() );
    try {
      job.execute( new Result(), 1 );
      downloaded = HopVFS.getFileObject( ramDir + "/" + FtpsServer.SAMPLE_FILE );
      assertTrue( downloaded.exists() );
    } finally {
      downloaded.delete();
    }
  }


  private static JobEntryFTPSGet createCommonJob() {
    JobEntryFTPSGet job = new JobEntryFTPSGet();
    setMockParent( job );
    setCommonServerProperties( job );
    return job;
  }

  private static void setMockParent( JobEntryBase job ) {
    Job parent = mock( Job.class );
    when( parent.isStopped() ).thenReturn( false );
    job.setParentJob( parent );
    job.setLogLevel( LogLevel.NOTHING );
  }

  private static void setCommonServerProperties( JobEntryFTPSGet job ) {
    job.setConnectionType( FTPSConnection.CONNECTION_TYPE_FTP_IMPLICIT_SSL );
    job.setUserName( FtpsServer.ADMIN );
    job.setPassword( FtpsServer.PASSWORD );
    job.setServerName( "localhost" );
    job.setPort( Integer.toString( FtpsServer.DEFAULT_PORT ) );
    job.setFTPSDirectory( "/" );
  }
}
