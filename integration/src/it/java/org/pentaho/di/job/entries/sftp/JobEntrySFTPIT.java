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

package org.apache.hop.job.entries.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.job.Job;
import org.apache.hop.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class JobEntrySFTPIT {

  private static TemporaryFolder folder;

  private static SftpServer server;

  @BeforeClass
  public static void prepareEnv() throws Exception {
    HopEnvironment.init();

    folder = new TemporaryFolder();
    folder.create();

    server = SftpServer.createDefaultServer( folder );
    server.start();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    server.stop();
    server = null;

    folder.delete();
    folder = null;
  }

  @Test
  public void getFile_WhenDestinationIsSetViaVariable() throws Exception {
    final String localDir = TestUtils.createTempDir();
    HopVFS.getFileObject( localDir ).createFolder();

    final String myVar = "my-var";

    final String sftpDir = "job-entry-sftp-test";
    final String fileName = "file.txt";

    uploadFile( sftpDir, fileName );

    JobEntrySFTP job = new JobEntrySFTP();
    job.setVariable( myVar, localDir );

    Job parent = mock( Job.class );
    when( parent.isStopped() ).thenReturn( false );
    job.setParentJob( parent );
    job.setLogLevel( LogLevel.NOTHING );

    job.setUserName( server.getUsername() );
    job.setPassword( server.getPassword() );
    job.setServerName( "localhost" );
    job.setServerPort( Integer.toString( server.getPort() ) );
    job.setScpDirectory( sftpDir );
    job.setTargetDirectory( String.format( "${%s}", myVar ) );

    job.execute( new Result(), 1 );

    FileObject downloaded = HopVFS.getFileObject( localDir + "/" + fileName );
    assertTrue( downloaded.exists() );
    downloaded.delete();
  }

  private void uploadFile( String dir, String file ) throws Exception {
    Session session = server.createJschSession();
    session.connect();
    try {
      ChannelSftp sftp = (ChannelSftp) session.openChannel( "sftp" );
      sftp.connect();
      try {
        sftp.mkdir( dir );
        sftp.cd( dir );
        sftp.put( new ByteArrayInputStream( "data".getBytes() ), file );
      } finally {
        sftp.disconnect();
      }
    } finally {
      session.disconnect();
    }
  }
}
