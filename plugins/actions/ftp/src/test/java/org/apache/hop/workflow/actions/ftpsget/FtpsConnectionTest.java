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

package org.apache.hop.workflow.actions.ftpsget;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.actions.ftpsget.ftp4che.SecureDataFtpConnection;
import org.ftp4che.FTPConnection;
import org.ftp4che.commands.Command;
import org.ftp4che.exception.AuthenticationNotSupportedException;
import org.ftp4che.exception.ConfigurationException;
import org.ftp4che.exception.FtpIOException;
import org.ftp4che.exception.FtpWorkflowException;
import org.ftp4che.exception.NotConnectedException;
import org.ftp4che.io.SocketProvider;
import org.ftp4che.reply.Reply;
import org.ftp4che.util.ftpfile.FTPFileFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class FtpsConnectionTest {

  @Test
  public void testEnforceProtP() throws Exception {
    FtpsTestConnection connection = spy(
      new FtpsTestConnection(
        FtpsConnection.CONNECTION_TYPE_FTP_IMPLICIT_TLS_WITH_CRYPTED,
        "the.perfect.host", 2010, "warwickw", "julia", null ) );
    connection.replies.put( "PWD", new Reply( Arrays.asList( "257 \"/la\" is current directory" ) ) );
    connection.connect();
    connection.getFileNames();
    assertEquals( "buffer not set", "PBSZ 0\r\n", connection.commands.get( 1 ).toString() );
    assertEquals( "data privacy not set", "PROT P\r\n", connection.commands.get( 2 ).toString() );
  }

  @Test
  public void testEnforceProtPOnPut() throws Exception {
    FileObject file = HopVfs.createTempFile( "FTPSConnectionTest_testEnforceProtPOnPut", HopVfs.Suffix.TMP );
    file.createFile();
    try {
      FtpsTestConnection connection = spy(
        new FtpsTestConnection(
          FtpsConnection.CONNECTION_TYPE_FTP_IMPLICIT_TLS_WITH_CRYPTED,
          "the.perfect.host", 2010, "warwickw", "julia", null ) );
      connection.replies.put( "PWD", new Reply( Arrays.asList( "257 \"/la\" is current directory" ) ) );
      connection.connect();
      connection.uploadFile( file.getPublicURIString(), "uploaded-file" );
      assertEquals( "buffer not set", "PBSZ 0\r\n", connection.commands.get( 0 ).toString() );
      assertEquals( "data privacy not set", "PROT P\r\n", connection.commands.get( 1 ).toString() );
    } finally {
      file.delete();
    }
  }

  static class FtpsTestConnection extends FtpsConnection {
    public List<Command> commands = new ArrayList<>();
    public SocketProvider connectionSocketProvider;
    public Map<String, Reply> replies = new HashMap<>();

    public FtpsTestConnection( int connectionType, String hostname, int port, String username, String password,
                               IVariables nameSpace ) {
      super( connectionType, hostname, port, username, password, nameSpace );
    }

    @Override
    protected FTPConnection getSecureDataFtpConnection(FTPConnection connection, String password, int timeout )
      throws ConfigurationException {
      return new SecureDataFtpConnection( connection, password, timeout ) {
        private Reply dummyReply = new Reply();

        @Override
        public void connect() throws NotConnectedException, IOException, AuthenticationNotSupportedException,
          FtpIOException, FtpWorkflowException {
          socketProvider = mock( SocketProvider.class );
          when( socketProvider.socket() ).thenReturn( mock( Socket.class ) );
          when( socketProvider.read( any() ) ).thenReturn( -1 );
          connectionSocketProvider = socketProvider;
          factory = new FTPFileFactory( "UNIX" );
        }

        @Override
        public SocketProvider sendPortCommand( Command command, Reply commandReply )
          throws IOException, FtpWorkflowException, FtpIOException {
          return socketProvider;
        }

        @Override
        public Reply sendCommand( Command cmd ) throws IOException {
          commands.add( cmd );
          return Optional.ofNullable( replies.get( cmd.getCommand() ) ).orElse( dummyReply );
        }
      };
    }
  }
}
