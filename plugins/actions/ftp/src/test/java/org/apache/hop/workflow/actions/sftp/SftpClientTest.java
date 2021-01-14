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

package org.apache.hop.workflow.actions.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetAddress;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SftpClientTest {
  @ClassRule
  public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private int port = 22;
  private String username = "admin";
  private String password = "password";
  private Session session = mock( Session.class );
  private ChannelSftp channel = mock( ChannelSftp.class );
  private InetAddress server = mock( InetAddress.class );
  private JSch jSch = mock( JSch.class );

  @Before
  public void setUp() throws JSchException {
    System.clearProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI );

    when( server.getHostAddress() ).thenReturn( "localhost" );
    when( jSch.getSession( username, "localhost", port ) ).thenReturn( session );
    when( session.openChannel( "sftp" ) ).thenReturn( channel );
  }

  @After
  public void tearDown() {
    System.clearProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI );
  }

  /**
   * Given SFTP connection configuration, and -Duserauth.gssapi.enabled param was NOT passed on application start.
   * <br/>
   * When SFTP Client is instantiated, then preferred authentications list should not contain
   * GSS API Authentication.
   */
  @Test
  public void shouldExcludeGssapiFromPreferredAuthenticationsByDefault() throws Exception {
    new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    };

    verify( session )
      .setConfig( "PreferredAuthentications", "publickey,keyboard-interactive,password" );
  }

  /**
   * Given SFTP connection configuration, and -Duserauth.gssapi.enabled param
   * was passed on application start with correct value.
   * <br/>
   * When SFTP Client is instantiated, then preferred authentications list should contain
   * GSS API Authentication as the last one.
   */
  @Test
  public void shouldIncludeGssapiToPreferredAuthenticationsIfSpecified() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "true" );

    new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    };

    verify( session )
      .setConfig( "PreferredAuthentications", "publickey,keyboard-interactive,password,gssapi-with-mic" );
  }

  /**
   * Given SFTP connection configuration, and -Duserauth.gssapi.enabled param
   * was passed on application start with incorrect value.
   * <br/>
   * When SFTP Client is instantiated, then preferred authentications list should not contain
   * GSS API Authentication.
   */
  @Test
  public void shouldIncludeGssapiToPreferredAuthenticationsIfOnlySpecifiedCorrectly() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );

    new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    };

    verify( session )
      .setConfig( "PreferredAuthentications", "publickey,keyboard-interactive,password" );
  }

  /**
   * Can't create root folder. An exception is expected.
   */
  @Test( expected = HopWorkflowException.class )
  public void folderCreationEmptyTest() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    };

    client.login( password );
    client.createFolder( "//" );
  }

  /**
   * Create a folder under the current user's home.
   */
  @Test
  public void folderCreation_Relative_Simple() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = spy( new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    } );

    doReturn( "/home/admin" ).when( client ).pwd();

    client.login( password );
    client.createFolder( "myfolder" );

    verify( channel, times( 1 ) ).mkdir( anyString() );
    verify( channel, times( 1 ) ).mkdir( "/home/admin/myfolder" );
  }

  /**
   * Create a folder with nested folders under the current user's home.
   */
  @Test
  public void folderCreation_Relative_Nested() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = spy( new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    } );

    doReturn( "/home/admin" ).when( client ).pwd();

    client.login( password );
    client.createFolder( "myfolder/subfolder/finalfolder" );

    verify( channel, times( 3 ) ).mkdir( anyString() );
    verify( channel, times( 1 ) ).mkdir( "/home/admin/myfolder" );
    verify( channel, times( 1 ) ).mkdir( "/home/admin/myfolder/subfolder" );
    verify( channel, times( 1 ) ).mkdir( "/home/admin/myfolder/subfolder/finalfolder" );
  }

  /**
   * Create a folder under an existing folder given an absolute path.
   */
  @Test
  public void folderCreation_Absolute_Simple() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = spy( new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    } );

    doReturn( true ).when( client ).folderExists( "/var" );
    doReturn( true ).when( client ).folderExists( "/var/ftproot" );

    client.login( password );
    client.createFolder( "/var/ftproot/myfolder" );

    verify( channel, times( 1 ) ).mkdir( anyString() );
    verify( channel, times( 1 ) ).mkdir( "/var/ftproot/myfolder" );
  }

  /**
   * Create a folder under an existing folder given an absolute path.
   * The specified folder ends with a slash.
   */
  @Test
  public void folderCreation_Absolute_TrailingSlash() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = spy( new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    } );

    doReturn( true ).when( client ).folderExists( "/var" );
    doReturn( true ).when( client ).folderExists( "/var/ftproot" );

    client.login( password );
    client.createFolder( "/var/ftproot/myfolder/" );

    verify( channel, times( 1 ) ).mkdir( anyString() );
    verify( channel, times( 1 ) ).mkdir( "/var/ftproot/myfolder" );
  }

  /**
   * Create a folder with nested folders under an existing folder given an absolute path.
   */
  @Test
  public void folderCreation_Absolute_Nested() throws Exception {
    System.setProperty( SftpClient.ENV_PARAM_USERAUTH_GSSAPI, "yes" );
    SftpClient client = spy( new SftpClient( server, port, username ) {
      @Override
      JSch createJSch() {
        return jSch;
      }
    } );

    doReturn( true ).when( client ).folderExists( "/var" );
    doReturn( true ).when( client ).folderExists( "/var/ftproot" );

    client.login( password );
    client.createFolder( "/var/ftproot/myfolder/subfolder/finalfolder" );

    verify( channel, times( 3 ) ).mkdir( anyString() );
    verify( channel, times( 1 ) ).mkdir( "/var/ftproot/myfolder" );
    verify( channel, times( 1 ) ).mkdir( "/var/ftproot/myfolder/subfolder" );
    verify( channel, times( 1 ) ).mkdir( "/var/ftproot/myfolder/subfolder/finalfolder" );
  }
}
