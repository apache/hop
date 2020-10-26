/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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
package org.apache.hop.www;

import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//import org.eclipse.jetty.server.bio.SocketConnector;

/**
 * @author Tatsiana_Kasiankova
 */
public class WebServerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  /**
   *
   */
  private static final String EMPTY_STRING = "";

  private static final boolean SHOULD_JOIN = false;

  private static final String HOST_NAME = "localhost";

  private static final int PORT = 8099;

  private static final String ACCEPTORS = "5";

  private static final int EXPECTED_ACCEPTORS = 5;

  private static final String ACCEPT_QUEUE_SIZE = "5000";

  private static final int EXPECTED_ACCEPT_QUEUE_SIZE = 5000;

  private static final String RES_MAX_IDLE_TIME = "200";

  private static final int EXPECTED_RES_MAX_IDLE_TIME = 200;

  private static final int EXPECTED_CONNECTORS_SIZE = 1;

  private WebServer webServer;
  private WebServer webServerNg;
  private PipelineMap trMapMock = mock( PipelineMap.class );
  private HopServerConfig sServerConfMock = mock( HopServerConfig.class );
  private HopServer sServer = mock( HopServer.class );
  private WorkflowMap jbMapMock = mock( WorkflowMap.class );
  private ILogChannel logMock = mock( ILogChannel.class );
  //  private static final SocketConnector defSocketConnector = new SocketConnector();
  private static ServerConnector defServerConnector;

  @Before
  public void setup() throws Exception {
    System.setProperty( Const.HOP_SERVER_JETTY_ACCEPTORS, ACCEPTORS );
    System.setProperty( Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE );
    System.setProperty( Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME, RES_MAX_IDLE_TIME );

    Server server = new Server();
    defServerConnector = new ServerConnector( server );

    when( sServerConfMock.getHopServer() ).thenReturn( sServer );
    when( trMapMock.getHopServerConfig() ).thenReturn( sServerConfMock );
    when( sServer.getPassword() ).thenReturn( "cluster" );
    when( sServer.getUsername() ).thenReturn( "cluster" );
    webServer = new WebServer( logMock, trMapMock, jbMapMock, HOST_NAME, PORT, SHOULD_JOIN, null );

  }

  @After
  public void tearDown() {
    webServer.setWebServerShutdownHandler( null ); // disable system.exit
    webServer.stopServer();

    System.getProperties().remove( Const.HOP_SERVER_JETTY_ACCEPTORS );
    System.getProperties().remove( Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE );
    System.getProperties().remove( Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME );

  }

  //  @Test
  //  public void testJettyOption_AcceptorsSetUp() throws Exception {
  //    assertEquals( getSocketConnectors( webServer ).size(), EXPECTED_CONNECTORS_SIZE );
  //    for ( ServerConnector sc : getSocketConnectors( webServer ) ) {
  //      assertEquals( EXPECTED_ACCEPTORS, sc.getAcceptors() );
  //
  //    }
  //
  //  }

  @Test
  public void testJettyOption_AcceptQueueSizeSetUp() throws Exception {
    assertEquals( getSocketConnectors( webServer ).size(), EXPECTED_CONNECTORS_SIZE );
    for ( ServerConnector sc : getSocketConnectors( webServer ) ) {
      assertEquals( EXPECTED_ACCEPT_QUEUE_SIZE, sc.getAcceptQueueSize() );
    }

  }

  @Test
  public void testJettyOption_LowResourceMaxIdleTimeSetUp() throws Exception {
    assertEquals( getSocketConnectors( webServer ).size(), EXPECTED_CONNECTORS_SIZE );
    for ( ServerConnector sc : getSocketConnectors( webServer ) ) {
      assertEquals( EXPECTED_RES_MAX_IDLE_TIME, sc.getIdleTimeout() );
    }

  }

  @Test
  public void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsInvalidValue() throws Exception {
    System.setProperty( Const.HOP_SERVER_JETTY_ACCEPTORS, "TEST" );
    try {
      webServerNg =
        new WebServer( logMock, trMapMock, jbMapMock, HOST_NAME, PORT + 1, SHOULD_JOIN, null );
    } catch ( NumberFormatException nmbfExc ) {
      fail( "Should not have thrown any NumberFormatException but it does: " + nmbfExc );
    }
    assertEquals( getSocketConnectors( webServerNg ).size(), EXPECTED_CONNECTORS_SIZE );
    for ( ServerConnector sc : getSocketConnectors( webServerNg ) ) {
      assertEquals( sc.getAcceptors(), sc.getAcceptors() );
    }
    webServerNg.setWebServerShutdownHandler( null ); // disable system.exit
    webServerNg.stopServer();
  }

  @Test
  public void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsEmpty() throws Exception {
    System.setProperty( Const.HOP_SERVER_JETTY_ACCEPTORS, EMPTY_STRING );
    try {
      webServerNg =
        new WebServer( logMock, trMapMock, jbMapMock, HOST_NAME, PORT + 1, SHOULD_JOIN, null );
    } catch ( NumberFormatException nmbfExc ) {
      fail( "Should not have thrown any NumberFormatException but it does: " + nmbfExc );
    }
    assertEquals( getSocketConnectors( webServerNg ).size(), EXPECTED_CONNECTORS_SIZE );
    for ( ServerConnector sc : getSocketConnectors( webServerNg ) ) {
      assertEquals( sc.getAcceptors(), sc.getAcceptors() );
    }
    webServerNg.setWebServerShutdownHandler( null ); // disable system.exit
    webServerNg.stopServer();
  }

  private List<ServerConnector> getSocketConnectors( WebServer wServer ) {
    List<ServerConnector> sConnectors = new ArrayList<ServerConnector>();
    Connector[] connectors = wServer.getServer().getConnectors();
    for ( Connector cn : connectors ) {
      if ( cn instanceof ServerConnector ) {
        sConnectors.add( (ServerConnector) cn );
      }
    }
    return sConnectors;
  }

}
