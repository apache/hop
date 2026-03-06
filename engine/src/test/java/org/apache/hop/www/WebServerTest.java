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
package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.server.HopServerMeta;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class WebServerTest {
  private static final String EMPTY_STRING = "";

  private static final boolean SHOULD_JOIN = false;

  private static final String HOST_NAME = "localhost";

  private static final int PORT = 8099;

  private static final int SHUTDOEN_PORT = 8098;

  private static final String ACCEPTORS = "5";

  private static final String ACCEPT_QUEUE_SIZE = "5000";

  private static final int EXPECTED_ACCEPT_QUEUE_SIZE = 5000;

  private static final String RES_MAX_IDLE_TIME = "200";

  private static final int EXPECTED_RES_MAX_IDLE_TIME = 200;

  private static final int EXPECTED_CONNECTORS_SIZE = 1;

  private WebServer webServer;
  private WebServer webServerNg;
  private final PipelineMap trMapMock = mock(PipelineMap.class);
  private final HopServerConfig sServerConfMock = mock(HopServerConfig.class);
  private final HopServerMeta sServer = mock(HopServerMeta.class);
  private final WorkflowMap jbMapMock = mock(WorkflowMap.class);
  private final ILogChannel logMock = mock(ILogChannel.class);

  @BeforeEach
  void setup() throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, ACCEPTORS);
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE);
    System.setProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME, RES_MAX_IDLE_TIME);

    when(sServerConfMock.getHopServer()).thenReturn(sServer);
    when(trMapMock.getHopServerConfig()).thenReturn(sServerConfMock);
    when(sServer.getPassword()).thenReturn("cluster");
    when(sServer.getUsername()).thenReturn("cluster");
    webServer =
        new WebServer(
            logMock, trMapMock, jbMapMock, HOST_NAME, PORT, SHUTDOEN_PORT, SHOULD_JOIN, null);
  }

  @AfterEach
  void tearDown() {
    webServer.setWebServerShutdownHandler(null); // disable system.exit
    webServer.stopServer();

    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPTORS);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME);
  }

  @Test
  void testJettyOption_AcceptQueueSizeSetUp() {
    assertEquals(EXPECTED_CONNECTORS_SIZE, getSocketConnectors(webServer).size());
    for (ServerConnector sc : getSocketConnectors(webServer)) {
      assertEquals(EXPECTED_ACCEPT_QUEUE_SIZE, sc.getAcceptQueueSize());
    }
  }

  @Test
  void testJettyOption_LowResourceMaxIdleTimeSetUp() {
    assertEquals(EXPECTED_CONNECTORS_SIZE, getSocketConnectors(webServer).size());
    for (ServerConnector sc : getSocketConnectors(webServer)) {
      assertEquals(EXPECTED_RES_MAX_IDLE_TIME, sc.getIdleTimeout());
    }
  }

  @Test
  void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsInvalidValue()
      throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, "TEST");
    try {
      webServerNg =
          new WebServer(
              logMock, trMapMock, jbMapMock, HOST_NAME, PORT + 1, SHUTDOEN_PORT, SHOULD_JOIN, null);
    } catch (NumberFormatException nmbfExc) {
      fail("Should not have thrown any NumberFormatException but it does: " + nmbfExc);
    }
    assertEquals(EXPECTED_CONNECTORS_SIZE, getSocketConnectors(webServerNg).size());
    for (ServerConnector sc : getSocketConnectors(webServerNg)) {
      assertEquals(sc.getAcceptors(), sc.getAcceptors());
    }
    webServerNg.setWebServerShutdownHandler(null); // disable system.exit
    webServerNg.stopServer();
  }

  @Test
  void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsEmpty() throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, EMPTY_STRING);
    try {
      webServerNg =
          new WebServer(
              logMock, trMapMock, jbMapMock, HOST_NAME, PORT + 1, SHUTDOEN_PORT, SHOULD_JOIN, null);
    } catch (NumberFormatException nmbfExc) {
      fail("Should not have thrown any NumberFormatException but it does: " + nmbfExc);
    }
    assertEquals(EXPECTED_CONNECTORS_SIZE, getSocketConnectors(webServerNg).size());
    for (ServerConnector sc : getSocketConnectors(webServerNg)) {
      assertEquals(sc.getAcceptors(), sc.getAcceptors());
    }
    webServerNg.setWebServerShutdownHandler(null); // disable system.exit
    webServerNg.stopServer();
  }

  private List<ServerConnector> getSocketConnectors(WebServer wServer) {
    List<ServerConnector> sConnectors = new ArrayList<>();
    Connector[] connectors = wServer.getServer().getConnectors();
    for (Connector cn : connectors) {
      if (cn instanceof ServerConnector) {
        sConnectors.add((ServerConnector) cn);
      }
    }
    return sConnectors;
  }
}
