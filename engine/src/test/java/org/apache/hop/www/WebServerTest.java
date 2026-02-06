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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.server.HopServerMeta;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class WebServerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  /** */
  private static final String PUBLIC_CONNECTOR_NAME = "webserver";

  private static final String SHUTDOWN_CONNECTOR_NAME = "shutdown";

  private static final String EMPTY_STRING = "";

  private static final String HOST_NAME = "localhost";

  private static final int PORT = 8199;

  private static final int SHUTDOWN_PORT = 8198;

  private static final boolean JOIN = true;

  private static final String ACCEPTORS = "5";

  private static final String ACCEPT_QUEUE_SIZE = "5000";

  private static final int EXPECTED_ACCEPT_QUEUE_SIZE = 5000;

  private static final String RES_MAX_IDLE_TIME = "200";

  private static final int EXPECTED_RES_MAX_IDLE_TIME = 200;

  private static final int EXPECTED_CONNECTORS_SIZE = 2;

  private PipelineMap pipelineMapMock = mock(PipelineMap.class);
  private HopServerConfig serverConfigMock = mock(HopServerConfig.class);
  private HopServerMeta serverMeta = mock(HopServerMeta.class);
  private WorkflowMap workflowMapMock = mock(WorkflowMap.class);
  private ILogChannel logMock = mock(ILogChannel.class);

  @Before
  public void setup() throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, ACCEPTORS);
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE);
    System.setProperty(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME, RES_MAX_IDLE_TIME);

    when(serverConfigMock.getHopServer()).thenReturn(serverMeta);
    when(pipelineMapMock.getHopServerConfig()).thenReturn(serverConfigMock);
    when(serverMeta.getPassword()).thenReturn("cluster");
    when(serverMeta.getUsername()).thenReturn("cluster");
  }

  @After
  public void tearDown() {
    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPTORS);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME);
  }

  @Test
  public void testSocketConnectors() throws Exception {
    WebServer webserver =
        new WebServer(
            logMock, pipelineMapMock, workflowMapMock, HOST_NAME, PORT, SHUTDOWN_PORT, JOIN);
    assertEquals(EXPECTED_CONNECTORS_SIZE, getSocketConnectors(webserver).size());
    webserver.stopServer();
  }

  @Test
  public void testShutdownDisabled() throws Exception {
    WebServer webserver =
        new WebServer(logMock, pipelineMapMock, workflowMapMock, HOST_NAME, PORT, -1, JOIN);
    assertEquals(1, getSocketConnectors(webserver).size());
    webserver.stopServer();
  }

  @Test
  public void testJettyOption() throws Exception {
    WebServer webserver =
        new WebServer(logMock, pipelineMapMock, workflowMapMock, HOST_NAME, PORT, -1, JOIN);

    // AcceptQueueSizeSetUp
    for (ServerConnector connector : getSocketConnectors(webserver, PUBLIC_CONNECTOR_NAME)) {
      assertEquals(EXPECTED_ACCEPT_QUEUE_SIZE, connector.getAcceptQueueSize());
    }
    for (ServerConnector connector : getSocketConnectors(webserver, SHUTDOWN_CONNECTOR_NAME)) {
      assertEquals(1, connector.getAcceptQueueSize());
    }

    // LowResourceMaxIdleTimeSetUp
    for (ServerConnector connector : getSocketConnectors(webserver, PUBLIC_CONNECTOR_NAME)) {
      assertEquals(EXPECTED_RES_MAX_IDLE_TIME, connector.getIdleTimeout());
    }

    webserver.stopServer();
  }

  @Test
  public void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsInvalidValue()
      throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, "TEST");
    WebServer webserver = null;
    try {
      webserver =
          new WebServer(
              logMock, pipelineMapMock, workflowMapMock, HOST_NAME, PORT, SHUTDOWN_PORT, JOIN);
    } catch (NumberFormatException nmbfExc) {
      fail("Should not have thrown any NumberFormatException but it does: " + nmbfExc);
    }
    assertTrue(webserver.getServer().isStarted());
    webserver.stopServer();
  }

  @Test
  public void testNoExceptionAndUsingDefaultServerValue_WhenJettyOptionSetAsEmpty()
      throws Exception {
    System.setProperty(Const.HOP_SERVER_JETTY_ACCEPTORS, EMPTY_STRING);
    WebServer webserver = null;
    try {
      webserver =
          new WebServer(
              logMock, pipelineMapMock, workflowMapMock, HOST_NAME, PORT, SHUTDOWN_PORT, JOIN);
    } catch (NumberFormatException nmbfExc) {
      fail("Should not have thrown any NumberFormatException but it does: " + nmbfExc);
    }
    assertTrue(webserver.getServer().isStarted());
    webserver.stopServer();
  }

  private List<ServerConnector> getSocketConnectors(WebServer webserver, String name) {
    List<ServerConnector> connectors = new ArrayList<>();
    for (Connector connector : webserver.getServer().getConnectors()) {
      if (connector instanceof ServerConnector serverConnector) {
        if (name.equals(serverConnector.getName())) {
          connectors.add(serverConnector);
        }
      }
    }
    return connectors;
  }

  private List<ServerConnector> getSocketConnectors(WebServer webserver) {
    List<ServerConnector> connectors = new ArrayList<>();
    for (Connector connector : webserver.getServer().getConnectors()) {
      if (connector instanceof ServerConnector serverConnector) {
        connectors.add(serverConnector);
      }
    }
    return connectors;
  }
}
