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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.server.HopServerMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class HopServerConfigTest {
  public static final String XML_TAG_HOP_SERVER_CONFIG = "hop-server-config";
  public static final String XML_TAG_JETTY_OPTIONS = "jetty_options";
  public static final String XML_TAG_ACCEPTORS = "acceptors";
  public static final String XML_TAG_ACCEPT_QUEUE_SIZE = "acceptQueueSize";
  public static final String XML_TAG_LOW_RES_MAX_IDLE_TIME = "lowResourcesMaxIdleTime";

  public static final String ACCEPTORS_VALUE = "10";
  public static final String EXPECTED_ACCEPTORS_VALUE = "10";
  public static final String EXPECTED_ACCEPTORS_KEY = Const.HOP_SERVER_JETTY_ACCEPTORS;

  public static final String ACCEPT_QUEUE_SIZE_VALUE = "8000";
  public static final String EXPECTED_ACCEPT_QUEUE_SIZE_VALUE = "8000";
  public static final String EXPECTED_ACCEPT_QUEUE_SIZE_KEY =
      Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE;

  public static final String LOW_RES_MAX_IDLE_TIME_VALUE = "300";
  public static final String EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE = "300";
  public static final String EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY =
      Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME;

  Map<String, String> jettyOptions;
  HopServerConfig slServerConfig;

  @BeforeEach
  void setup() {
    slServerConfig = new HopServerConfig();
  }

  @AfterEach
  void tearDown() {
    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPTORS);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE);
    System.getProperties().remove(Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME);
  }

  @Test
  void testSetUpJettyOptionsAsSystemParameters() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithAllOptions());

    slServerConfig.setUpJettyOptions(configNode);

    assertTrue(
        System.getProperties().containsKey(EXPECTED_ACCEPTORS_KEY),
        "Expected containing jetty option " + EXPECTED_ACCEPTORS_KEY);
    assertEquals(EXPECTED_ACCEPTORS_VALUE, System.getProperty(EXPECTED_ACCEPTORS_KEY));
    assertTrue(
        System.getProperties().containsKey(EXPECTED_ACCEPT_QUEUE_SIZE_KEY),
        "Expected containing jetty option " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY);
    assertEquals(
        EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, System.getProperty(EXPECTED_ACCEPT_QUEUE_SIZE_KEY));
    assertTrue(
        System.getProperties().containsKey(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY),
        "Expected containing jetty option " + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY);
    assertEquals(
        EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE,
        System.getProperty(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY));
  }

  @Test
  void testDoNotSetUpJettyOptionsAsSystemParameters_WhenNoOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithNoOptionsNode());

    slServerConfig.setUpJettyOptions(configNode);

    assertFalse(
        System.getProperties().containsKey(EXPECTED_ACCEPTORS_KEY),
        "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPTORS_KEY);
    assertFalse(
        System.getProperties().containsKey(EXPECTED_ACCEPT_QUEUE_SIZE_KEY),
        "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY);
    assertFalse(
        System.getProperties().containsKey(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY),
        "There should not be any jetty option but it is here:  "
            + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY);
  }

  @Test
  void testDoNotSetUpJettyOptionsAsSystemParameters_WhenEmptyOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithEmptyOptionsNode());

    slServerConfig.setUpJettyOptions(configNode);

    assertFalse(
        System.getProperties().containsKey(EXPECTED_ACCEPTORS_KEY),
        "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPTORS_KEY);
    assertFalse(
        System.getProperties().containsKey(EXPECTED_ACCEPT_QUEUE_SIZE_KEY),
        "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY);
    assertFalse(
        System.getProperties().containsKey(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY),
        "There should not be any jetty option but it is here:  "
            + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY);
  }

  @Test
  void testParseJettyOption_Acceptors() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithAcceptorsOnlyOption());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);

    assertNotNull(parseJettyOptions);
    assertEquals(1, parseJettyOptions.size());
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_ACCEPTORS_KEY),
        "Expected containing key=" + EXPECTED_ACCEPTORS_KEY);
    assertEquals(EXPECTED_ACCEPTORS_VALUE, parseJettyOptions.get(EXPECTED_ACCEPTORS_KEY));
  }

  @Test
  void testParseJettyOption_AcceptQueueSize() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithAcceptQueueSizeOnlyOption());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);

    assertNotNull(parseJettyOptions);
    assertEquals(1, parseJettyOptions.size());
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_ACCEPT_QUEUE_SIZE_KEY),
        "Expected containing key=" + EXPECTED_ACCEPT_QUEUE_SIZE_KEY);
    assertEquals(
        EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, parseJettyOptions.get(EXPECTED_ACCEPT_QUEUE_SIZE_KEY));
  }

  @Test
  void testParseJettyOption_LowResourcesMaxIdleTime() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithLowResourcesMaxIdleTimeeOnlyOption());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);

    assertNotNull(parseJettyOptions);
    assertEquals(1, parseJettyOptions.size());
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY),
        "Expected containing key=" + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY);
    assertEquals(
        EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE,
        parseJettyOptions.get(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY));
  }

  @Test
  void testParseJettyOption_AllOptions() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithAllOptions());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);

    assertNotNull(parseJettyOptions);
    assertEquals(3, parseJettyOptions.size());
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_ACCEPTORS_KEY),
        "Expected containing key=" + EXPECTED_ACCEPTORS_KEY);
    assertEquals(EXPECTED_ACCEPTORS_VALUE, parseJettyOptions.get(EXPECTED_ACCEPTORS_KEY));
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_ACCEPT_QUEUE_SIZE_KEY),
        "Expected containing key=" + EXPECTED_ACCEPT_QUEUE_SIZE_KEY);
    assertEquals(
        EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, parseJettyOptions.get(EXPECTED_ACCEPT_QUEUE_SIZE_KEY));
    assertTrue(
        parseJettyOptions.containsKey(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY),
        "Expected containing key=" + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY);
    assertEquals(
        EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE,
        parseJettyOptions.get(EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY));
  }

  @Test
  void testParseJettyOption_EmptyOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithEmptyOptionsNode());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);

    assertNotNull(parseJettyOptions);
    assertEquals(0, parseJettyOptions.size());
  }

  @Test
  void testParseJettyOption_NoOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode(getConfigWithNoOptionsNode());

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions(configNode);
    assertNull(parseJettyOptions);
  }

  @Test
  void testSetInternalHopServerVariables_populatesAllVariables() {
    HopServerMeta meta =
        new HopServerMeta("my-server", "host.example.com", "8181", "8180", "admin", "secret");
    meta.setWebAppName("hop");
    meta.setSslMode(true);
    slServerConfig.setHopServer(meta);

    IVariables variables = new Variables();
    slServerConfig.setInternalHopServerVariables(variables, 8181);

    assertEquals("my-server", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_NAME));
    assertEquals(
        "host.example.com", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_HOSTNAME));
    assertEquals("8181", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_PORT));
    assertEquals("hop", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_WEB_APP_NAME));
    assertEquals("admin", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_USERNAME));
    assertEquals("true", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_SSL_MODE));
  }

  @Test
  void testSetInternalHopServerVariables_resolvedPortOverridesConfigured() {
    HopServerMeta meta = new HopServerMeta("local8080", "localhost", "8080", "8079", null, null);
    slServerConfig.setHopServer(meta);

    IVariables variables = new Variables();
    slServerConfig.setInternalHopServerVariables(variables, 9090);

    assertEquals("9090", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_PORT));
  }

  @Test
  void testSetInternalHopServerVariables_handlesEmptyOptionalFields() {
    HopServerMeta meta = new HopServerMeta();
    meta.setName("minimal");
    meta.setHostname("localhost");
    slServerConfig.setHopServer(meta);

    IVariables variables = new Variables();
    slServerConfig.setInternalHopServerVariables(variables, 8080);

    assertEquals("minimal", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_NAME));
    assertEquals("localhost", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_HOSTNAME));
    assertEquals("", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_WEB_APP_NAME));
    assertEquals("", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_USERNAME));
    assertEquals("false", variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_SSL_MODE));
  }

  @Test
  void testSetInternalHopServerVariables_nullServerIsNoop() {
    // No HopServerMeta on the config — should not throw and should not set any variable.
    IVariables variables = new Variables();
    slServerConfig.setInternalHopServerVariables(variables, 8080);

    assertNull(variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_NAME));
    assertNull(variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_HOSTNAME));
    assertNull(variables.getVariable(Const.INTERNAL_VARIABLE_HOP_SERVER_PORT));
  }

  @Test
  void testSetInternalHopServerVariables_nullVariablesIsNoop() {
    HopServerMeta meta = new HopServerMeta("server", "host", "8181", "8180", null, null);
    slServerConfig.setHopServer(meta);

    // Should not throw on null variables.
    slServerConfig.setInternalHopServerVariables(null, 8181);
  }

  private Node getConfigNode(String configString) throws HopXmlException {
    Document document = XmlHandler.loadXmlString(configString);
    return XmlHandler.getSubNode(document, HopServerConfig.XML_TAG);
  }

  private String getConfigWithAcceptorsOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put(XML_TAG_ACCEPTORS, ACCEPTORS_VALUE);
    return getConfig(jettyOptions);
  }

  private String getConfigWithAcceptQueueSizeOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put(XML_TAG_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE_VALUE);
    return getConfig(jettyOptions);
  }

  private String getConfigWithLowResourcesMaxIdleTimeeOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put(XML_TAG_LOW_RES_MAX_IDLE_TIME, LOW_RES_MAX_IDLE_TIME_VALUE);
    return getConfig(jettyOptions);
  }

  private String getConfigWithAllOptions() {
    jettyOptions = new HashMap<>();
    jettyOptions.put(XML_TAG_ACCEPTORS, ACCEPTORS_VALUE);
    jettyOptions.put(XML_TAG_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE_VALUE);
    jettyOptions.put(XML_TAG_LOW_RES_MAX_IDLE_TIME, LOW_RES_MAX_IDLE_TIME_VALUE);
    return getConfig(jettyOptions);
  }

  private String getConfigWithEmptyOptionsNode() {
    jettyOptions = new HashMap<>();
    return getConfig(jettyOptions);
  }

  private String getConfigWithNoOptionsNode() {
    return getConfig(jettyOptions);
  }

  private String getConfig(Map<String, String> jettyOptions) {
    StringBuilder xml = new StringBuilder(50);
    xml.append(XmlHandler.getXmlHeader(Const.UTF_8));
    xml.append("<" + XML_TAG_HOP_SERVER_CONFIG + ">").append(Const.CR);
    if (jettyOptions != null) {
      xml.append("<" + XML_TAG_JETTY_OPTIONS + ">").append(Const.CR);
      for (Entry<String, String> jettyOption : jettyOptions.entrySet()) {
        xml.append("<").append(jettyOption.getKey()).append(">").append(jettyOption.getValue());
        xml.append("</").append(jettyOption.getKey()).append(">").append(Const.CR);
      }
      xml.append("</" + XML_TAG_JETTY_OPTIONS + ">").append(Const.CR);
    }
    xml.append("</" + XML_TAG_HOP_SERVER_CONFIG + ">").append(Const.CR);
    return xml.toString();
  }
}
