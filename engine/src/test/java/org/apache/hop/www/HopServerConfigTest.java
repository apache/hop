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

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Tatsiana_Kasiankova
 */
public class HopServerConfigTest {

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
  public static final String EXPECTED_ACCEPT_QUEUE_SIZE_KEY = Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE;

  public static final String LOW_RES_MAX_IDLE_TIME_VALUE = "300";
  public static final String EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE = "300";
  public static final String EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY = Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME;

  Map<String, String> jettyOptions;
  HopServerConfig slServerConfig;

  @Before
  public void setup() throws Exception {
    slServerConfig = new HopServerConfig();
  }

  @After
  public void tearDown() {
    System.getProperties().remove( Const.HOP_SERVER_JETTY_ACCEPTORS );
    System.getProperties().remove( Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE );
    System.getProperties().remove( Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME );
  }

  @Test
  public void testSetUpJettyOptionsAsSystemParameters() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithAllOptions() );

    slServerConfig.setUpJettyOptions( configNode );

    assertTrue( "Expected containing jetty option " + EXPECTED_ACCEPTORS_KEY, System.getProperties().containsKey(
      EXPECTED_ACCEPTORS_KEY ) );
    assertEquals( EXPECTED_ACCEPTORS_VALUE, System.getProperty( EXPECTED_ACCEPTORS_KEY ) );
    assertTrue( "Expected containing jetty option " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY, System.getProperties()
      .containsKey( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertEquals( EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, System.getProperty( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertTrue( "Expected containing jetty option " + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY, System.getProperties()
      .containsKey( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
    assertEquals( EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE, System.getProperty( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
  }

  @Test
  public void testDoNotSetUpJettyOptionsAsSystemParameters_WhenNoOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithNoOptionsNode() );

    slServerConfig.setUpJettyOptions( configNode );

    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPTORS_KEY, System
      .getProperties().containsKey( EXPECTED_ACCEPTORS_KEY ) );
    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY, System
      .getProperties().containsKey( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY, System
      .getProperties().containsKey( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
  }

  @Test
  public void testDoNotSetUpJettyOptionsAsSystemParameters_WhenEmptyOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithEmptyOptionsNode() );

    slServerConfig.setUpJettyOptions( configNode );

    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPTORS_KEY, System
      .getProperties().containsKey( EXPECTED_ACCEPTORS_KEY ) );
    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_ACCEPT_QUEUE_SIZE_KEY, System
      .getProperties().containsKey( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertFalse( "There should not be any jetty option but it is here:  " + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY, System
      .getProperties().containsKey( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
  }

  @Test
  public void testParseJettyOption_Acceptors() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithAcceptorsOnlyOption() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );

    assertNotNull( parseJettyOptions );
    assertEquals( 1, parseJettyOptions.size() );
    assertTrue( "Expected containing key=" + EXPECTED_ACCEPTORS_KEY, parseJettyOptions
      .containsKey( EXPECTED_ACCEPTORS_KEY ) );
    assertEquals( EXPECTED_ACCEPTORS_VALUE, parseJettyOptions.get( EXPECTED_ACCEPTORS_KEY ) );
  }

  @Test
  public void testParseJettyOption_AcceptQueueSize() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithAcceptQueueSizeOnlyOption() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );

    assertNotNull( parseJettyOptions );
    assertEquals( 1, parseJettyOptions.size() );
    assertTrue( "Expected containing key=" + EXPECTED_ACCEPT_QUEUE_SIZE_KEY, parseJettyOptions
      .containsKey( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertEquals( EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, parseJettyOptions.get( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
  }

  @Test
  public void testParseJettyOption_LowResourcesMaxIdleTime() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithLowResourcesMaxIdleTimeeOnlyOption() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );

    assertNotNull( parseJettyOptions );
    assertEquals( 1, parseJettyOptions.size() );
    assertTrue( "Expected containing key=" + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY, parseJettyOptions
      .containsKey( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
    assertEquals( EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE, parseJettyOptions.get( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
  }

  @Test
  public void testParseJettyOption_AllOptions() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithAllOptions() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );

    assertNotNull( parseJettyOptions );
    assertEquals( 3, parseJettyOptions.size() );
    assertTrue( "Expected containing key=" + EXPECTED_ACCEPTORS_KEY, parseJettyOptions
      .containsKey( EXPECTED_ACCEPTORS_KEY ) );
    assertEquals( EXPECTED_ACCEPTORS_VALUE, parseJettyOptions.get( EXPECTED_ACCEPTORS_KEY ) );
    assertTrue( "Expected containing key=" + EXPECTED_ACCEPT_QUEUE_SIZE_KEY, parseJettyOptions
      .containsKey( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertEquals( EXPECTED_ACCEPT_QUEUE_SIZE_VALUE, parseJettyOptions.get( EXPECTED_ACCEPT_QUEUE_SIZE_KEY ) );
    assertTrue( "Expected containing key=" + EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY, parseJettyOptions
      .containsKey( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
    assertEquals( EXPECTED_LOW_RES_MAX_IDLE_TIME_VALUE, parseJettyOptions.get( EXPECTED_LOW_RES_MAX_IDLE_TIME_KEY ) );
  }

  @Test
  public void testParseJettyOption_EmptyOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithEmptyOptionsNode() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );

    assertNotNull( parseJettyOptions );
    assertEquals( 0, parseJettyOptions.size() );
  }

  @Test
  public void testParseJettyOption_NoOptionsNode() throws HopXmlException {
    Node configNode = getConfigNode( getConfigWithNoOptionsNode() );

    Map<String, String> parseJettyOptions = slServerConfig.parseJettyOptions( configNode );
    assertNull( parseJettyOptions );
  }

  private Node getConfigNode( String configString ) throws HopXmlException {
    Document document = XmlHandler.loadXmlString( configString );
    Node configNode = XmlHandler.getSubNode( document, HopServerConfig.XML_TAG );
    return configNode;
  }

  private String getConfigWithAcceptorsOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put( XML_TAG_ACCEPTORS, ACCEPTORS_VALUE );
    return getConfig( jettyOptions );
  }

  private String getConfigWithAcceptQueueSizeOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put( XML_TAG_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE_VALUE );
    return getConfig( jettyOptions );
  }

  private String getConfigWithLowResourcesMaxIdleTimeeOnlyOption() {
    jettyOptions = new HashMap<>();
    jettyOptions.put( XML_TAG_LOW_RES_MAX_IDLE_TIME, LOW_RES_MAX_IDLE_TIME_VALUE );
    return getConfig( jettyOptions );
  }

  private String getConfigWithAllOptions() {
    jettyOptions = new HashMap<>();
    jettyOptions.put( XML_TAG_ACCEPTORS, ACCEPTORS_VALUE );
    jettyOptions.put( XML_TAG_ACCEPT_QUEUE_SIZE, ACCEPT_QUEUE_SIZE_VALUE );
    jettyOptions.put( XML_TAG_LOW_RES_MAX_IDLE_TIME, LOW_RES_MAX_IDLE_TIME_VALUE );
    return getConfig( jettyOptions );
  }

  private String getConfigWithEmptyOptionsNode() {
    jettyOptions = new HashMap<>();
    return getConfig( jettyOptions );
  }

  private String getConfigWithNoOptionsNode() {
    return getConfig( jettyOptions );
  }

  private String getConfig( Map<String, String> jettyOptions ) {
    StringBuilder xml = new StringBuilder( 50 );
    xml.append( XmlHandler.getXmlHeader( Const.XML_ENCODING ) );
    xml.append( "<" + XML_TAG_HOP_SERVER_CONFIG + ">" ).append( Const.CR );
    if ( jettyOptions != null ) {
      xml.append( "<" + XML_TAG_JETTY_OPTIONS + ">" ).append( Const.CR );
      for ( Entry<String, String> jettyOption : jettyOptions.entrySet() ) {
        xml.append( "<" + jettyOption.getKey() + ">" ).append( jettyOption.getValue() );
        xml.append( "</" + jettyOption.getKey() + ">" ).append( Const.CR );
      }
      xml.append( "</" + XML_TAG_JETTY_OPTIONS + ">" ).append( Const.CR );
    }
    xml.append( "</" + XML_TAG_HOP_SERVER_CONFIG + ">" ).append( Const.CR );
    return xml.toString();
  }

}
