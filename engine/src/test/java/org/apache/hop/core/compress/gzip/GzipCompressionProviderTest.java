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

package org.apache.hop.core.compress.gzip;

import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GzipCompressionProviderTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static final String PROVIDER_NAME = "GZip";

  public CompressionProviderFactory factory = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( CompressionPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    factory = CompressionProviderFactory.getInstance();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testCtor() {
    GzipCompressionProvider ncp = new GzipCompressionProvider();
    assertNotNull( ncp );
  }

  @Test
  public void testGetName() {
    GzipCompressionProvider provider = (GzipCompressionProvider) factory.getCompressionProviderByName( PROVIDER_NAME );
    assertNotNull( provider );
    assertEquals( PROVIDER_NAME, provider.getName() );
  }

  @Test
  public void testGetProviderAttributes() {
    GzipCompressionProvider provider = (GzipCompressionProvider) factory.getCompressionProviderByName( PROVIDER_NAME );
    assertEquals( "GZIP compression", provider.getDescription() );
    assertTrue( provider.supportsInput() );
    assertTrue( provider.supportsOutput() );
    assertEquals( "gz", provider.getDefaultExtension() );
  }

  @Test
  public void testCreateInputStream() throws IOException {
    GzipCompressionProvider provider = (GzipCompressionProvider) factory.getCompressionProviderByName( PROVIDER_NAME );

    // Create an in-memory GZIP output stream for use by the input stream (to avoid exceptions)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream( baos );
    byte[] testBytes = "Test".getBytes();
    gos.write( testBytes );
    ByteArrayInputStream in = new ByteArrayInputStream( baos.toByteArray() );

    // Test stream creation paths
    GZIPInputStream gis = new GZIPInputStream( in );
    in = new ByteArrayInputStream( baos.toByteArray() );
    GzipCompressionInputStream ncis = provider.createInputStream( in );
    assertNotNull( ncis );
    GzipCompressionInputStream ncis2 = provider.createInputStream( gis );
    assertNotNull( ncis2 );
  }

  @Test
  public void testCreateOutputStream() throws IOException {
    GzipCompressionProvider provider = (GzipCompressionProvider) factory.getCompressionProviderByName( PROVIDER_NAME );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream( out );
    GzipCompressionOutputStream outStream = new GzipCompressionOutputStream( out, provider );
    assertNotNull( outStream );
    out = new ByteArrayOutputStream();
    GzipCompressionOutputStream ncis = provider.createOutputStream( out );
    assertNotNull( ncis );
    GzipCompressionOutputStream ncis2 = provider.createOutputStream( gos );
    assertNotNull( ncis2 );
  }
}
