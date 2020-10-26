/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.compress;

import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CompressionInputStreamTest {

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static final String PROVIDER_NAME = "None";

  public CompressionProviderFactory factory = null;
  public CompressionInputStream inStream = null;

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
    ICompressionProvider provider = factory.getCompressionProviderByName( PROVIDER_NAME );
    ByteArrayInputStream in = createTestInputStream();
    inStream = new DummyCompressionIS( in, provider );
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testCtor() {
    assertNotNull( inStream );
  }

  @Test
  public void getCompressionProvider() {
    ICompressionProvider provider = inStream.getCompressionProvider();
    assertEquals( provider.getName(), PROVIDER_NAME );
  }

  @Test
  public void testNextEntry() throws IOException {
    assertNull( inStream.nextEntry() );
  }

  @Test
  public void testClose() throws IOException {
    ICompressionProvider provider = inStream.getCompressionProvider();
    ByteArrayInputStream in = createTestInputStream();
    inStream = new DummyCompressionIS( in, provider );
    inStream.close();
  }

  @Test
  public void testRead() throws IOException {
    ICompressionProvider provider = inStream.getCompressionProvider();
    ByteArrayInputStream in = createTestInputStream();
    inStream = new DummyCompressionIS( in, provider );
    assertEquals( inStream.available(), inStream.read( new byte[ 100 ], 0, inStream.available() ) );
  }

  @Test
  public void delegatesReadBuffer() throws Exception {
    ByteArrayInputStream in = createTestInputStream();
    in = spy( in );
    inStream = new DummyCompressionIS( in, inStream.getCompressionProvider() );
    inStream.read( new byte[ 16 ] );
    verify( in ).read( any( byte[].class ) );
  }

  @Test
  public void delegatesReadBufferWithParams() throws Exception {
    ByteArrayInputStream in = createTestInputStream();
    in = spy( in );
    inStream = new DummyCompressionIS( in, inStream.getCompressionProvider() );
    inStream.read( new byte[ 16 ], 0, 16 );
    verify( in ).read( any( byte[].class ), anyInt(), anyInt() );
  }


  private static ByteArrayInputStream createTestInputStream() {
    return new ByteArrayInputStream( "Test".getBytes() );
  }

  private static class DummyCompressionIS extends CompressionInputStream {
    public DummyCompressionIS( InputStream in, ICompressionProvider provider ) {
      super( in, provider );
    }
  }
}
