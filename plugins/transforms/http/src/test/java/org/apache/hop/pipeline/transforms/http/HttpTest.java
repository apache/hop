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

package org.apache.hop.pipeline.transforms.http;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.reflect.Whitebox.setInternalState;

/**
 * @author Luis Martins
 * @since 14-Aug-2018
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( HttpClientManager.class )
public class HttpTest {

  private ILogChannel log = mock( ILogChannel.class );
  private IRowMeta rmi = mock( IRowMeta.class );
  private HttpData data = mock( HttpData.class );
  private HttpMeta meta = mock( HttpMeta.class );
  private Http http = mock( Http.class );

  private final String DATA = "This is the description, there's some HTML here, like &lt;strong&gt;this&lt;/strong&gt;. "
    + "Sometimes this text is another language that might contain these characters:\n"
    + "&lt;p&gt;é, è, ô, ç, à, ê, â.&lt;/p&gt; They can, of course, come in uppercase as well: &lt;p&gt;É, È Ô, Ç, À,"
    + " Ê, Â&lt;/p&gt;. UTF-8 handles this well.";

  @Before
  public void setup() throws Exception {
    HttpClientManager.HttpClientBuilderFacade builder = mock( HttpClientManager.HttpClientBuilderFacade.class );

    HttpClientManager manager = mock( HttpClientManager.class );
    doReturn( builder ).when( manager ).createBuilder();

    CloseableHttpClient client = mock( CloseableHttpClient.class );
    doReturn( client ).when( builder ).build();

    CloseableHttpResponse response = mock( CloseableHttpResponse.class );
    doReturn( response ).when( client ).execute( any( HttpGet.class ) );

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent( new ByteArrayInputStream( DATA.getBytes() ) );
    doReturn( entity ).when( response ).getEntity();

    mockStatic( HttpClientManager.class );
    when( HttpClientManager.getInstance() ).thenReturn( manager );

    setInternalState( data, "realUrl", "http://project-hop.org" );
    setInternalState( data, "argnrs", new int[ 0 ] );

    doReturn( false ).when( meta ).isUrlInField();
    doReturn( "body" ).when( meta ).getFieldName();

    doReturn( false ).when( log ).isDetailed();

    doCallRealMethod().when( http ).callHttpService( any( IRowMeta.class ), any( Object[].class ) );
    doReturn( HttpURLConnection.HTTP_OK ).when( http ).requestStatusCode( any( CloseableHttpResponse.class ) );
    doReturn( new Header[ 0 ] ).when( http ).searchForHeaders( any( CloseableHttpResponse.class ) );
    setInternalState( http, "log", log );
    setInternalState( http, "data", data );
    setInternalState( http, "meta", meta );
  }

  @Test
  public void callHttpServiceWithUTF8Encoding() throws Exception {
    doReturn( "UTF-8" ).when( meta ).getEncoding();
    assertEquals( DATA, http.callHttpService( rmi, new Object[] { 0 } )[ 0 ] );
  }

  @Test
  public void callHttpServiceWithoutEncoding() throws Exception {
    doReturn( null ).when( meta ).getEncoding();
    assertNotEquals( DATA, http.callHttpService( rmi, new Object[] { 0 } )[ 0 ] );
  }
}
