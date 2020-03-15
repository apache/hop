/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.cluster;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.utils.TestUtils;
import org.apache.hop.www.GetPropertiesServlet;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for SlaveServer class
 *
 * @author Pavel Sakun
 * @see SlaveServer
 */
public class SlaveServerTest {
  SlaveServer slaveServer;

  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @AfterClass
  public static void tearDown() {
    PluginRegistry.getInstance().reset();
  }

  @Before
  public void init() throws IOException {
    SlaveConnectionManager connectionManager = SlaveConnectionManager.getInstance();
    HttpClient httpClient = spy( connectionManager.createHttpClient() );

    // mock response
    CloseableHttpResponse closeableHttpResponseMock = mock( CloseableHttpResponse.class );

    // mock status line
    StatusLine statusLineMock = mock( StatusLine.class );
    doReturn( HttpStatus.SC_NOT_FOUND ).when( statusLineMock ).getStatusCode();
    doReturn( statusLineMock ).when( closeableHttpResponseMock ).getStatusLine();

    // mock entity
    HttpEntity httpEntityMock = mock( HttpEntity.class );
    doReturn( httpEntityMock ).when( closeableHttpResponseMock ).getEntity();

    doReturn( closeableHttpResponseMock ).when( httpClient ).execute( any( HttpGet.class ) );
    doReturn( closeableHttpResponseMock ).when( httpClient ).execute( any( HttpPost.class ) );
    doReturn( closeableHttpResponseMock ).when( httpClient ).execute( any( HttpPost.class ), any( HttpClientContext.class ) );

    slaveServer = spy( new SlaveServer() );
    doReturn( httpClient ).when( slaveServer ).getHttpClient();
    doReturn( "response_body" ).when( slaveServer ).getResponseBodyAsString( any( InputStream.class ) );
  }

  private HttpResponse mockResponse( int statusCode, String entityText ) throws IOException {
    HttpResponse resp = mock( HttpResponse.class );
    StatusLine status = mock( StatusLine.class );
    when( status.getStatusCode() ).thenReturn( statusCode );
    when( resp.getStatusLine() ).thenReturn( status );
    HttpEntity entity = mock( HttpEntity.class );
    when( entity.getContent() ).thenReturn( new ByteArrayInputStream(entityText.getBytes(StandardCharsets.UTF_8)) );
    when( resp.getEntity() ).thenReturn( entity );
    return resp;
  }
  
  

  @Test( expected = HopException.class )
  public void testExecService() throws Exception {
    HttpGet httpGetMock = mock( HttpGet.class );
    URI uriMock = new URI( "fake" );
    doReturn( uriMock ).when( httpGetMock ).getURI();
    doReturn( httpGetMock ).when( slaveServer ).buildExecuteServiceMethod( anyString(), anyMapOf( String.class,
      String.class ) );
    slaveServer.setHostname( "hostNameStub" );
    slaveServer.setUsername( "userNAmeStub" );
    slaveServer.execService( "wrong_app_name" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test( expected = HopException.class )
  public void testSendXML() throws Exception {
    slaveServer.setHostname( "hostNameStub" );
    slaveServer.setUsername( "userNAmeStub" );
    HttpPost httpPostMock = mock( HttpPost.class );
    URI uriMock = new URI( "fake" );
    doReturn( uriMock ).when( httpPostMock ).getURI();
    doReturn( httpPostMock ).when( slaveServer ).buildSendXMLMethod( any( byte[].class ), anyString() );
    slaveServer.sendXML( "", "" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test( expected = HopException.class )
  public void testSendExport() throws Exception {
    slaveServer.setHostname( "hostNameStub" );
    slaveServer.setUsername( "userNAmeStub" );
    HttpPost httpPostMock = mock( HttpPost.class );
    URI uriMock = new URI( "fake" );
    doReturn( uriMock ).when( httpPostMock ).getURI();
    doReturn( httpPostMock ).when( slaveServer ).buildSendExportMethod( anyString(), anyString(), any(
      InputStream.class ) );
    File tempFile;
    tempFile = File.createTempFile( "PDI-", "tmp" );
    tempFile.deleteOnExit();
    slaveServer.sendExport( tempFile.getAbsolutePath(), "", "" );
    fail( "Incorrect connection details had been used, but no exception was thrown" );
  }

  @Test
  public void testSendExportOk() throws Exception {
    slaveServer.setUsername( "uname" );
    slaveServer.setPassword( "passw" );
    slaveServer.setHostname( "hname" );
    slaveServer.setPort( "1111" );
    HttpPost httpPostMock = mock( HttpPost.class );
    URI uriMock = new URI( "fake" );
    final String responseContent = "baah";
    when( httpPostMock.getURI() ).thenReturn( uriMock );
    doReturn( uriMock ).when( httpPostMock ).getURI();

    HttpClient client = mock( HttpClient.class );
    when( client.execute( any(), any( HttpContext.class ) ) ).then( new Answer<HttpResponse>() {
      @Override
      public HttpResponse answer( InvocationOnMock invocation ) throws Throwable {
        HttpClientContext context = (HttpClientContext) invocation.getArguments()[ 1 ];
        Credentials cred = context.getCredentialsProvider().getCredentials( new AuthScope( "hname", 1111 ) );
        assertEquals( "uname", cred.getUserPrincipal().getName() );
        return mockResponse( 200, responseContent );
      }
    } );
    // override init
    when( slaveServer.getHttpClient() ).thenReturn( client );
    when( slaveServer.getResponseBodyAsString( any() ) ).thenCallRealMethod();

    doReturn( httpPostMock ).when( slaveServer ).buildSendExportMethod( anyString(), anyString(), any(
      InputStream.class ) );
    File tempFile;
    tempFile = File.createTempFile( "PDI-", "tmp" );
    tempFile.deleteOnExit();
    String result = slaveServer.sendExport( tempFile.getAbsolutePath(), null, null );
    assertEquals( responseContent, result );
  }

  @Test
  public void testAddCredentials() throws IOException, ClassNotFoundException {
    String testUser = "test_username";
    slaveServer.setUsername( testUser );
    String testPassword = "test_password";
    slaveServer.setPassword( testPassword );
    String host = "somehost";
    slaveServer.setHostname( host );
    int port = 1000;
    slaveServer.setPort( "" + port );

    HttpClientContext auth = slaveServer.getAuthContext();
    Credentials cred = auth.getCredentialsProvider().getCredentials( new AuthScope( host, port ) );
    assertEquals( testUser, cred.getUserPrincipal().getName() );
    assertEquals( testPassword, cred.getPassword() );

    String user2 = "user2";
    slaveServer.setUsername( user2 );
    slaveServer.setPassword( "pass2" );
    auth = slaveServer.getAuthContext();
    cred = auth.getCredentialsProvider().getCredentials( new AuthScope( host, port ) );
    assertEquals( user2, cred.getUserPrincipal().getName() );
  }

  @Test
  public void testAuthCredentialsSchemeWithSSL() {
    slaveServer.setUsername( "admin" );
    slaveServer.setPassword( "password" );
    slaveServer.setHostname( "localhost" );
    slaveServer.setPort( "8443" );
    slaveServer.setSslMode( true );

    AuthCache cache = slaveServer.getAuthContext().getAuthCache();
    assertNotNull( cache.get( new HttpHost( "localhost", 8443, "https" ) ) );
    assertNull( cache.get( new HttpHost( "localhost", 8443, "http" ) ) );
  }

  @Test
  public void testAuthCredentialsSchemeWithoutSSL() {
    slaveServer.setUsername( "admin" );
    slaveServer.setPassword( "password" );
    slaveServer.setHostname( "localhost" );
    slaveServer.setPort( "8080" );
    slaveServer.setSslMode( false );

    AuthCache cache = slaveServer.getAuthContext().getAuthCache();
    assertNull( cache.get( new HttpHost( "localhost", 8080, "https" ) ) );
    assertNotNull( cache.get( new HttpHost( "localhost", 8080, "http" ) ) );
  }

  @Test
  public void testModifyingName() {
    slaveServer.setName( "test" );
    List<SlaveServer> list = new ArrayList<SlaveServer>();
    list.add( slaveServer );

    SlaveServer slaveServer2 = spy( new SlaveServer() );
    slaveServer2.setName( "test" );

    slaveServer2.verifyAndModifySlaveServerName( list, null );

    assertTrue( !slaveServer.getName().equals( slaveServer2.getName() ) );
  }

  @Test
  public void testEqualsHashCodeConsistency() throws Exception {
    SlaveServer slave = new SlaveServer();
    slave.setName( "slave" );
    TestUtils.checkEqualsHashCodeConsistency( slave, slave );

    SlaveServer slaveSame = new SlaveServer();
    slaveSame.setName( "slave" );
    assertTrue( slave.equals( slaveSame ) );
    TestUtils.checkEqualsHashCodeConsistency( slave, slaveSame );

    SlaveServer slaveCaps = new SlaveServer();
    slaveCaps.setName( "SLAVE" );
    TestUtils.checkEqualsHashCodeConsistency( slave, slaveCaps );

    SlaveServer slaveOther = new SlaveServer();
    slaveOther.setName( "something else" );
    TestUtils.checkEqualsHashCodeConsistency( slave, slaveOther );
  }

  @Test
  public void testGetHopProperties() throws Exception {
    String encryptedResponse = "3c3f786d6c2076657273696f6e3d22312e302220656e636f64696e6"
      + "73d225554462d38223f3e0a3c21444f43545950452070726f706572"
      + "746965730a202053595354454d2022687474703a2f2f6a6176612e737"
      + "56e2e636f6d2f6474642f70726f706572746965732e647464223e0a3c"
      + "70726f706572746965733e0a2020203c636f6d6d656e743e3c2f636f6d6d6"
      + "56e743e0a2020203c656e747279206b65793d224167696c6542494461746162"
      + "617365223e4167696c6542493c2f656e7470c7a6a5f445d7808bbb1cbc64d797bc84";
    doReturn( encryptedResponse ).when( slaveServer ).execService( GetPropertiesServlet.CONTEXT_PATH + "/?xml=Y" );
    slaveServer.getHopProperties().getProperty( "AgileBIDatabase" );
    assertEquals( "AgileBI", slaveServer.getHopProperties().getProperty( "AgileBIDatabase" ) );

  }

}
