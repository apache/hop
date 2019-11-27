/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.apache.hop.core.logging.LogChannelInterface;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BaseHopServerPluginTest {

  HttpServletRequest req = mock( HttpServletRequest.class );
  HttpServletResponse resp = mock( HttpServletResponse.class );
  LogChannelInterface log = mock( LogChannelInterface.class );
  HopServerRequestHandler.WriterResponse writerResponse = mock( HopServerRequestHandler.WriterResponse.class );
  HopServerRequestHandler.OutputStreamResponse outputStreamResponse = mock( HopServerRequestHandler.OutputStreamResponse.class );
  PrintWriter printWriter = mock( PrintWriter.class );
  javax.servlet.ServletOutputStream outputStream = mock( javax.servlet.ServletOutputStream.class );

  ArgumentCaptor<HopServerRequestHandler.CarteRequest> carteReqCaptor = ArgumentCaptor.forClass( HopServerRequestHandler.CarteRequest.class );

  BaseHopServerPlugin baseHopServerPlugin;

  @Before
  public void before() {
    baseHopServerPlugin = spy(new BaseHopServerPlugin() {
      @Override
      public void handleRequest( CarteRequest request ) throws IOException {
      }

      @Override
      public String getContextPath() {
        return null;
      }
    } );
    baseHopServerPlugin.log = log;
  }

  @Test
  @SuppressWarnings( "deprecation" )
  public void testDoGet() throws Exception {
    baseHopServerPlugin.doGet( req, resp );
    // doGet should delegate to .service
    verify(baseHopServerPlugin).service( req, resp );
  }

  @Test
  public void testService() throws Exception {
    when( req.getContextPath() ).thenReturn( "/Path" );
    when( baseHopServerPlugin.getContextPath() ).thenReturn( "/Path" );
    when( log.isDebug() ).thenReturn( true );

    baseHopServerPlugin.service( req, resp );

    verify( log ).logDebug( baseHopServerPlugin.getService() );
    verify(baseHopServerPlugin).handleRequest( carteReqCaptor.capture() );

    HopServerRequestHandler.CarteRequest carteRequest = carteReqCaptor.getValue();

    testCarteRequest( carteRequest );
    testCarteResponse( carteRequest.respond( 200 ) );
  }

  private void testCarteResponse( HopServerRequestHandler.CarteResponse response ) throws IOException {
    when( resp.getWriter() ).thenReturn( printWriter );
    when( resp.getOutputStream() ).thenReturn( outputStream );

    response.with( "text/xml", writerResponse );

    verify( resp ).setContentType( "text/xml" );
    verify( writerResponse ).write( printWriter );

    response.with( "text/sgml", outputStreamResponse );

    verify( resp ).setContentType( "text/sgml" );
    verify( outputStreamResponse ).write( outputStream );

    response.withMessage( "Message" );
    verify( resp ).setContentType( "text/plain" );
    verify( printWriter ).println( "Message" );
  }

  private void testCarteRequest( HopServerRequestHandler.CarteRequest carteRequest ) {
    when( req.getMethod() ).thenReturn( "POST" );
    when( req.getHeader( "Connection" ) ).thenReturn( "Keep-Alive" );
    when( req.getParameter( "param1" ) ).thenReturn( "val1" );
    when( req.getParameterNames() ).thenReturn( Collections.enumeration(
      Arrays.asList( "name1", "name2" ) ) );
    when( req.getParameterValues( any( String.class ) ) )
      .thenReturn( new String[] { "val" } );
    when( req.getHeaderNames() ).thenReturn( Collections.enumeration(
      Arrays.asList( "name1", "name2" ) ) );
    when( req.getHeaders( "name1" ) ).thenReturn(
      Collections.enumeration( ImmutableList.of( "val" ) ) );
    when( req.getHeaders( "name2" ) ).thenReturn(
      Collections.enumeration( ImmutableList.of( "val" ) ) );

    assertThat( carteRequest.getMethod(), is( "POST" ) );
    assertThat( carteRequest.getHeader( "Connection" ), is( "Keep-Alive" ) );
    assertThat( carteRequest.getParameter( "param1" ), is( "val1" ) );

    checkMappedVals( carteRequest.getParameters() );
    checkMappedVals( carteRequest.getHeaders() );
  }

  private void checkMappedVals( Map<String, Collection<String>> map ) {
    assertThat( map.size(), is( 2 ) );
    Collection<String> name1Params = map.get( "name1" );
    Collection<String> name2Params = map.get( "name2" );
    assertThat( name1Params.contains( "val" ), is( true ) );
    assertThat( name2Params.contains( "val" ), is( true ) );
    assertThat( name1Params.size() == 1 && name2Params.size() == 1, is( true ) );
  }

  @Test
  public void testGetService() throws Exception {
    when( baseHopServerPlugin.getContextPath() )
      .thenReturn( "/Path" );
    assertThat( baseHopServerPlugin.getService().startsWith( "/Path" ), is( true ) );
  }
}
