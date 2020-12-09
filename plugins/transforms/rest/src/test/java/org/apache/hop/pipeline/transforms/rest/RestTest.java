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

package org.apache.hop.pipeline.transforms.rest;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.MultivaluedMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.util.reflection.Whitebox.setInternalState;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith( PowerMockRunner.class )
@PrepareForTest( ApacheHttpClient4.class )
public class RestTest {

  @Test
  public void testCreateMultivalueMap() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName( "TestRest" );
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "TestRest" );
    pipelineMeta.addTransform( transformMeta );
    Rest rest = new Rest( transformMeta, mock(RestMeta.class),  mock( RestData.class ),
      1, pipelineMeta, spy( new LocalPipelineEngine() ) );
    MultivaluedMapImpl map = rest.createMultivalueMap( "param1", "{a:{[val1]}}" );
    String val1 = map.getFirst( "param1" );
    assertTrue( val1.contains( "%7D" ) );
  }

  @Test
  public void testCallEndpointWithDeleteVerb() throws HopException {
    MultivaluedMap<String, String> headers = new MultivaluedMapImpl();
    headers.add( "Content-Type", "application/json" );

    ClientResponse response = mock( ClientResponse.class );
    doReturn( 200 ).when( response ).getStatus();
    doReturn( headers ).when( response ).getHeaders();
    doReturn( "true" ).when( response ).getEntity( String.class );

    WebResource.Builder builder = mock( WebResource.Builder.class );
    doReturn( response ).when( builder ).delete( ClientResponse.class );

    WebResource resource = mock( WebResource.class );
    doReturn( builder ).when( resource ).getRequestBuilder();

    ApacheHttpClient4 client = mock( ApacheHttpClient4.class );
    doReturn( resource ).when( client ).resource( anyString() );

    mockStatic( ApacheHttpClient4.class );
    when( ApacheHttpClient4.create( any() ) ).thenReturn( client );

    RestMeta meta = mock( RestMeta.class );
    doReturn( false ).when( meta ).isDetailed();
    doReturn( false ).when( meta ).isUrlInField();
    doReturn( false ).when( meta ).isDynamicMethod();

    IRowMeta rmi = mock( IRowMeta.class );
    doReturn( 1 ).when( rmi ).size();

    RestData data = mock( RestData.class );
    data.method = RestMeta.HTTP_METHOD_DELETE;
    data.inputRowMeta = rmi;
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";
    data.resultHeaderFieldName = "headers";

    Rest rest = mock( Rest.class );
    doCallRealMethod().when( rest ).callRest( any() );
    doCallRealMethod().when( rest ).searchForHeaders( any() );

    setInternalState( rest, "meta", meta );
    setInternalState( rest, "data", data );

    Object[] output = rest.callRest( new Object[] { 0 } );

    verify( builder, times( 1 ) ).delete( ClientResponse.class );
    assertEquals( "true", output[ 1 ] );
    assertEquals( 200L, output[ 2 ] );
    assertEquals( "{\"Content-Type\":\"application\\/json\"}", output[ 3 ] );
  }
}
