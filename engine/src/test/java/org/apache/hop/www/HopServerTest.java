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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.reflect.Whitebox.getInternalState;

/**
 * Created by ccaspanello on 5/31/2016.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( Client.class )
public class HopServerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void callStopHopServerRestService() throws Exception {
    WebResource status = mock( WebResource.class );
    doReturn( "<serverstatus>" ).when( status ).get( String.class );

    WebResource stop = mock( WebResource.class );
    doReturn( "Shutting Down" ).when( stop ).get( String.class );

    Client client = mock( Client.class );
    doCallRealMethod().when( client ).addFilter( any( HTTPBasicAuthFilter.class ) );
    doCallRealMethod().when( client ).getHeadHandler();
    doReturn( status ).when( client ).resource( "http://localhost:8080/hop/status/?xml=Y" );
    doReturn( stop ).when( client ).resource( "http://localhost:8080/hop/stopHopServer" );

    mockStatic( Client.class );
    when( Client.create( any( ClientConfig.class ) ) ).thenReturn( client );

    HopServer.callStopHopServerRestService( "localhost", "8080", "admin", "Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde" );

    // the expected value is: "Basic <base64 encoded username:password>"
    assertEquals( "Basic " + new String( Base64.getEncoder().encode( "admin:password".getBytes( "utf-8" ) ) ),
      getInternalState( client.getHeadHandler(), "authentication" ) );
  }
}
