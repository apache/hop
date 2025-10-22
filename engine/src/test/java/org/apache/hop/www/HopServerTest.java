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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;

public class HopServerTest {
  private MockedStatic<Client> mockedClient;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpStaticMocks() {
    mockedClient = mockStatic(Client.class);
  }

  @After
  public void tearDownStaticMocks() {
    mockedClient.closeOnDemand();
  }

  @Ignore("This test needs to be reviewed")
  @Test
  public void callStopHopServerRestService() throws Exception {
    WebTarget target = mock(WebTarget.class);
    doReturn("<serverstatus>").when(target).request(MediaType.TEXT_PLAIN).get();

    WebTarget stop = mock(WebTarget.class);
    doReturn("Shutting Down").when(stop).request(MediaType.TEXT_PLAIN).get();

    Client client = mock(Client.class);
    doCallRealMethod().when(client).register(any(HttpAuthenticationFeature.class));
    doReturn(target).when(client).target("http://localhost:8080/hop/status/?xml=Y");
    doReturn(stop).when(client).target("http://localhost:8080/hop/stopHopServer");
    when(ClientBuilder.newClient(any(ClientConfig.class))).thenReturn(client);

    HopServer.callStopHopServerRestService(
        "localhost", "8080", "8079", "admin", "Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde");
  }
}
