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

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class HopServerTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Ignore
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

    mockStatic(Client.class);
    when(ClientBuilder.newClient(any(ClientConfig.class))).thenReturn(client);

    HopServer.callStopHopServerRestService(
        "localhost", "8080", "admin", "Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde");
  }
}
