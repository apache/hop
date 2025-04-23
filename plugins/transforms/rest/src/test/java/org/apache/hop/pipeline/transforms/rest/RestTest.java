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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.glassfish.jersey.client.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedStatic;

class RestTest {

  private MockedStatic<Client> mockedClient;

  @BeforeEach
  void setUpStaticMocks() {
    mockedClient = mockStatic(Client.class);
  }

  @AfterEach
  void tearDownStaticMocks() {
    mockedClient.closeOnDemand();
  }

  @Test
  void testCreateMultivalueMap() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);
    Rest rest =
        new Rest(
            transformMeta,
            mock(RestMeta.class),
            mock(RestData.class),
            1,
            pipelineMeta,
            spy(new LocalPipelineEngine()));
    MultivaluedHashMap map = rest.createMultivalueMap("param1", "{a:{[val1]}}");
    String val1 = map.getFirst("param1").toString();
    assertTrue(val1.contains("%7D"));
  }

  @Disabled("This test needs to be reviewed")
  @Test
  void testCallEndpointWithDeleteVerb() throws HopException {
    MultivaluedMap<String, String> headers = null;
    headers.add("Content-Type", "application/json");

    Response response = mock(Response.class);
    doReturn(200).when(response).getStatus();
    doReturn(headers).when(response).getHeaders();
    doReturn("true").when(response).getEntity().toString();

    Invocation.Builder builder = mock(Invocation.Builder.class);
    doReturn(response).when(builder).delete(ClientResponse.class);

    WebTarget resource = mock(WebTarget.class);

    Client client = mock(Client.class);
    doReturn(resource).when(client).target(nullable(String.class));

    RestMeta meta = mock(RestMeta.class);
    doReturn(false).when(meta).isDetailed();
    doReturn(false).when(meta).isUrlInField();
    doReturn(false).when(meta).isDynamicMethod();

    IRowMeta rmi = mock(IRowMeta.class);
    doReturn(1).when(rmi).size();

    RestData data = mock(RestData.class);
    data.method = RestMeta.HTTP_METHOD_DELETE;
    data.inputRowMeta = rmi;
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";
    data.resultHeaderFieldName = "headers";

    Rest rest = mock(Rest.class, Answers.RETURNS_DEFAULTS);
    doCallRealMethod().when(rest).callRest(any());
    doCallRealMethod().when(rest).searchForHeaders(any());

    Object[] output = rest.callRest(new Object[] {0});

    verify(builder, times(1)).delete(ClientResponse.class);
    assertEquals("true", output[1]);
    assertEquals(200L, output[2]);
    assertEquals("{\"Content-Type\":\"application\\/json\"}", output[3]);
  }
}
