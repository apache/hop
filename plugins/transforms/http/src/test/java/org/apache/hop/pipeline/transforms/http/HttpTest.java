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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class HttpTest {

  private static MockedStatic<HttpClientManager> mockedHttpClientManager;

  private final ILogChannel log = mock(ILogChannel.class);
  private final IRowMeta rmi = mock(IRowMeta.class);
  private final HttpData data = mock(HttpData.class);
  private final HttpMeta meta = mock(HttpMeta.class);
  private final Http http = mock(Http.class);

  private final String DATA =
      "This is the description, there's some HTML here, like &lt;strong&gt;this&lt;/strong&gt;. "
          + "Sometimes this text is another language that might contain these characters:\n"
          + "&lt;p&gt;é, è, ô, ç, à, ê, â.&lt;/p&gt; They can, of course, come in uppercase as well: &lt;p&gt;É, È Ô, Ç, À,"
          + " Ê, Â&lt;/p&gt;. UTF-8 handles this well.";

  @BeforeEach
  void setup() throws Exception {
    HttpClientManager.HttpClientBuilderFacade builder =
        mock(HttpClientManager.HttpClientBuilderFacade.class);

    HttpClientManager manager = mock(HttpClientManager.class);
    doReturn(builder).when(manager).createBuilder();

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    doReturn(client).when(builder).build();

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    doReturn(response)
        .when(client)
        .execute(any(HttpHost.class), any(HttpRequest.class), any(HttpContext.class));

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(DATA.getBytes()));
    doReturn(entity).when(response).getEntity();
    mockedHttpClientManager.when(HttpClientManager::getInstance).thenReturn(manager);

    doReturn(false).when(meta).isUrlInField();
    doReturn("body").when(meta).getFieldName();

    doReturn(false).when(log).isDetailed();

    doCallRealMethod().when(http).callHttpService(any(IRowMeta.class), any(Object[].class));
    doReturn(HttpURLConnection.HTTP_OK)
        .when(http)
        .requestStatusCode(any(CloseableHttpResponse.class));
    doReturn(new Header[0]).when(http).searchForHeaders(any(CloseableHttpResponse.class));
  }

  @BeforeAll
  static void setUpStaticMocks() {
    mockedHttpClientManager = mockStatic(HttpClientManager.class);
  }

  @AfterAll
  static void tearDownStaticMocks() {
    mockedHttpClientManager.close();
  }

  @Disabled("This test needs to be reviewed")
  @Test
  void callHttpServiceWithUTF8Encoding() throws Exception {
    doReturn("UTF-8").when(meta).getEncoding();
    assertEquals(DATA, http.callHttpService(rmi, new Object[] {0})[0]);
  }

  @Disabled("This test needs to be reviewed")
  @Test
  void callHttpServiceWithoutEncoding() throws Exception {
    doReturn(null).when(meta).getEncoding();
    assertNotEquals(DATA, http.callHttpService(rmi, new Object[] {0})[0]);
  }
}
