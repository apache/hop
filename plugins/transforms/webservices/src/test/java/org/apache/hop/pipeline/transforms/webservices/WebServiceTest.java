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

package org.apache.hop.pipeline.transforms.webservices;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebServiceTest {

  private static final String LOCATION_HEADER = "Location";

  private static final String TEST_URL = "TEST_URL";

  private static final String NOT_VALID_URL = "NOT VALID URL";

  private TransformMockHelper<WebServiceMeta, WebServiceData> mockHelper;

  private WebService webServiceTransform;

  @BeforeEach
  void setUpBefore() {
    mockHelper =
        new TransformMockHelper<>("WebService", WebServiceMeta.class, WebServiceData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    webServiceTransform =
        spy(
            new WebService(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void newHttpMethodWithInvalidUrl() {
    assertThrows(URISyntaxException.class, () -> webServiceTransform.getHttpMethod(NOT_VALID_URL));
  }

  @Test
  void getLocationFrom() {
    HttpPost postMethod = mock(HttpPost.class);
    Header locationHeader = new BasicHeader(LOCATION_HEADER, TEST_URL);
    doReturn(locationHeader).when(postMethod).getFirstHeader(LOCATION_HEADER);

    assertEquals(TEST_URL, WebService.getLocationFrom(postMethod));
  }
}
