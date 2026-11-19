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

package org.apache.hop.pipeline.transforms.googlesheets;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class GoogleSheetsOutputTest {

  private TransformMockHelper<GoogleSheetsOutputMeta, GoogleSheetsOutputData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "GoogleSheetsOutput", GoogleSheetsOutputMeta.class, GoogleSheetsOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  /**
   * After the last input row, processRow() opens a new connection to write the buffered rows. That
   * connection must use the proxy host and port with variables resolved, as init() does.
   */
  @Test
  void processRowResolvesVariablesInProxySettings() throws Exception {
    GoogleSheetsOutputMeta meta = new GoogleSheetsOutputMeta();
    meta.setProxyHost("${PROXY_HOST}");
    meta.setProxyPort("${PROXY_PORT}");
    GoogleSheetsOutputData data = new GoogleSheetsOutputData();
    // Rows were already buffered: this call sees the end of the input.
    data.currentRow = 1;

    GoogleSheetsOutput transform =
        spy(
            new GoogleSheetsOutput(
                mockHelper.transformMeta,
                meta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
    transform.setVariable("PROXY_HOST", "proxy.example.com");
    transform.setVariable("PROXY_PORT", "3128");
    transform.first = false;
    doReturn(null).when(transform).getRow();

    try (MockedStatic<GoogleSheetsConnectionFactory> factory =
        mockStatic(GoogleSheetsConnectionFactory.class)) {
      // Stop right after the transport is requested; the Google APIs are not under test.
      factory
          .when(() -> GoogleSheetsConnectionFactory.newTransport(anyString(), anyString()))
          .thenThrow(new IllegalStateException("stop after transport"));

      assertThrows(HopException.class, transform::processRow);

      factory.verify(() -> GoogleSheetsConnectionFactory.newTransport("proxy.example.com", "3128"));
    }
  }
}
