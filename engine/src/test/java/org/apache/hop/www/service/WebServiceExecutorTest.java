/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers how a request is resolved against the Web Service metadata. Both the {@code
 * /hop/webService} servlet and the JSON API run through this, so a wrong answer here is wrong on
 * both surfaces.
 */
class WebServiceExecutorTest {

  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private IHopMetadataSerializer<WebService> serializer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    variables = new Variables();
    metadataProvider = mock(IHopMetadataProvider.class);
    serializer = mock(IHopMetadataSerializer.class);
    when(metadataProvider.getSerializer(WebService.class)).thenReturn(serializer);
  }

  private WebServiceExecutor executor() {
    return new WebServiceExecutor(variables, metadataProvider, null);
  }

  @Test
  void anEmptyServiceNameIsABadRequest() {
    WebServiceException thrown =
        assertThrows(
            WebServiceException.class, () -> executor().prepare(new WebServiceRequest("")));
    assertEquals(WebServiceException.Reason.BAD_REQUEST, thrown.getReason());
  }

  @Test
  void aNullServiceNameIsABadRequest() {
    WebServiceException thrown =
        assertThrows(
            WebServiceException.class, () -> executor().prepare(new WebServiceRequest(null)));
    assertEquals(WebServiceException.Reason.BAD_REQUEST, thrown.getReason());
  }

  @Test
  void anUnknownServiceIsNotFound() throws Exception {
    when(serializer.load("nope")).thenReturn(null);

    WebServiceException thrown =
        assertThrows(
            WebServiceException.class, () -> executor().prepare(new WebServiceRequest("nope")));

    assertEquals(WebServiceException.Reason.NOT_FOUND, thrown.getReason());
    assertTrue(thrown.getMessage().contains("nope"));
  }

  @Test
  void aDisabledServiceIsReportedAsDisabled() throws Exception {
    WebService service = new WebService();
    service.setName("test");
    service.setEnabled(false);
    when(serializer.load("test")).thenReturn(service);

    WebServiceException thrown =
        assertThrows(
            WebServiceException.class, () -> executor().prepare(new WebServiceRequest("test")));

    assertEquals(WebServiceException.Reason.DISABLED, thrown.getReason());
  }

  @Test
  void theBodyIsNotReadWhenTheServiceDeclaresNoBodyVariable() throws Exception {
    WebService service = new WebService();
    service.setName("test");
    service.setEnabled(true);
    service.setBodyContentVariable("");
    service.setFilename("/does/not/exist.hpl");
    when(serializer.load("test")).thenReturn(service);

    boolean[] bodyWasRead = {false};
    WebServiceRequest request = new WebServiceRequest("test");
    request.setBodyContentSupplier(
        () -> {
          bodyWasRead[0] = true;
          return "body";
        });

    // Loading the (missing) pipeline file fails, but only after the body decision has been made.
    assertThrows(HopException.class, () -> executor().prepare(request));
    assertTrue(!bodyWasRead[0], "the request body must not be consumed when it is not wanted");
  }

  @Test
  void aRequestDefensivelyNormalizesNullCollections() {
    WebServiceRequest request = new WebServiceRequest("test");
    request.setHeaders(null);
    request.setParameters(null);
    request.setBodyContentSupplier(null);

    assertTrue(request.getHeaders().isEmpty());
    assertTrue(request.getParameters().isEmpty());
    assertEquals("", assertDoesNotThrowSupplier(request));
  }

  private static String assertDoesNotThrowSupplier(WebServiceRequest request) {
    try {
      return request.getBodyContentSupplier().get();
    } catch (HopException e) {
      throw new AssertionError("the default body supplier must not throw", e);
    }
  }

  @Test
  void theReasonEnumCoversEveryStatusTheApiMaps() {
    // The exception mapper switches exhaustively over these; a new one must be a deliberate change.
    assertEquals(3, WebServiceException.Reason.values().length);
  }
}
