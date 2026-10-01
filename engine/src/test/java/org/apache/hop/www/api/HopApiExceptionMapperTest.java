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

package org.apache.hop.www.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.execution.ExecutionInfoLocations;
import org.apache.hop.www.service.WebServiceException;
import org.junit.jupiter.api.Test;

/**
 * The mapper is the API's whole error contract: it decides the status a client sees and is the
 * thing that stops server internals leaking, which the old hop-rest BaseResource did not do.
 */
class HopApiExceptionMapperTest {

  private final HopApiExceptionMapper mapper = new HopApiExceptionMapper(mock(ILogChannel.class));

  @SuppressWarnings("unchecked")
  private static String errorOf(Response response) {
    return ((Map<String, String>) response.getEntity()).get("error");
  }

  @Test
  void aBadRequestReasonBecomes400() {
    Response response =
        mapper.toResponse(
            new WebServiceException(WebServiceException.Reason.BAD_REQUEST, "no service given"));

    assertEquals(400, response.getStatus());
    assertEquals("no service given", errorOf(response));
  }

  @Test
  void aNotFoundReasonBecomes404() {
    Response response =
        mapper.toResponse(
            new WebServiceException(WebServiceException.Reason.NOT_FOUND, "no such service"));

    assertEquals(404, response.getStatus());
  }

  @Test
  void aDisabledServiceBecomes409() {
    Response response =
        mapper.toResponse(
            new WebServiceException(WebServiceException.Reason.DISABLED, "service is disabled"));

    assertEquals(409, response.getStatus());
  }

  @Test
  void anApiNotFoundBecomes404() {
    Response response = mapper.toResponse(new HopApiNotFoundException("no such element"));

    assertEquals(404, response.getStatus());
    assertEquals("no such element", errorOf(response));
  }

  @Test
  void aMissingLocationBecomes404() {
    Response response =
        mapper.toResponse(new ExecutionInfoLocations.LocationNotFoundException("no such location"));

    assertEquals(404, response.getStatus());
  }

  @Test
  void anApiBadRequestBecomes400() {
    Response response = mapper.toResponse(new HopApiBadRequestException("bad execType"));

    assertEquals(400, response.getStatus());
    assertEquals("bad execType", errorOf(response));
  }

  @Test
  void aPlainHopExceptionBecomes500ButKeepsItsMessage() {
    Response response = mapper.toResponse(new HopException("could not reach the database"));

    assertEquals(500, response.getStatus());
    assertEquals("could not reach the database", errorOf(response));
  }

  @Test
  void anUnexpectedFailureIsReportedGenerically() {
    // A raw runtime failure must not tell a client anything about the server internals.
    Response response =
        mapper.toResponse(
            new NullPointerException("Cannot invoke getChildIds() because it is null"));

    assertEquals(500, response.getStatus());
    assertFalse(errorOf(response).contains("getChildIds"));
    assertFalse(errorOf(response).contains("NullPointerException"));
  }

  @Test
  void neverLeaksAStackTrace() {
    Exception cause = new IllegalStateException("internal detail");
    Response response = mapper.toResponse(new RuntimeException("wrapper", cause));

    String body = errorOf(response);
    assertFalse(body.contains("at org.apache.hop"));
    assertFalse(body.contains("internal detail"));
  }

  @Test
  void alwaysRespondsAsJson() {
    Response response = mapper.toResponse(new HopException("x"));

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  void messagesAreFlattenedForJson() {
    // HopException builds messages with surrounding newlines; they read badly inside a JSON string.
    Response response = mapper.toResponse(new HopException("line one\nline two"));

    String body = errorOf(response);
    assertFalse(body.contains("\n"));
    assertTrue(body.contains("line one line two"));
  }

  @Test
  void aNullMessageStillProducesABody() {
    Response response = mapper.toResponse(new HopException((String) null));

    assertEquals(500, response.getStatus());
    assertFalse(errorOf(response).isEmpty());
  }

  @Test
  void aNullLogChannelIsTolerated() {
    Response response = new HopApiExceptionMapper(null).toResponse(new HopException("x"));

    assertEquals(500, response.getStatus());
  }

  @Test
  void theFrameworksOwn404IsNotFlattenedIntoA500() {
    // Jersey raises an unknown path as an entity-less NotFoundException; mapping it to 500 would
    // stop a client telling "wrong URL" apart from "server broken".
    Response response = mapper.toResponse(new jakarta.ws.rs.NotFoundException());

    assertEquals(404, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
  }

  @Test
  void aWrongVerbStaysA405() {
    Response response = mapper.toResponse(new jakarta.ws.rs.NotAllowedException("GET"));

    assertEquals(405, response.getStatus());
  }

  @Test
  void anUnsupportedContentTypeStaysA415() {
    Response response = mapper.toResponse(new jakarta.ws.rs.NotSupportedException());

    assertEquals(415, response.getStatus());
  }

  @Test
  void aMalformedBodyStaysA400() {
    Response response = mapper.toResponse(new jakarta.ws.rs.BadRequestException());

    assertEquals(400, response.getStatus());
  }

  @Test
  void routingFailuresAreNotLoggedAsServerErrors() {
    // A 404 probe must not write a stack trace, or the log is trivially floodable.
    ILogChannel log = mock(ILogChannel.class);
    new HopApiExceptionMapper(log).toResponse(new jakarta.ws.rs.NotFoundException());

    org.mockito.Mockito.verify(log, org.mockito.Mockito.never())
        .logError(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any(Throwable.class));
  }

  @Test
  void anUnparseableBodyIsAJson400() {
    // Jackson's own mappers answer in plain text; ours keeps the JSON error contract whole.
    com.fasterxml.jackson.core.JsonProcessingException parseError =
        new com.fasterxml.jackson.core.JsonParseException(null, "unexpected character 'n'");

    Response response = mapper.toResponse(parseError);

    assertEquals(400, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    assertTrue(errorOf(response).contains("Could not parse the request body"));
    assertTrue(errorOf(response).contains("unexpected character"));
  }
}
