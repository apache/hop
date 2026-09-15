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

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.execution.ExecutionInfoLocations;
import org.apache.hop.www.service.WebServiceException;

/**
 * Turns any error raised by a JSON API resource into a JSON body.
 *
 * <p>Mirrors what the servlets do with {@code sendSafeError}: the reason is reported to the caller
 * only when it came from Hop itself, everything else is reported generically and the detail stays
 * in the server log. Stack traces are never sent to the client.
 */
@Provider
public class HopApiExceptionMapper implements ExceptionMapper<Throwable> {

  private static final String GENERIC_MESSAGE = "Unable to serve the request.";

  private final ILogChannel log;

  public HopApiExceptionMapper(ILogChannel log) {
    this.log = log;
  }

  @Override
  public Response toResponse(Throwable throwable) {
    // Jersey raises its own routing failures (unknown path, wrong verb, wrong content type,
    // unparseable body) as entity-less WebApplicationExceptions. Those already carry the right
    // status and must not be flattened into a 500 - nor logged as a server error, or any 404
    // probe would write a stack trace.
    if (throwable instanceof WebApplicationException webApplicationException) {
      int frameworkStatus = webApplicationException.getResponse().getStatus();
      if (log != null) {
        log.logDebug("Hop API request rejected with status " + frameworkStatus);
      }
      return Response.status(frameworkStatus)
          .entity(
              Map.of(
                  "error", tidy(Const.NVL(webApplicationException.getMessage(), GENERIC_MESSAGE))))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }

    // An unparseable request body is the client's mistake, and the parser's message points at the
    // offending character, which is genuinely useful. It says nothing about the server.
    if (throwable instanceof JsonProcessingException jsonProcessingException) {
      if (log != null) {
        log.logDebug("Hop API request body could not be parsed");
      }
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              Map.of(
                  "error",
                  "Could not parse the request body: "
                      + tidy(
                          Const.NVL(jsonProcessingException.getOriginalMessage(), "invalid JSON"))))
          .type(MediaType.APPLICATION_JSON)
          .build();
    }

    if (log != null) {
      log.logError("Hop API request failed", throwable);
    }

    Response.Status status = Response.Status.INTERNAL_SERVER_ERROR;
    String message = GENERIC_MESSAGE;

    if (throwable instanceof WebServiceException webServiceException) {
      status =
          switch (webServiceException.getReason()) {
            case BAD_REQUEST -> Response.Status.BAD_REQUEST;
            case NOT_FOUND -> Response.Status.NOT_FOUND;
            case DISABLED -> Response.Status.CONFLICT;
          };
      message = Const.NVL(webServiceException.getMessage(), GENERIC_MESSAGE);
    } else if (throwable instanceof HopApiBadRequestException) {
      status = Response.Status.BAD_REQUEST;
      message = Const.NVL(throwable.getMessage(), GENERIC_MESSAGE);
    } else if (throwable instanceof HopApiNotFoundException
        || throwable instanceof ExecutionInfoLocations.LocationNotFoundException) {
      status = Response.Status.NOT_FOUND;
      message = Const.NVL(throwable.getMessage(), GENERIC_MESSAGE);
    } else if (throwable instanceof HopException) {
      message = Const.NVL(throwable.getMessage(), GENERIC_MESSAGE);
    }

    return Response.status(status)
        .entity(Map.of("error", tidy(message)))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /** HopException messages are built with surrounding newlines; they read badly inside JSON. */
  private static String tidy(String message) {
    return message.replace("\n", " ").replaceAll("\\s+", " ").trim();
  }
}
