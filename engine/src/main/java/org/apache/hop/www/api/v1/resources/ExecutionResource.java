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

package org.apache.hop.www.api.v1.resources;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.www.api.v1.StreamingWebServiceOutput;
import org.apache.hop.www.api.v1.model.SyncRequest;
import org.apache.hop.www.service.PreparedWebService;
import org.apache.hop.www.service.WebServiceExecutor;
import org.apache.hop.www.service.WebServiceRequest;

/** Executes web services synchronously. */
@Path("/execute")
public class ExecutionResource extends BaseApiResource {

  /**
   * Execute a pipeline synchronously, by referencing the Web Service metadata object name.
   *
   * <p>This runs the exact same code as the {@code /hop/webService} servlet, so the body and header
   * content variables, binary output fields and status listing all behave identically. The response
   * is streamed row by row.
   *
   * @param request the request details
   * @return the output of the web service
   */
  @POST
  @Path("/sync")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response executeSynchronously(SyncRequest request, @Context HttpHeaders httpHeaders)
      throws HopException {

    // An empty POST body deserializes to null; let the executor report it as a bad request
    // rather than NPEing our way to a 500.
    SyncRequest syncRequest = request == null ? new SyncRequest() : request;

    WebServiceRequest webServiceRequest = new WebServiceRequest(syncRequest.getService());
    webServiceRequest.setRunConfigurationName(syncRequest.getRunConfig());
    webServiceRequest.setBodyContentSupplier(() -> Const.NVL(syncRequest.getBodyContent(), ""));
    webServiceRequest.setHeaders(collectHeaders(httpHeaders));
    webServiceRequest.setParameters(syncRequest.getVariables());

    PreparedWebService prepared =
        new WebServiceExecutor(
                context.getVariables(), context.getMetadataProvider(), context.getPipelineMap())
            .prepare(webServiceRequest);

    StreamingOutput stream =
        output -> {
          try {
            prepared.execute(new StreamingWebServiceOutput(output, context.getLog()));
          } catch (HopException e) {
            throw new WebServiceStreamingException(e);
          }
        };

    // The charset belongs in the Content-Type parameter. Response.encoding() would set
    // Content-Encoding instead, which is a different header entirely.
    MediaType mediaType = mediaType(prepared.getContentType(), prepared.getEncoding());
    return Response.ok(stream).type(mediaType).build();
  }

  private static Map<String, String> collectHeaders(HttpHeaders httpHeaders) {
    Map<String, String> headers = new LinkedHashMap<>();
    if (httpHeaders != null) {
      httpHeaders
          .getRequestHeaders()
          .forEach((name, values) -> headers.put(name, values.isEmpty() ? "" : values.get(0)));
    }
    return headers;
  }

  /**
   * The content type is free text on the Web Service metadata, so it may not parse. The servlet
   * passes whatever it is straight to the response; here an unparseable value would abort the whole
   * request, so fall back to the same default the executor uses.
   */
  private MediaType mediaType(String contentType, String encoding) {
    try {
      return MediaType.valueOf(contentType).withCharset(encoding);
    } catch (IllegalArgumentException e) {
      context
          .getLog()
          .logError("Invalid web service content type '" + contentType + "', using text/plain", e);
      return MediaType.TEXT_PLAIN_TYPE.withCharset(encoding);
    }
  }

  /** Wraps a failure raised while the response body was already being streamed. */
  static class WebServiceStreamingException extends RuntimeException {
    WebServiceStreamingException(HopException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
