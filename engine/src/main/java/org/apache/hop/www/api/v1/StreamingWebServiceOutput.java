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

package org.apache.hop.www.api.v1;

import java.io.OutputStream;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.www.service.IWebServiceOutput;

/**
 * Writes web service output to a JAX-RS {@code StreamingOutput} stream.
 *
 * <p>The content type and the response status are fixed by JAX-RS at the moment the {@code
 * Response} is returned, which is before this stream is written. The content type is therefore
 * taken from the prepared web service by the resource itself, and the per-row status code field is
 * not honoured here - use {@code /hop/webService} when a pipeline needs to drive the HTTP status.
 */
public class StreamingWebServiceOutput implements IWebServiceOutput {

  private final OutputStream outputStream;
  private final ILogChannel log;
  private boolean statusReported;

  public StreamingWebServiceOutput(OutputStream outputStream, ILogChannel log) {
    this.outputStream = outputStream;
    this.log = log;
  }

  @Override
  public void setContentType(String contentType, String encoding) {
    // Already set on the Response by the resource, before this stream was opened.
  }

  @Override
  public void setStatus(int statusCode) {
    // The response status is committed before the body is streamed, so this can only be logged.
    if (statusCode != 200 && !statusReported) {
      statusReported = true;
      if (log != null) {
        log.logDetailed(
            "The web service returned status code "
                + statusCode
                + ", which the JSON API cannot apply to an already committed response. "
                + "Use /hop/webService if the status code field has to reach the client.");
      }
    }
  }

  @Override
  public OutputStream getOutputStream() {
    return outputStream;
  }
}
