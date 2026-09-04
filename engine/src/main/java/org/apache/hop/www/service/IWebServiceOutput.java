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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Where a web service execution writes its output.
 *
 * <p>This is the only thing that differs between the transports which can run a {@link WebService}:
 * the servlet on {@code /hop/webService} drives an {@code HttpServletResponse}, the JSON API drives
 * a JAX-RS {@code StreamingOutput}.
 */
public interface IWebServiceOutput {

  /**
   * Called once, before the first row is written.
   *
   * @param contentType the content type configured on the web service, never empty
   * @param encoding the character encoding to report
   */
  void setContentType(String contentType, String encoding);

  /**
   * Called for every row when the web service defines a status code field. Transports which have
   * already committed their response status may ignore this.
   *
   * @param statusCode the status code found on the row
   */
  void setStatus(int statusCode);

  /**
   * @return the stream to write the rows to
   */
  OutputStream getOutputStream() throws IOException;
}
