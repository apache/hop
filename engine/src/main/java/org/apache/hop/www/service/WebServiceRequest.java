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

import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;

/**
 * A transport-independent request to execute a {@link WebService}: everything {@link
 * WebServiceExecutor} needs, with nothing servlet- or JAX-RS-specific in it.
 */
@Getter
@Setter
public class WebServiceRequest {

  /**
   * Supplies the request body. Only called when the web service defines a body content variable.
   */
  @FunctionalInterface
  public interface IBodyContentSupplier {
    String get() throws HopException;
  }

  /** The name of the web service metadata element to run. */
  private String serviceName;

  /** Overrides the run configuration named on the web service. Optional. */
  private String runConfigurationName;

  /**
   * Reads the request body. Deliberately lazy: the body is only consumed when the web service
   * actually declares a body content variable, so a request to a service which doesn't want one
   * never has its payload slurped into memory.
   */
  private IBodyContentSupplier bodyContentSupplier = () -> "";

  /** The request headers, serialised to JSON when the web service declares a header variable. */
  private Map<String, String> headers = Collections.emptyMap();

  /** Request parameters, applied as pipeline parameters when they match, as variables otherwise. */
  private Map<String, String> parameters = Collections.emptyMap();

  public WebServiceRequest() {
    // Default constructor
  }

  public WebServiceRequest(String serviceName) {
    this.serviceName = serviceName;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers == null ? Collections.emptyMap() : headers;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters == null ? Collections.emptyMap() : parameters;
  }

  public void setBodyContentSupplier(IBodyContentSupplier bodyContentSupplier) {
    this.bodyContentSupplier = bodyContentSupplier == null ? () -> "" : bodyContentSupplier;
  }
}
