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

package org.apache.hop.www.api.v1.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** The body posted to the synchronous web service execution endpoint. */
@Getter
@Setter
public class SyncRequest {

  /** The name of the Web Service metadata element to run. */
  private String service;

  /** The pipeline run configuration to use, overriding the one on the web service. */
  private String runConfig;

  /** Variables or parameters to set on the pipeline. */
  private Map<String, String> variables = new HashMap<>();

  /** Set as a variable using the body content variable option on the Web Service metadata. */
  private String bodyContent;

  public void setVariables(Map<String, String> variables) {
    // A client sending "variables": null must not blow up the execution.
    this.variables = variables == null ? new HashMap<>() : variables;
  }
}
