/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.advisor;

/**
 * Well-known location ids Hop itself uses when opening an AI session. These are ordinary strings:
 * other plugins (Data Vault, Business Vault, dimensional modelers, lineage, metadata editors, …)
 * define their own ids and pass them on {@link AiAdvisorOpenRequest#location}. Hop never enumerates
 * the full set.
 */
public final class AiAdvisorLocations {

  /** Pipeline canvas / toolbar AI Help. */
  public static final String PIPELINE_GRAPH = "pipeline-graph";

  /** Workflow canvas / toolbar AI Help. */
  public static final String WORKFLOW_GRAPH = "workflow-graph";

  /** Perspective, Tools menu, or an unbound new session. */
  public static final String PERSPECTIVE = "perspective";

  private AiAdvisorLocations() {}
}
