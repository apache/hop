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

package org.apache.hop.core.failure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A pipeline, workflow, action or transform taking part in a failure: the run that failed, or one
 * of its parents.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FailureRun {
  /** The log channel id, which is also the execution id in the execution information. */
  private String executionId;

  /** The logging object type: PIPELINE, WORKFLOW, ACTION, TRANSFORM, ... */
  private String type;

  private String name;

  private String filename;

  /** The container id of the top-level execution, as a Hop Server knows it. */
  private String containerId;
}
