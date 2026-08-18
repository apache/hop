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

package org.apache.hop.server.loadbalance;

import lombok.Getter;
import lombok.Setter;

/** Ledger record for one load-balanced execution attempt. */
@Getter
@Setter
public class LoadBalancingAssignment {

  public static final String STATUS_ASSIGNING = "ASSIGNING";
  public static final String STATUS_RUNNING = "RUNNING";
  public static final String STATUS_RETRYING = "RETRYING";
  public static final String STATUS_FINISHED = "FINISHED";
  public static final String STATUS_FAILED = "FAILED";

  private String executionId;
  private String runConfigurationName;
  private String executorName;
  private String executorType;
  private String serverName;
  private int attempt;
  private long firstAttemptEpochMs;
  private String status;
  private String containerId;
  private String lastError;
  private String algorithm;
  private int occupyingSlotsAtAssignment;
  private int maxConcurrent;

  public LoadBalancingAssignment() {}
}
