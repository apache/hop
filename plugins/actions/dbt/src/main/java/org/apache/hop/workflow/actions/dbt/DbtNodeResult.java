/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.dbt;

/** Per-node outcome parsed from a single entry of dbt's {@code run_results.json}. */
public final class DbtNodeResult {

  private final String uniqueId;
  private final String status;
  private final double executionTime;
  private final String message;
  private final String relationName;

  public DbtNodeResult(
      String uniqueId, String status, double executionTime, String message, String relationName) {
    this.uniqueId = uniqueId;
    this.status = status;
    this.executionTime = executionTime;
    this.message = message;
    this.relationName = relationName;
  }

  public String getUniqueId() {
    return uniqueId;
  }

  /** dbt node status: success, error, fail, pass, warn, skipped, runtime error. */
  public String getStatus() {
    return status;
  }

  public double getExecutionTime() {
    return executionTime;
  }

  public String getMessage() {
    return message;
  }

  /** Fully-qualified relation (e.g. {@code "db"."schema"."table"}) when dbt provides it. */
  public String getRelationName() {
    return relationName;
  }

  /** Whether this node represents a failed outcome (drives error-hop routing). */
  public boolean isFailure() {
    if (status == null) {
      return false;
    }
    String s = status.toLowerCase();
    return s.equals("error") || s.equals("fail") || s.equals("runtime error");
  }
}
