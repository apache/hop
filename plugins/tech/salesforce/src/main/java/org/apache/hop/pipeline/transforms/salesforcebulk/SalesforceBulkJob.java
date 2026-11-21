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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import lombok.Getter;

/** One Bulk API 2.0 ingest or query job. */
@Getter
public class SalesforceBulkJob {
  private final String id;
  private final String state;
  private final String errorMessage;
  private final int recordsProcessed;
  private final int recordsFailed;

  public SalesforceBulkJob(
      String id, String state, String errorMessage, int recordsProcessed, int recordsFailed) {
    this.id = id;
    this.state = state;
    this.errorMessage = errorMessage;
    this.recordsProcessed = recordsProcessed;
    this.recordsFailed = recordsFailed;
  }

  public boolean isFinished() {
    return "JobComplete".equals(state) || "Failed".equals(state) || "Aborted".equals(state);
  }

  public boolean isAborted() {
    return "Aborted".equals(state);
  }
}
