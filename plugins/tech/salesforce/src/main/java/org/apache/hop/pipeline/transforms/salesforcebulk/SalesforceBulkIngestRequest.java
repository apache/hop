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

/** Fields sent when a Bulk API 2.0 ingest job is created. */
@Getter
public class SalesforceBulkIngestRequest {
  private final String salesforceObject;
  private final SalesforceBulkOperation operation;
  private final String externalIdField;
  private final SalesforceBulkLineEnding lineEnding;
  private final SalesforceBulkDelimiter delimiter;
  private final SalesforceBulkConcurrency concurrency;

  public SalesforceBulkIngestRequest(
      String salesforceObject,
      SalesforceBulkOperation operation,
      String externalIdField,
      SalesforceBulkLineEnding lineEnding,
      SalesforceBulkDelimiter delimiter,
      SalesforceBulkConcurrency concurrency) {
    this.salesforceObject = salesforceObject;
    this.operation = operation;
    this.externalIdField = externalIdField;
    this.lineEnding = lineEnding == null ? SalesforceBulkLineEnding.LF : lineEnding;
    this.delimiter = delimiter == null ? SalesforceBulkDelimiter.COMMA : delimiter;
    this.concurrency = concurrency == null ? SalesforceBulkConcurrency.PARALLEL : concurrency;
  }
}
