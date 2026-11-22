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

import org.apache.hop.core.exception.HopException;

/** Bulk API 2.0 calls used by the load and input transforms. */
public interface SalesforceBulkClient extends AutoCloseable {
  SalesforceBulkJob createIngestJob(SalesforceBulkIngestRequest request) throws HopException;

  void uploadIngestData(String jobId, byte[] csv) throws HopException;

  SalesforceBulkJob completeIngestUpload(String jobId) throws HopException;

  SalesforceBulkJob getIngestJob(String jobId) throws HopException;

  /** Successful results when {@code failed} is false, failed results otherwise. */
  String ingestResults(String jobId, boolean failed) throws HopException;

  void abortIngestJob(String jobId) throws HopException;

  SalesforceBulkJob createQueryJob(String operation, String soql) throws HopException;

  SalesforceBulkJob getQueryJob(String jobId) throws HopException;

  SalesforceBulkQueryPage queryResults(String jobId, String locator) throws HopException;

  void abortQueryJob(String jobId) throws HopException;

  @Override
  void close();
}
