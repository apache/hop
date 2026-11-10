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

import java.util.function.BooleanSupplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;

/** Opens a Bulk API client from a Salesforce connection and polls a job to a final state. */
public final class SalesforceBulkRuntime {
  private static final Class<?> PKG = SalesforceBulkRuntime.class;

  private SalesforceBulkRuntime() {}

  public static SalesforceBulkClient open(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      ILogChannel log,
      String connectionName)
      throws HopException {
    String name = variables.resolve(connectionName);
    if (Utils.isEmpty(name)) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulk.Error.NoConnection"));
    }
    IHopMetadataSerializer<org.apache.hop.metadata.salesforce.SalesforceConnection> serializer =
        metadataProvider.getSerializer(
            org.apache.hop.metadata.salesforce.SalesforceConnection.class);
    org.apache.hop.metadata.salesforce.SalesforceConnection metadata = serializer.load(name);
    if (metadata == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "SalesforceBulk.Error.MissingConnection", name));
    }
    SalesforceConnection connection = metadata.createConnection(variables, log);
    boolean tokenReady =
        !Utils.isEmpty(connection.getOauthAccessToken())
            && !Utils.isEmpty(connection.getOauthInstanceUrl());
    if (!tokenReady) {
      connection.connect();
    }
    return new SalesforceBulkHttpClient(SalesforceBulkSession.from(connection));
  }

  public static long secondsToMillis(String resolved, long defaultSeconds) {
    int seconds = Const.toInt(resolved, (int) defaultSeconds);
    if (seconds <= 0) {
      seconds = (int) defaultSeconds;
    }
    return seconds * 1000L;
  }

  public static SalesforceBulkJob poll(
      SalesforceBulkClient client,
      String jobId,
      boolean query,
      long timeoutMillis,
      long pollMillis,
      BooleanSupplier stopped)
      throws HopException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (true) {
      if (stopped.getAsBoolean()) {
        abort(client, jobId, query);
        throw new HopException(BaseMessages.getString(PKG, "SalesforceBulk.Error.Stopped", jobId));
      }
      SalesforceBulkJob job = query ? client.getQueryJob(jobId) : client.getIngestJob(jobId);
      if (job.isFinished()) {
        return job;
      }
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        abort(client, jobId, query);
        throw new HopException(
            BaseMessages.getString(PKG, "SalesforceBulk.Error.Timeout", jobId, job.getState()));
      }
      try {
        Thread.sleep(Math.min(pollMillis, remaining));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        abort(client, jobId, query);
        throw new HopException(
            BaseMessages.getString(PKG, "SalesforceBulk.Error.Stopped", jobId), e);
      }
    }
  }

  public static void abort(SalesforceBulkClient client, String jobId, boolean query) {
    if (client == null || Utils.isEmpty(jobId)) {
      return;
    }
    try {
      if (query) {
        client.abortQueryJob(jobId);
      } else {
        client.abortIngestJob(jobId);
      }
    } catch (Exception ignored) {
      // The pipeline is already leaving the job.
    }
  }
}
