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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Reads a SOQL query through a Bulk API 2.0 query job. */
public class SalesforceBulkInput
    extends BaseTransform<SalesforceBulkInputMeta, SalesforceBulkInputData> {
  private static final Class<?> PKG = SalesforceBulkInput.class;

  public SalesforceBulkInput(
      TransformMeta transformMeta,
      SalesforceBulkInputMeta meta,
      SalesforceBulkInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    if (data.client != null) {
      return prepareSettings();
    }
    try {
      data.client =
          SalesforceBulkRuntime.open(this, metadataProvider, getLogChannel(), meta.getConnection());
      return prepareSettings();
    } catch (Exception e) {
      logError(e.getMessage(), e);
      return false;
    }
  }

  private boolean prepareSettings() {
    try {
      data.outputRowMeta = new org.apache.hop.core.row.RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      data.columnNames = new ArrayList<>();
      data.targets = new ArrayList<>();
      if (meta.getFields() != null) {
        for (SalesforceBulkInputField field : meta.getFields()) {
          if (field == null || Utils.isEmpty(field.getName())) {
            continue;
          }
          String name = resolve(field.getName());
          data.columnNames.add(name);
          data.targets.add(data.outputRowMeta.searchValueMeta(name));
        }
      }
      if (data.columnNames.isEmpty()) {
        logError(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.NoFields"));
        return false;
      }
      String jobField = resolve(meta.getJobIdField());
      data.jobIdIndex = Utils.isEmpty(jobField) ? -1 : data.outputRowMeta.indexOfValue(jobField);
      data.timeoutMillis = SalesforceBulkRuntime.secondsToMillis(resolve(meta.getTimeout()), 3600);
      data.pollMillis = SalesforceBulkRuntime.secondsToMillis(resolve(meta.getPollInterval()), 10);
      data.maxRows = longValue(resolve(meta.getMaxRows()));
      SalesforceBulkQueryOperation operation =
          meta.getOperation() == null ? SalesforceBulkQueryOperation.QUERY : meta.getOperation();
      data.operationCode = operation.getCode();
      data.soql = resolve(meta.getSoql());
      if (Utils.isEmpty(data.soql)) {
        logError(BaseMessages.getString(PKG, "SalesforceBulkInput.Error.NoSoql"));
        return false;
      }
      return true;
    } catch (Exception e) {
      logError(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    if (!data.started) {
      startQuery();
    }
    if (data.pageIndex >= data.page.size() && !loadNextPage()) {
      setOutputDone();
      return false;
    }
    if (data.maxRows > 0 && getLinesOutput() >= data.maxRows) {
      setOutputDone();
      return false;
    }
    putRow(data.outputRowMeta, data.page.get(data.pageIndex++));
    return true;
  }

  private void startQuery() throws HopException {
    data.started = true;
    SalesforceBulkJob created = data.client.createQueryJob(data.operationCode, data.soql);
    data.openJobId = created.getId();
    data.jobId = created.getId();
    SalesforceBulkJob finished =
        SalesforceBulkRuntime.poll(
            data.client,
            created.getId(),
            true,
            data.timeoutMillis,
            data.pollMillis,
            this::isStopped);
    data.openJobId = null;
    if (finished.isAborted() || "Failed".equals(finished.getState())) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "SalesforceBulkInput.Error.QueryFailed",
              created.getId(),
              Utils.isEmpty(finished.getErrorMessage())
                  ? finished.getState()
                  : finished.getErrorMessage()));
    }
    logBasic(BaseMessages.getString(PKG, "SalesforceBulkInput.Log.JobFinished", created.getId()));
    data.locator = null;
  }

  private boolean loadNextPage() throws HopException {
    if (data.started && data.locator == null && data.pageIndex > 0 && !data.page.isEmpty()) {
      return false;
    }
    if (data.pageIndex > 0 && (data.locator == null || "null".equalsIgnoreCase(data.locator))) {
      return false;
    }
    SalesforceBulkQueryPage page = data.client.queryResults(data.jobId, data.locator);
    data.locator = page.hasMore() ? page.getLocator() : null;
    data.page = convert(page.getCsv());
    data.pageIndex = 0;
    return !data.page.isEmpty();
  }

  private List<Object[]> convert(String csv) throws HopException {
    List<List<String>> table = SalesforceBulkCsv.parse(csv);
    List<Object[]> rows = new ArrayList<>();
    if (table.size() < 2) {
      return rows;
    }
    Map<String, Integer> index = new HashMap<>();
    List<String> headers = table.get(0);
    for (int i = 0; i < headers.size(); i++) {
      index.put(headers.get(i), i);
    }
    for (String column : data.columnNames) {
      if (!index.containsKey(column)) {
        throw new HopException(
            BaseMessages.getString(PKG, "SalesforceBulkInput.Error.MissingColumn", column));
      }
    }
    for (int r = 1; r < table.size(); r++) {
      List<String> source = table.get(r);
      Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      for (int c = 0; c < data.columnNames.size(); c++) {
        int at = index.get(data.columnNames.get(c));
        String text = at < source.size() ? source.get(at) : "";
        IValueMeta target = data.targets.get(c);
        row[c] = SalesforceBulkValues.parse(target, text);
      }
      if (data.jobIdIndex >= 0) {
        row[data.jobIdIndex] = data.jobId;
      }
      rows.add(row);
    }
    return rows;
  }

  private static long longValue(String text) {
    if (Utils.isEmpty(text)) {
      return 0;
    }
    try {
      return Long.parseLong(text.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public void dispose() {
    if (data.openJobId != null) {
      SalesforceBulkRuntime.abort(data.client, data.openJobId, true);
      data.openJobId = null;
    }
    if (data.client != null) {
      data.client.close();
      data.client = null;
    }
    super.dispose();
  }
}
