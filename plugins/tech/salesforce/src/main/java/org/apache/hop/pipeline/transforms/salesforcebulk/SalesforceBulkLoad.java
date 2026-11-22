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
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Writes incoming rows with one or more Bulk API 2.0 ingest jobs. */
public class SalesforceBulkLoad
    extends BaseTransform<SalesforceBulkLoadMeta, SalesforceBulkLoadData> {
  private static final Class<?> PKG = SalesforceBulkLoad.class;

  public SalesforceBulkLoad(
      TransformMeta transformMeta,
      SalesforceBulkLoadMeta meta,
      SalesforceBulkLoadData data,
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
      return true;
    }
    try {
      data.client =
          SalesforceBulkRuntime.open(this, metadataProvider, getLogChannel(), meta.getConnection());
      return true;
    } catch (Exception e) {
      logError(e.getMessage(), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (first) {
      first = false;
      if (row != null) {
        prepare();
      }
    }
    if (row == null) {
      flush();
      setOutputDone();
      return false;
    }
    List<String> values = new ArrayList<>(data.headers.size());
    for (int i = 0; i < data.streamIndexes.length; i++) {
      values.add(
          SalesforceBulkValues.format(
              data.inputRowMeta.getValueMeta(data.streamIndexes[i]),
              row[data.streamIndexes[i]],
              data.nullAsNa));
    }
    int additional = rowBytes(values);
    if (!data.buffer.isEmpty() && data.bufferedBytes + additional > data.maxBytes) {
      flush();
    }
    data.buffer.add(new SalesforceBulkRowMatcher.Pending(row, values));
    data.bufferedBytes += additional;
    return true;
  }

  private void prepare() throws HopException {
    data.inputRowMeta = getInputRowMeta();
    if (data.inputRowMeta == null) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.NoInput"));
    }
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    List<SalesforceBulkField> fields = meta.getFields() == null ? List.of() : meta.getFields();
    if (fields.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.NoFields"));
    }
    data.streamIndexes = new int[fields.size()];
    data.headers = new ArrayList<>();
    SalesforceBulkOperation operation =
        meta.getOperation() == null ? SalesforceBulkOperation.INSERT : meta.getOperation();
    String externalId = resolve(meta.getExternalIdField());
    int key = -1;
    for (int i = 0; i < fields.size(); i++) {
      String stream = resolve(fields.get(i).getStreamField());
      String salesforce = resolve(fields.get(i).getSalesforceField());
      if (Utils.isEmpty(stream) || Utils.isEmpty(salesforce)) {
        throw new HopException(BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.BlankField"));
      }
      int index = data.inputRowMeta.indexOfValue(stream);
      if (index < 0) {
        throw new HopException(
            BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.MissingField", stream));
      }
      data.streamIndexes[i] = index;
      data.headers.add(salesforce);
      if (operation.isDelete() && "Id".equals(salesforce)) {
        key = i;
      }
      if (operation == SalesforceBulkOperation.UPDATE && "Id".equals(salesforce)) {
        key = i;
      }
      if (operation == SalesforceBulkOperation.UPSERT && salesforce.equals(externalId)) {
        key = i;
      }
    }
    if (operation == SalesforceBulkOperation.UPSERT && Utils.isEmpty(externalId)) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.NoExternalId"));
    }
    if (key < 0 && operation != SalesforceBulkOperation.INSERT) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              operation == SalesforceBulkOperation.UPSERT
                  ? "SalesforceBulkLoad.Error.ExternalIdNotMapped"
                  : "SalesforceBulkLoad.Error.IdNotMapped"));
    }
    data.keyIndexes =
        operation == SalesforceBulkOperation.INSERT ? allColumns(fields.size()) : new int[] {key};
    data.nullAsNa =
        operation == SalesforceBulkOperation.UPDATE || operation == SalesforceBulkOperation.UPSERT;
    data.timeoutMillis = SalesforceBulkRuntime.secondsToMillis(resolve(meta.getTimeout()), 3600);
    data.pollMillis = SalesforceBulkRuntime.secondsToMillis(resolve(meta.getPollInterval()), 10);
    long maxBytes = longValue(resolve(meta.getMaxBytes()));
    data.maxBytes = maxBytes <= 0 ? 104_857_600L : maxBytes;
    data.request =
        new SalesforceBulkIngestRequest(
            resolve(meta.getSalesforceObject()),
            operation,
            externalId,
            meta.getLineEnding() == null ? SalesforceBulkLineEnding.LF : meta.getLineEnding(),
            meta.getDelimiter() == null ? SalesforceBulkDelimiter.COMMA : meta.getDelimiter(),
            meta.getConcurrency() == null
                ? SalesforceBulkConcurrency.PARALLEL
                : meta.getConcurrency());
    if (Utils.isEmpty(data.request.getSalesforceObject())) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.NoObject"));
    }
    data.idIndex = indexOf(meta.getSalesforceIdField());
    data.jobIdIndex = indexOf(meta.getJobIdField());
    data.successIndex = indexOf(meta.getSuccessField());
    data.errorIndex = indexOf(meta.getErrorField());
    SalesforceBulkCsv.Builder headerOnly =
        new SalesforceBulkCsv.Builder(
            data.headers, data.request.getDelimiter(), data.request.getLineEnding());
    data.bufferedBytes = headerOnly.sizeBytes();
  }

  private int indexOf(String fieldName) {
    String resolved = resolve(fieldName);
    if (Utils.isEmpty(resolved)) {
      return -1;
    }
    return data.outputRowMeta.indexOfValue(resolved);
  }

  private static int[] allColumns(int size) {
    int[] indexes = new int[size];
    for (int i = 0; i < size; i++) {
      indexes[i] = i;
    }
    return indexes;
  }

  private int rowBytes(List<String> values) {
    SalesforceBulkCsv.Builder one =
        new SalesforceBulkCsv.Builder(
            data.headers, data.request.getDelimiter(), data.request.getLineEnding());
    int header = one.sizeBytes();
    one.add(values);
    return one.sizeBytes() - header;
  }

  private void flush() throws HopException {
    if (data.buffer.isEmpty()) {
      return;
    }
    SalesforceBulkCsv.Builder csv =
        new SalesforceBulkCsv.Builder(
            data.headers, data.request.getDelimiter(), data.request.getLineEnding());
    for (SalesforceBulkRowMatcher.Pending row : data.buffer) {
      csv.add(row.getValues());
    }
    SalesforceBulkJob created = data.client.createIngestJob(data.request);
    data.openJobId = created.getId();
    data.client.uploadIngestData(created.getId(), csv.toByteArray());
    data.client.completeIngestUpload(created.getId());
    SalesforceBulkJob finished =
        SalesforceBulkRuntime.poll(
            data.client,
            created.getId(),
            false,
            data.timeoutMillis,
            data.pollMillis,
            this::isStopped);
    data.openJobId = null;
    if (finished.isAborted()) {
      throw new HopException(
          BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.Aborted", created.getId()));
    }
    logBasic(
        BaseMessages.getString(
            PKG,
            "SalesforceBulkLoad.Log.JobFinished",
            created.getId(),
            finished.getState(),
            String.valueOf(finished.getRecordsProcessed()),
            String.valueOf(finished.getRecordsFailed())));
    SalesforceBulkRowMatcher.match(
        data.buffer,
        data.headers,
        data.keyIndexes,
        data.client.ingestResults(created.getId(), false),
        data.request.getDelimiter(),
        false);
    SalesforceBulkRowMatcher.match(
        data.buffer,
        data.headers,
        data.keyIndexes,
        data.client.ingestResults(created.getId(), true),
        data.request.getDelimiter(),
        true);
    for (SalesforceBulkRowMatcher.Pending row : data.buffer) {
      if (!row.isMatched()) {
        row.setError(
            Utils.isEmpty(finished.getErrorMessage())
                ? BaseMessages.getString(PKG, "SalesforceBulkLoad.Error.NoResult")
                : finished.getErrorMessage());
      }
      emit(row, created.getId());
    }
    data.buffer.clear();
    SalesforceBulkCsv.Builder headerOnly =
        new SalesforceBulkCsv.Builder(
            data.headers, data.request.getDelimiter(), data.request.getLineEnding());
    data.bufferedBytes = headerOnly.sizeBytes();
  }

  private void emit(SalesforceBulkRowMatcher.Pending row, String jobId) throws HopException {
    boolean success = row.isMatched() && Utils.isEmpty(row.getError());
    if (!success && getTransformMeta().isDoingErrorHandling()) {
      putError(
          data.inputRowMeta,
          row.getInputRow(),
          1,
          Utils.isEmpty(row.getError()) ? "failed" : row.getError(),
          null,
          "SalesforceBulkLoad001");
      return;
    }
    Object[] output = RowDataUtil.resizeArray(row.getInputRow(), data.outputRowMeta.size());
    if (data.idIndex >= 0) {
      output[data.idIndex] = row.getSalesforceId();
    }
    if (data.jobIdIndex >= 0) {
      output[data.jobIdIndex] = jobId;
    }
    if (data.successIndex >= 0) {
      output[data.successIndex] = success;
    }
    if (data.errorIndex >= 0) {
      output[data.errorIndex] = success ? null : row.getError();
    }
    putRow(data.outputRowMeta, output);
  }

  private static long longValue(String text) {
    if (Utils.isEmpty(text)) {
      return -1;
    }
    try {
      return Long.parseLong(text.trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  @Override
  public void dispose() {
    if (data.openJobId != null) {
      SalesforceBulkRuntime.abort(data.client, data.openJobId, false);
      data.openJobId = null;
    }
    if (data.client != null) {
      data.client.close();
      data.client = null;
    }
    super.dispose();
  }
}
