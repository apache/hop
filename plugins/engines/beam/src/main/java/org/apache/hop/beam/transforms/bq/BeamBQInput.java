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

package org.apache.hop.beam.transforms.bq;

import com.google.cloud.bigquery.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Date;
import java.util.StringJoiner;
import java.util.UUID;

public class BeamBQInput extends BaseTransform<BeamBQInputMeta, BeamBQInputData> {

  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hashtables etc.
   * @param copyNr The copynumber for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public BeamBQInput(
      TransformMeta transformMeta,
      BeamBQInputMeta meta,
      BeamBQInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    data.bigquery = BigQueryOptions.getDefaultInstance().getService();
    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    // Calculate output fields...
    //
    data.outputRowMeta = new RowMeta();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    QueryJobConfiguration queryConfig;

    String query;
    if (StringUtils.isNotEmpty(meta.getQuery())) {
      query = resolve(meta.getQuery());
    } else {
      // Query the table...

      StringJoiner joiner = new StringJoiner(",");

      for (int i = 0; i < meta.getFields().size(); i++)
        joiner.add(meta.getFields().get(i).getName());

      String bgFields = joiner.toString();

      query =
          "SELECT "
              + bgFields
              + " FROM "
              + resolve(meta.getDatasetId() + "." + resolve(meta.getTableId()));
    }

    queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

    data.jobId = JobId.of(UUID.randomUUID().toString());
    data.queryJob =
        data.bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(data.jobId).build());

    // Wait for the query to complete.
    try {
      data.queryJob = data.queryJob.waitFor();
    } catch (InterruptedException e) {
      throw new HopException("BigQuery job was interrupted", e);
    }

    // Check for errors
    if (data.queryJob == null) {
      throw new HopException("Job no longer exists");
    } else if (data.queryJob.getStatus().getError() != null) {
      // We can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new HopException(
          "Error in BigQuery job: " + data.queryJob.getStatus().getError().toString());
    }

    // Pre-calculate field types...
    //
    int[] fieldTypes = new int[meta.getFields().size()];
    for (int i = 0; i < fieldTypes.length; i++) {
      BQField field = meta.getFields().get(i);
      fieldTypes[i] = ValueMetaFactory.getIdForValueMeta(field.getHopType());
      if (fieldTypes[i] == IValueMeta.TYPE_NONE) {
        throw new HopException("Unable to find Hop data type for return field: " + field);
      }
    }

    // Get the data
    //
    try {
      TableResult result = data.queryJob.getQueryResults();

      // Iterate over the results
      //
      for (FieldValueList row : result.iterateAll()) {
        incrementLinesInput();

        // Allocate a new Hop row...
        //
        Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
        int outputIndex = 0;

        // Retrieve the specified fields...
        //
        for (int i = 0; i < meta.getFields().size(); i++) {
          BQField field = meta.getFields().get(i);
          FieldValue fieldValue = row.get(field.getName());
          Object hopValue = null;
          if (!fieldValue.isNull()) {
            switch (fieldTypes[i]) {
              case IValueMeta.TYPE_STRING:
                hopValue = fieldValue.getStringValue();
                break;
              case IValueMeta.TYPE_INTEGER:
                hopValue = fieldValue.getLongValue();
                break;
              case IValueMeta.TYPE_DATE:
                hopValue = new Date(fieldValue.getTimestampValue());
                break;
              case IValueMeta.TYPE_BOOLEAN:
                hopValue = fieldValue.getBooleanValue();
                break;
              case IValueMeta.TYPE_NUMBER:
                hopValue = fieldValue.getDoubleValue();
                break;
              case IValueMeta.TYPE_BINARY:
                hopValue = fieldValue.getBytesValue();
                break;
              case IValueMeta.TYPE_BIGNUMBER:
                hopValue = fieldValue.getNumericValue();
                break;
              default:
                throw new HopException(
                    "Converting BigQuery data to Hop type "
                        + field.getHopType()
                        + " isn't supported yet");
            }
          }
          outputRow[outputIndex++] = hopValue;
        }
        // Pass the row along the way...
        //
        putRow(data.outputRowMeta, outputRow);

        if (isStopped()) {
          setOutputDone();
          return false;
        }
      }

    } catch (InterruptedException e) {
      throw new HopException("BigQuery data retrieval was interrupted", e);
    }

    setOutputDone();
    return false;
  }
}
