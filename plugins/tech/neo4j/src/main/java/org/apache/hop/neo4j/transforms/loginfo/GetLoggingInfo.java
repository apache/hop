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
 *
 */

package org.apache.hop.neo4j.transforms.loginfo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.neo4j.logging.Defaults;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

/** Get information from the System or the supervising pipeline. */
public class GetLoggingInfo extends BaseTransform<GetLoggingInfoMeta, GetLoggingInfoData> {

  public static final String CONST_UNABLE_TO_FIND_LOGGING_NEO_4_J_CONNECTION_VARIABLE =
      "Unable to find logging Neo4j connection (variable ";
  public static final String CONST_STATUS = "status";

  public GetLoggingInfo(
      TransformMeta transformMeta,
      GetLoggingInfoMeta meta,
      GetLoggingInfoData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private Object[] getLoggingInfo(IRowMeta inputRowMeta, Object[] inputRowData) throws Exception {
    Object[] row = new Object[data.outputRowMeta.size()];
    for (int i = 0; i < inputRowMeta.size(); i++) {
      row[i] = inputRowData[i]; // no data is changed, clone is not needed here.
    }
    for (int i = 0, index = inputRowMeta.size(); i < meta.getFields().size(); i++, index++) {
      Calendar cal;

      int argnr = 0;

      String argument = meta.getFields().get(i).getFieldArgument();
      if (StringUtils.isEmpty(argument)) {
        argument = getPipeline().getPipelineMeta().getName();
      } else {
        argument = resolve(argument);
      }

      switch (GetLoggingInfoTypes.getTypeFromString(meta.getFields().get(i).getFieldType())) {
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM:
          {
            Date previousSuccess = getPreviousPipelineSuccess(argument);
            if (previousSuccess == null) {
              previousSuccess = Const.MIN_DATE;
            }
            row[index] = previousSuccess;
          }
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_DATE_TO:
          row[index] = getPipeline().getExecutionStartDate();
          break;
        case TYPE_SYSTEM_INFO_WORKFLOW_DATE_FROM:
          {
            Date previousSuccess = getPreviousWorkflowSuccess(argument);
            if (previousSuccess == null) {
              previousSuccess = Const.MIN_DATE;
            }
            row[index] = previousSuccess;
          }
          break;
        case TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO:
          row[index] = getPipeline().getExecutionStartDate();
          break;

        case TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_EXECUTION_DATE:
          row[index] = getPreviousPipelineExecution(argument);
          break;
        case TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_SUCCESS_DATE:
          row[index] = getPreviousPipelineSuccess(argument);
          break;
        case TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_EXECUTION_DATE:
          row[index] = getPreviousWorkflowExecution(argument);
          break;
        case TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_SUCCESS_DATE:
          row[index] = getPreviousWorkflowSuccess(argument);
          break;

        default:
          break;
      }
    }

    return row;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row;
    if (data.readsRows) {
      row = getRow();
      if (row == null) {
        setOutputDone();
        return false;
      }

      if (first) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }

    } else {
      row = new Object[] {}; // empty row
      incrementLinesRead();

      if (first) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }
    }

    IRowMeta imeta = getInputRowMeta();
    if (imeta == null) {
      imeta = new RowMeta();
      this.setInputRowMeta(imeta);
    }

    try {
      row = getLoggingInfo(imeta, row);
    } catch (Exception e) {
      throw new HopException("Error getting Neo4j logging information", e);
    }

    if (isRowLevel()) {
      logRowlevel("System info returned: " + data.outputRowMeta.getString(row));
    }

    putRow(data.outputRowMeta, row);

    if (!data.readsRows) {
      // Just one row and then stop!
      setOutputDone();
      return false;
    }

    return true;
  }

  @Override
  public boolean init() {
    if (super.init()) {
      data.readsRows = false;
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms(getTransformMeta());
      if (previous != null && !previous.isEmpty()) {
        data.readsRows = true;
      }

      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    super.dispose();
  }

  private Date getPreviousPipelineExecution(String pipelineName) throws Exception {

    final NeoConnection connection =
        LoggingCore.getConnection(getPipeline().getMetadataProvider(), getPipeline());
    if (connection == null) {
      throw new HopException(
          CONST_UNABLE_TO_FIND_LOGGING_NEO_4_J_CONNECTION_VARIABLE
              + Defaults.NEO4J_LOGGING_CONNECTION
              + ")");
    }

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("type", "PIPELINE");
    parameters.put("name", pipelineName);
    parameters.put(CONST_STATUS, Pipeline.STRING_FINISHED);

    String cypher =
        "MATCH(e:Execution { type: $type, name : $name }) "
            + "WHERE e.status = $status "
            + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
            + "ORDER BY startDate DESC "
            + "LIMIT 1 ";

    return getResultStartDate(getLogChannel(), connection, cypher, parameters);
  }

  private Date getPreviousPipelineSuccess(String pipelineName) throws Exception {

    final NeoConnection connection =
        LoggingCore.getConnection(getPipeline().getMetadataProvider(), getPipeline());
    if (connection == null) {
      throw new HopException(
          CONST_UNABLE_TO_FIND_LOGGING_NEO_4_J_CONNECTION_VARIABLE
              + Defaults.NEO4J_LOGGING_CONNECTION
              + ")");
    }

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("type", "TRANS");
    parameters.put("name", pipelineName);
    parameters.put(CONST_STATUS, Pipeline.STRING_FINISHED);

    String cypher =
        "MATCH(e:Execution { type: $type, name : $name }) "
            + "WHERE e.errors = 0 "
            + "  AND e.status = $status "
            + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
            + "ORDER BY startDate DESC "
            + "LIMIT 1 ";

    return getResultStartDate(getLogChannel(), connection, cypher, parameters);
  }

  private Date getPreviousWorkflowExecution(String jobName) throws Exception {

    final NeoConnection connection =
        LoggingCore.getConnection(getPipeline().getMetadataProvider(), getPipeline());
    if (connection == null) {
      throw new HopException(
          CONST_UNABLE_TO_FIND_LOGGING_NEO_4_J_CONNECTION_VARIABLE
              + Defaults.NEO4J_LOGGING_CONNECTION
              + ")");
    }

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("type", "JOB");
    parameters.put("workflow", jobName);
    parameters.put(CONST_STATUS, Pipeline.STRING_FINISHED);

    String cypher =
        "MATCH(e:Execution { type: $type, name : $job }) "
            + "WHERE e.status = $status "
            + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
            + "ORDER BY startDate DESC "
            + "LIMIT 1 ";

    return getResultStartDate(getLogChannel(), connection, cypher, parameters);
  }

  private Date getPreviousWorkflowSuccess(String jobName) throws Exception {

    final NeoConnection connection =
        LoggingCore.getConnection(getPipeline().getMetadataProvider(), getPipeline());
    if (connection == null) {
      throw new HopException(
          CONST_UNABLE_TO_FIND_LOGGING_NEO_4_J_CONNECTION_VARIABLE
              + Defaults.NEO4J_LOGGING_CONNECTION
              + ")");
    }

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("type", "JOB");
    parameters.put("workflow", jobName);
    parameters.put(CONST_STATUS, Pipeline.STRING_FINISHED);

    String cypher =
        "MATCH(e:Execution { type: $type, name : $job }) "
            + "WHERE e.errors = 0 "
            + "  AND e.status = $status "
            + "RETURN e.name AS Name, e.executionStart AS startDate, e.errors AS errors, e.id AS id "
            + "ORDER BY startDate DESC "
            + "LIMIT 1 ";

    return getResultStartDate(getLogChannel(), connection, cypher, parameters);
  }

  private Date getResultStartDate(
      ILogChannel log, NeoConnection connection, String cypher, Map<String, Object> parameters)
      throws Exception {
    return LoggingCore.executeCypher(
        log,
        this,
        connection,
        cypher,
        parameters,
        result -> {
          try {
            return getResultDate(result, "startDate");
          } catch (ParseException e) {
            throw new RuntimeException("Unable to get start date with cypher : " + cypher, e);
          }
        });
  }

  private Date getResultDate(Result result, String startDate) throws ParseException {
    // One row, get it
    //
    if (result.hasNext()) {
      Record record = result.next();
      String string = record.get("startDate").asString(); // Dates in logging are in String formats
      return new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss").parse(string);
    }

    return null;
  }
}
