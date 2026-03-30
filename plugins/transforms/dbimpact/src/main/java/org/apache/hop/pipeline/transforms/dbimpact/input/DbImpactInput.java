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

package org.apache.hop.pipeline.transforms.dbimpact.input;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;

/** Generates a number of (empty or the same) rows */
public class DbImpactInput extends BaseTransform<DbImpactInputMeta, DbImpactInputData> {
  public DbImpactInput(
      TransformMeta transformMeta,
      DbImpactInputMeta meta,
      DbImpactInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] inputRow = getRow();
    if (inputRow == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // Calculate the output row meta-data
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.pipelineFileType = new HopPipelineFileType<>();
      data.workflowFileType = new HopWorkflowFileType<>();
    }
    String fieldName = resolve(meta.getFileNameField());
    String fileName = getInputRowMeta().getString(inputRow, fieldName, null);

    if (StringUtils.isNotEmpty(fileName)) {
      List<DatabaseImpact> impacts = new ArrayList<>();
      try {
        if (data.pipelineFileType.isHandledBy(fileName, false)) {
          extractPipelineLineage(fileName, impacts);
        }
        if (data.workflowFileType.isHandledBy(fileName, false)) {
          extractWorkflowLineage(fileName, impacts);
        }
        outputDbImpact(fileName, impacts);
      } catch (Throwable e) {
        if (getTransformMeta().isDoingErrorHandling()) {
          putError(
              getInputRowMeta(),
              inputRow,
              1,
              Const.getSimpleStackTrace(e),
              fieldName,
              "DBImpact001");
        } else {
          throw e;
        }
      }
    }

    return true;
  }

  private void outputDbImpact(String fileName, List<DatabaseImpact> impacts)
      throws HopTransformException {
    for (DatabaseImpact impact : impacts) {
      Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int index = 0;
      row[index++] = impact.getTypeDesc();
      row[index++] = impact.getPipelineName();
      row[index++] = fileName;
      row[index++] = impact.getTransformName();
      row[index++] = impact.getDatabaseName();
      row[index++] = impact.getTable();
      row[index++] = impact.getField();
      row[index++] = impact.getValue();
      row[index++] = impact.getValueOrigin();
      row[index++] = impact.getSql();
      row[index] = impact.getRemark();
      putRow(data.outputRowMeta, row);
    }
  }

  private void extractPipelineLineage(String fileName, List<DatabaseImpact> impact)
      throws HopException {
    try {
      PipelineMeta pipelineMeta = new PipelineMeta(fileName, metadataProvider, this);
      incrementLinesInput();
      pipelineMeta.analyseImpact(this, impact, null);
    } catch (Throwable e) {
      throw new HopException("Error extracting lineage information from pipeline " + fileName, e);
    }
  }

  private void extractWorkflowLineage(String fileName, List<DatabaseImpact> impact) {
    logBasic("DB Impact analyses of workflow '" + fileName + "' is not supported yet.");
  }
}
