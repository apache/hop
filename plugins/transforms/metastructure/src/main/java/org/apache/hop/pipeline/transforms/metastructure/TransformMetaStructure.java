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

package org.apache.hop.pipeline.transforms.metastructure;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Return the structure of the stream */
public class TransformMetaStructure
    extends BaseTransform<TransformMetaStructureMeta, TransformMetaStructureData>
    implements ITransform<TransformMetaStructureMeta, TransformMetaStructureData> {

  public TransformMetaStructure(
      TransformMeta transformMeta,
      TransformMetaStructureMeta meta,
      TransformMetaStructureData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (super.init()) {
      // Add init code here.
      data.rowCount = 0;
      return true;
    }
    return false;
  }

  @Override
  public boolean processRow() throws HopException {

    // Get row from input row set & set row busy!
    //
    Object[] r = getRow();

    // initialize
    if (first) {
      first = false;

      // handle empty input
      if (r == null) {
        setOutputDone();
        return false;
      }

      // Create the row metadata for the output rows
      //
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    if (r == null) {
      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

      IRowMeta row = getInputRowMeta().clone();

      for (int i = 0; i < row.size(); i++) {
        int pos = 0;

        IValueMeta v = row.getValueMeta(i);

        if (meta.isIncludePositionField()) {
          IValueMeta vPosition = data.outputRowMeta.getValueMeta(pos);
          outputRow =
              RowDataUtil.addValueData(
                  outputRow,
                  pos++,
                  vPosition.convertDataCompatible(vPosition, Long.valueOf(i + 1)));
        }

        if (meta.isIncludeFieldnameField()) {
          outputRow = RowDataUtil.addValueData(outputRow, pos++, v.getName());
        }

        if (meta.isIncludeCommentsField()) {
          outputRow = RowDataUtil.addValueData(outputRow, pos++, v.getComments());
        }

        if (meta.isIncludeTypeField()) {
          outputRow = RowDataUtil.addValueData(outputRow, pos++, v.getTypeDesc());
        }

        if (meta.isIncludeLengthField()) {
          IValueMeta vLength = data.outputRowMeta.getValueMeta(pos);
          outputRow =
              RowDataUtil.addValueData(
                  outputRow,
                  pos++,
                  vLength.convertDataCompatible(vLength, Long.valueOf(v.getLength())));
        }

        if (meta.isIncludePrecisionField()) {
          IValueMeta vPrecision = data.outputRowMeta.getValueMeta(pos);
          outputRow =
              RowDataUtil.addValueData(
                  outputRow,
                  pos++,
                  vPrecision.convertDataCompatible(vPrecision, Long.valueOf(v.getPrecision())));
        }

        if (meta.isIncludeOriginField()) {
          outputRow = RowDataUtil.addValueData(outputRow, pos++, v.getOrigin());
        }

        if (meta.isOutputRowcount()) {
          IValueMeta vRowCount = data.outputRowMeta.getValueMeta(pos);
          outputRow =
              RowDataUtil.addValueData(
                  outputRow,
                  pos,
                  vRowCount.convertDataCompatible(vRowCount, Long.valueOf(data.rowCount)));
        }
        putRow(data.outputRowMeta, outputRow.clone());
      }

      // We're done, call it a day
      //
      setOutputDone();
      return false;
    }

    data.rowCount++;

    return true;
  }
}
