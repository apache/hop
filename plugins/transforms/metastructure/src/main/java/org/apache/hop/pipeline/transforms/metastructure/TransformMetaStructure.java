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

/**
 * Return the structure of the stream
 *
 * @author Ingo Klose
 * @since 22-april-2008
 */
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

  public boolean init() {
    if (super.init()) {
      // Add init code here.
      data.rowCount = 0;
      return true;
    }
    return false;
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    Object[] metastructureRow = null;

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
      metastructureRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

      IRowMeta row = getInputRowMeta().clone();

      for (int i = 0; i < row.size(); i++) {

        IValueMeta v = row.getValueMeta(i);

        IValueMeta v_position = data.outputRowMeta.getValueMeta(0);
        metastructureRow =
            RowDataUtil.addValueData(
                metastructureRow, 0, v_position.convertDataCompatible(v_position, new Long(i + 1)));

        metastructureRow = RowDataUtil.addValueData(metastructureRow, 1, v.getName());
        metastructureRow = RowDataUtil.addValueData(metastructureRow, 2, v.getComments());
        metastructureRow = RowDataUtil.addValueData(metastructureRow, 3, v.getTypeDesc());

        IValueMeta v_length = data.outputRowMeta.getValueMeta(4);
        metastructureRow =
            RowDataUtil.addValueData(
                metastructureRow,
                4,
                v_length.convertDataCompatible(v_length, new Long(v.getLength())));

        IValueMeta v_precision = data.outputRowMeta.getValueMeta(5);
        metastructureRow =
            RowDataUtil.addValueData(
                metastructureRow,
                5,
                v_precision.convertDataCompatible(v_precision, new Long(v.getPrecision())));

        metastructureRow = RowDataUtil.addValueData(metastructureRow, 6, v.getOrigin());

        if (meta.isOutputRowcount()) {
          IValueMeta v_rowCount = data.outputRowMeta.getValueMeta(7);
          metastructureRow =
              RowDataUtil.addValueData(
                  metastructureRow,
                  7,
                  v_rowCount.convertDataCompatible(v_rowCount, new Long(data.rowCount)));
        }
        putRow(data.outputRowMeta, metastructureRow.clone());
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
