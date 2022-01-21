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
 *
 */

package org.apache.hop.avro.transforms.avroencode;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;

public class AvroEncode extends BaseTransform<AvroEncodeMeta, AvroEncodeData>
    implements ITransform<AvroEncodeMeta, AvroEncodeData> {
  public AvroEncode(
      TransformMeta transformMeta,
      AvroEncodeMeta meta,
      AvroEncodeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;

      // The output row
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.sourceFieldIndexes = new ArrayList<>();

      // Index the selected fields
      //
      for (SourceField field : meta.getSourceFields()) {
        int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
        if (index<0) {
          throw new HopException("Unable to find input field "+field.getSourceFieldName());
        }
        IValueMeta valueMeta = getInputRowMeta().getValueMeta(index);
        data.sourceFieldIndexes.add(index);
      }

      // Build the Avro schema.
      //
      data.avroSchema = meta.createAvroSchema(getInputRowMeta());
    }

    // Create a new generic record.
    //
    GenericRecord genericRecord = new GenericData.Record(data.avroSchema);

    // Add the field data...
    //
    for (int i=0;i<meta.getSourceFields().size();i++) {
      SourceField field = meta.getSourceFields().get(i);
      int index = data.sourceFieldIndexes.get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(index);
      Object value = row[index];
      // If needed convert to the actual data type, not lazy or indexed
      //
      Object nativeValue = valueMeta.getNativeDataType(value);

      // Store this object in the Avro record
      //
      genericRecord.put(field.calculateTargetFieldName(), nativeValue);
    }

    // Add the record to the input row and send it on its way...
    //
    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    outputRow[getInputRowMeta().size()] = genericRecord;
    putRow(data.outputRowMeta, outputRow);

    return true;
  }
}
