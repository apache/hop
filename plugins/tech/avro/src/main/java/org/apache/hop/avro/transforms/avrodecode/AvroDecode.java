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

package org.apache.hop.avro.transforms.avrodecode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.Map;

public class AvroDecode extends BaseTransform<AvroDecodeMeta, AvroDecodeData> {
  public AvroDecode(
      TransformMeta transformMeta,
      AvroDecodeMeta meta,
      AvroDecodeData data,
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

      String sourceFieldName = resolve(meta.getSourceFieldName());
      data.inputIndex = getInputRowMeta().indexOfValue(sourceFieldName);
      if (data.inputIndex < 0) {
        throw new HopException("Unable to find Avro source field: " + sourceFieldName);
      }
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.inputIndex);
      if (!(valueMeta instanceof ValueMetaAvroRecord)) {
        throw new HopException(
            "We can only decode Avro data types and field "
                + sourceFieldName
                + " is of type "
                + valueMeta.getTypeDesc());
      }
      data.avroValueMeta = (ValueMetaAvroRecord) valueMeta;
    }

    GenericRecord genericRecord = data.avroValueMeta.getGenericRecord(row[data.inputIndex]);
    if (genericRecord != null) {
      Schema schema = genericRecord.getSchema();

      int idx = getInputRowMeta().size();
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      for (TargetField targetField : meta.getTargetFields()) {
        String avroFieldName = resolve(targetField.getSourceField());
        Schema.Field field = schema.getField(avroFieldName);
        Object avroValue = genericRecord.get(avroFieldName);
        Object hopValue = getStandardHopObject(field, avroValue);

        // Did the user ask for a different data type?
        //
        IValueMeta standardValueMeta =
            ValueMetaFactory.createValueMeta("standard", getStandardHopType(field));
        IValueMeta targetValueMeta = targetField.createTargetValueMeta(this);
        standardValueMeta.setConversionMask(targetValueMeta.getConversionMask());

        // Convert the data if needed
        //
        outputRow[idx++] = targetValueMeta.convertData(standardValueMeta, hopValue);
      }

      // Pass the row along
      //
      putRow(data.outputRowMeta, outputRow);
    }

    return true;
  }

  public static final int getStandardHopType(Schema.Field field) throws HopException {
    Schema.Type type = field.schema().getType();
    int basicType = getBasicType(type);
    if (basicType != 0) {
      return basicType;
    }
    if (type == Schema.Type.UNION) {
      // Often we have a union of a type and "null"
      for (Schema subType : field.schema().getTypes()) {
        if (subType.getType() != Schema.Type.NULL) {
          basicType = getBasicType(subType.getType());
          if (basicType != 0) {
            return basicType;
          }
        }
      }
    }

    throw new HopException("Schema type '" + type + " isn't handled");
  }

  private static int getBasicType(Schema.Type type) {
    switch (type) {
      case BYTES:
        return IValueMeta.TYPE_BINARY;
      case INT:
      case LONG:
        return IValueMeta.TYPE_INTEGER;
      case FLOAT:
      case DOUBLE:
        return IValueMeta.TYPE_NUMBER;
      case BOOLEAN:
        return IValueMeta.TYPE_BOOLEAN;
      case STRING:
      case RECORD:
      case ARRAY:
      case MAP:
      case FIXED:
        return IValueMeta.TYPE_STRING;
      default:
        return IValueMeta.TYPE_NONE;
    }
  }

  public static final Object getStandardHopObject(Schema.Field field, Object avroValue)
      throws HopException {
    Object hopValue;
    if (avroValue == null) {
      hopValue = null;
    } else {
      Schema.Type type = field.schema().getType();
      switch (type) {
        case NULL:
          hopValue = null;
          break;
        case ENUM:
          hopValue = avroValue.toString();
          break;
        case STRING:
          hopValue = ((Utf8) avroValue).toString();
          break;
        case BYTES:
        case LONG:
        case DOUBLE:
          hopValue = avroValue;
          break;
        case INT:
          hopValue = Long.valueOf((int) avroValue);
          break;
        case FLOAT:
          hopValue = Double.valueOf((float) avroValue);
          break;
        case BOOLEAN:
          hopValue = Boolean.valueOf((boolean) avroValue);
          break;
        case RECORD:
          GenericData.Record record = (GenericData.Record) avroValue;
          hopValue = record.toString();
          break;
        case ARRAY:
          GenericData.Array array = (GenericData.Array) avroValue;
          hopValue = array.toString();
          break;
        case MAP:
          Map<Utf8, Object> map = (Map<Utf8, Object>) avroValue;
          hopValue = map.toString();
          break;
        case UNION:
          // This value can be a set of possible values...
          //
          if (avroValue instanceof Long
              || avroValue instanceof Double
              || avroValue instanceof String
              || avroValue instanceof Boolean
              || avroValue instanceof byte[]) {
            hopValue = avroValue;
          } else if (avroValue instanceof Integer) {
            hopValue = Integer.valueOf((int) avroValue).longValue();
          } else {
            hopValue = avroValue.toString();
          }
          break;
        case FIXED:
          GenericContainer container = (GenericContainer) avroValue;
          hopValue = container.toString();
          break;
        default:
          throw new HopException("Schema type " + type + " isn't handled yet");
      }
    }
    return hopValue;
  }
}
