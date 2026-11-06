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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
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

      // The target fields don't change during the run: resolve their names and build their value
      // metadata once. Creating value metadata scans the plugin registry.
      List<TargetField> targetFields = meta.getTargetFields();
      data.avroFieldNames = new String[targetFields.size()];
      data.targetValueMetas = new IValueMeta[targetFields.size()];
      data.standardValueMetas = new ArrayList<>(targetFields.size());
      for (int i = 0; i < targetFields.size(); i++) {
        TargetField targetField = targetFields.get(i);
        data.avroFieldNames[i] = resolve(targetField.getSourceField());
        data.targetValueMetas[i] = targetField.createTargetValueMeta(this);
        data.standardValueMetas.add(new HashMap<>());
      }
    }

    GenericRecord genericRecord = data.avroValueMeta.getGenericRecord(row[data.inputIndex]);
    if (genericRecord != null) {
      Schema schema = genericRecord.getSchema();

      int idx = getInputRowMeta().size();
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      for (int i = 0; i < data.avroFieldNames.length; i++) {
        String avroFieldName = data.avroFieldNames[i];
        Schema.Field field = schema.getField(avroFieldName);
        Object avroValue = genericRecord.get(avroFieldName);
        Object hopValue = getStandardHopObject(field, avroValue);

        // Did the user ask for a different data type?
        //
        IValueMeta targetValueMeta = data.targetValueMetas[i];
        IValueMeta standardValueMeta = getStandardValueMeta(i, getStandardHopType(field));

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

  /**
   * The value metadata of the Hop type an Avro field arrives as, for one target field, with the
   * target's conversion mask. The type comes from each record's schema and can differ between
   * records, so it is kept per type.
   */
  private IValueMeta getStandardValueMeta(int targetIndex, int hopType) throws HopException {
    Map<Integer, IValueMeta> byType = data.standardValueMetas.get(targetIndex);
    IValueMeta standardValueMeta = byType.get(hopType);
    if (standardValueMeta == null) {
      standardValueMeta = ValueMetaFactory.createValueMeta("standard", hopType);
      standardValueMeta.setConversionMask(data.targetValueMetas[targetIndex].getConversionMask());
      byType.put(hopType, standardValueMeta);
    }
    return standardValueMeta;
  }

  public static int getStandardHopType(Schema.Field field) throws HopException {
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
    return switch (type) {
      case BYTES -> IValueMeta.TYPE_BINARY;
      case INT, LONG -> IValueMeta.TYPE_INTEGER;
      case FLOAT, DOUBLE -> IValueMeta.TYPE_NUMBER;
      case BOOLEAN -> IValueMeta.TYPE_BOOLEAN;
      case STRING, RECORD, ARRAY, MAP, FIXED -> IValueMeta.TYPE_STRING;
      default -> IValueMeta.TYPE_NONE;
    };
  }

  @VisibleForTesting
  public static Object getStandardHopObject(Schema.Field field, Object avroValue) {
    if (avroValue == null) {
      return null;
    }

    return convertBySchemaType(field.schema().getType(), avroValue);
  }

  private static Object convertBySchemaType(Schema.Type type, Object avroValue) {
    return switch (type) {
      case NULL -> null;
      case ENUM, RECORD, ARRAY, MAP, FIXED -> avroValue.toString();
      case STRING -> ((Utf8) avroValue).toString();
      case BYTES, LONG, DOUBLE -> avroValue;
      case INT -> (long) (int) avroValue;
      case FLOAT -> (double) (float) avroValue;
      case BOOLEAN -> (boolean) avroValue;
      case UNION -> union(avroValue);
    };
  }

  private static Object union(Object avroValue) {
    return switch (avroValue) {
      case Long l -> l;
      case Double d -> d;
      case String s -> s;
      case Boolean b -> b;
      case byte[] b -> b;
      case Float f -> (double) f;
      case Integer i -> i.longValue();
      default -> avroValue.toString();
    };
  }
}
