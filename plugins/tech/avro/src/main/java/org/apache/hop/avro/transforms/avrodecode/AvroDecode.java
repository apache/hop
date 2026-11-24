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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class AvroDecode extends BaseTransform<AvroDecodeMeta, AvroDecodeData> {
  private static final Class<?> PKG = AvroDecodeMeta.class;
  private static final String CONST_ERROR_CODE = "AvroDecode001";
  private static final String CONST_TRAILING_BYTES = "bytes remain after reading the record";

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
      // Configuration failures stop the transform. They are not row errors.
      prepareFirstRow();
    }

    try {
      if (data.decodingBinary) {
        decodeBinaryRow(row);
      } else {
        decodeAvroRecordRow(row);
      }
    } catch (HopException e) {
      return rejectOrThrow(row, e);
    } catch (RuntimeException e) {
      return rejectOrThrow(row, e);
    }

    return true;
  }

  @Override
  public void dispose() {
    // The reused decoder holds the last row's bytes. Drop it with the copy.
    data.reader = null;
    data.decoder = null;
    data.reusedRecord = null;
    data.schema = null;
    super.dispose();
  }

  private void prepareFirstRow() throws HopException {
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    data.sourceFieldName = resolve(meta.getSourceFieldName());
    data.inputIndex = getInputRowMeta().indexOfValue(data.sourceFieldName);
    if (data.inputIndex < 0) {
      throw new HopException("Unable to find Avro source field: " + data.sourceFieldName);
    }
    IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.inputIndex);
    if (valueMeta instanceof ValueMetaAvroRecord avroValueMeta) {
      // The schema file is only for bare binary. An Avro Record carries its own schema.
      data.avroValueMeta = avroValueMeta;
      data.decodingBinary = false;
    } else if (valueMeta.getType() == IValueMeta.TYPE_BINARY
        && !StringUtils.isBlank(meta.getSchemaFileName())) {
      String schemaFileName = resolve(meta.getSchemaFileName());
      if (StringUtils.isBlank(schemaFileName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "AvroDecode.Exception.SchemaFileNameNotSet"));
      }
      data.decodingBinary = true;
      data.sourceValueMeta = valueMeta;
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "AvroDecode.Log.ReadingSchemaFile", schemaFileName));
      }
      data.schema = readSchemaFile(this, meta.getSchemaFileName());
      // One schema for both sides: the bytes have no writer schema of their own, so resolution
      // against a different reader schema is not applied.
      data.reader = new GenericDatumReader<>(data.schema);
    } else {
      throw new HopException(
          "We can only decode Avro data types and field "
              + data.sourceFieldName
              + " is of type "
              + valueMeta.getTypeDesc());
    }

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
    if (data.decodingBinary) {
      for (String avroFieldName : data.avroFieldNames) {
        if (data.schema.getField(avroFieldName) == null) {
          throw new HopException(
              BaseMessages.getString(PKG, "AvroDecode.Exception.FieldNotInSchema", avroFieldName));
        }
      }
    }
  }

  private void decodeAvroRecordRow(Object[] row) throws HopException {
    GenericRecord genericRecord = data.avroValueMeta.getGenericRecord(row[data.inputIndex]);
    if (genericRecord != null) {
      putMappedRow(row, genericRecord);
    }
  }

  private void decodeBinaryRow(Object[] row) throws HopException {
    byte[] bytes = data.sourceValueMeta.getBinary(row[data.inputIndex]);
    if (bytes == null) {
      // A null binary value (a Kafka tombstone, for example) keeps the rest of the row.
      putRow(data.outputRowMeta, RowDataUtil.createResizedCopy(row, data.outputRowMeta.size()));
      return;
    }
    putMappedRow(row, readBinaryRecord(bytes));
  }

  /**
   * Reads one bare Avro record. The schema was parsed in {@link #prepareFirstRow()}, and the reader
   * and decoder are reused across rows of this copy.
   */
  private GenericRecord readBinaryRecord(byte[] bytes) throws HopException {
    try {
      data.decoder = DecoderFactory.get().binaryDecoder(bytes, data.decoder);
      GenericRecord record = data.reader.read(data.reusedRecord, data.decoder);
      if (!data.decoder.isEnd()) {
        throw schemaMismatch(CONST_TRAILING_BYTES);
      }
      data.reusedRecord = record;
      return record;
    } catch (HopException e) {
      data.reusedRecord = null;
      throw e;
    } catch (Exception e) {
      data.reusedRecord = null;
      String detail = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      throw schemaMismatch(detail, e);
    }
  }

  private static HopException schemaMismatch(String detail) {
    return new HopException(
        BaseMessages.getString(PKG, "AvroDecode.Exception.SchemaMismatch", detail));
  }

  private static HopException schemaMismatch(String detail, Exception cause) {
    return new HopException(
        BaseMessages.getString(PKG, "AvroDecode.Exception.SchemaMismatch", detail), cause);
  }

  /**
   * Reads an Avro record schema from a VFS location. Variables in {@code schemaFileName} are
   * resolved. The schema is the writer schema of the bytes.
   */
  public static Schema readSchemaFile(IVariables variables, String schemaFileName)
      throws HopException {
    String resolved = variables.resolve(schemaFileName);
    if (StringUtils.isBlank(resolved)) {
      throw new HopException(
          BaseMessages.getString(PKG, "AvroDecode.Exception.SchemaFileNameNotSet"));
    }
    try (InputStream inputStream = HopVfs.getInputStream(resolved, variables)) {
      Schema schema = new Schema.Parser().parse(inputStream);
      if (schema.getType() != Schema.Type.RECORD) {
        throw new HopException(
            BaseMessages.getString(PKG, "AvroDecode.Exception.SchemaNotRecord", resolved));
      }
      return schema;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "AvroDecode.Exception.ReadingSchemaFile", resolved), e);
    }
  }

  private void putMappedRow(Object[] row, GenericRecord genericRecord) throws HopException {
    Schema schema = genericRecord.getSchema();
    int idx = getInputRowMeta().size();
    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    for (int i = 0; i < data.avroFieldNames.length; i++) {
      String avroFieldName = data.avroFieldNames[i];
      Schema.Field field = schema.getField(avroFieldName);
      Object avroValue = genericRecord.get(avroFieldName);
      Object hopValue = getStandardHopObject(field, avroValue);
      if (data.decodingBinary) {
        // The record is reused, and Avro reuses the byte buffer inside it. Copy before the next
        // row.
        hopValue = detachMutableAvroValue(hopValue);
      }

      // Did the user ask for a different data type?
      //
      IValueMeta targetValueMeta = data.targetValueMetas[i];
      IValueMeta standardValueMeta = getStandardValueMeta(i, getStandardHopType(field));

      // Convert the data if needed
      //
      outputRow[idx++] = targetValueMeta.convertData(standardValueMeta, hopValue);
    }
    putRow(data.outputRowMeta, outputRow);
  }

  private static Object detachMutableAvroValue(Object hopValue) {
    if (hopValue instanceof byte[] bytes) {
      return Arrays.copyOf(bytes, bytes.length);
    }
    if (hopValue instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    if (hopValue instanceof Utf8 utf8) {
      return utf8.toString();
    }
    return hopValue;
  }

  private boolean rejectOrThrow(Object[] row, Exception e) throws HopException {
    if (getTransformMeta().isDoingErrorHandling()) {
      String message = e.getMessage() == null ? e.toString() : e.getMessage();
      putError(getInputRowMeta(), row, 1L, message, data.sourceFieldName, CONST_ERROR_CODE);
      return true;
    }
    if (e instanceof HopException hopException) {
      throw hopException;
    }
    if (e instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    throw new HopException(e);
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
