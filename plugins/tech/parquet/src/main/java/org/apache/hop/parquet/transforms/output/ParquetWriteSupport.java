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

package org.apache.hop.parquet.transforms.output;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

public class ParquetWriteSupport extends WriteSupport<RowMetaAndData> {

  private final MessageType messageType;
  private RecordConsumer recordConsumer;
  private final List<Integer> sourceFieldIndexes;
  private final List<ParquetField> fields;

  /** The logical type of the column of every field, which decides how its values are stored. */
  private final LogicalTypeAnnotation[] logicalTypes;

  public ParquetWriteSupport(
      MessageType messageType, List<Integer> sourceFieldIndexes, List<ParquetField> fields) {
    this.messageType = messageType;
    this.sourceFieldIndexes = sourceFieldIndexes;
    this.fields = fields;
    this.logicalTypes = new LogicalTypeAnnotation[fields.size()];
    for (int i = 0; i < fields.size() && i < messageType.getFieldCount(); i++) {
      logicalTypes[i] = messageType.getType(i).getLogicalTypeAnnotation();
    }
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(messageType, new HashMap<>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(RowMetaAndData row) {
    recordConsumer.startMessage();
    try {
      // Grab the fields that are mapped...
      // Write a value
      //
      for (int i = 0; i < fields.size(); i++) {
        ParquetField field = fields.get(i);
        int index = sourceFieldIndexes.get(i);
        IValueMeta valueMeta = row.getValueMeta(index);
        Object valueData = row.getData()[index];

        boolean isNull = valueMeta.isNull(valueData);
        if (!isNull) {
          recordConsumer.startField(field.getTargetFieldName(), i);

          // The column type, as built by ParquetOutput.avroType(), decides how the value is stored.
          // Anything without a column type of its own goes out as a string.
          //
          LogicalTypeAnnotation logicalType = logicalTypes[i];
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_INTEGER -> recordConsumer.addLong(valueMeta.getInteger(valueData));
            case IValueMeta.TYPE_NUMBER -> recordConsumer.addDouble(valueMeta.getNumber(valueData));
            case IValueMeta.TYPE_BOOLEAN ->
                recordConsumer.addBoolean(valueMeta.getBoolean(valueData));
            case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP ->
                recordConsumer.addLong(epochValue(valueMeta, valueData, logicalType));
            case IValueMeta.TYPE_BINARY ->
                recordConsumer.addBinary(
                    Binary.fromConstantByteArray(valueMeta.getBinary(valueData)));
            case IValueMeta.TYPE_BIGNUMBER -> {
              if (logicalType instanceof DecimalLogicalTypeAnnotation decimal) {
                recordConsumer.addBinary(
                    decimalBytes(
                        field.getTargetFieldName(),
                        valueMeta,
                        valueMeta.getBigNumber(valueData),
                        decimal));
              } else {
                recordConsumer.addBinary(Binary.fromString(valueMeta.getString(valueData)));
              }
            }
            case IValueMeta.TYPE_UUID -> {
              if (logicalType instanceof UUIDLogicalTypeAnnotation) {
                recordConsumer.addBinary(uuidBytes(valueMeta.getString(valueData)));
              } else {
                recordConsumer.addBinary(Binary.fromString(valueMeta.getString(valueData)));
              }
            }
            default -> recordConsumer.addBinary(Binary.fromString(valueMeta.getString(valueData)));
          }
          recordConsumer.endField(field.getTargetFieldName(), i);
        }
      }
      recordConsumer.endMessage();
    } catch (HopException e) {
      throw new HopRuntimeException("Error writing row to Parquet", e);
    }
  }

  /**
   * A date or timestamp as the number the column holds: microseconds for a TIMESTAMP(MICROS)
   * column, milliseconds otherwise. The value meta takes care of lazy (binary string) storage and
   * the date mask.
   */
  static long epochValue(IValueMeta valueMeta, Object valueData, LogicalTypeAnnotation logicalType)
      throws HopException {
    if (logicalType instanceof TimestampLogicalTypeAnnotation timestamp
        && timestamp.getUnit() == TimeUnit.MICROS) {
      Timestamp ts =
          valueMeta instanceof ValueMetaTimestamp timestampMeta
              ? timestampMeta.getTimestamp(valueData)
              : new Timestamp(valueMeta.getDate(valueData).getTime());
      // getTime() holds the whole milliseconds, getNanos() the complete fraction of the second.
      return Math.floorDiv(ts.getTime(), 1000L) * 1_000_000L + ts.getNanos() / 1_000L;
    }
    return valueMeta.getDate(valueData).getTime();
  }

  /**
   * A big number as the unscaled two's complement bytes of a DECIMAL column, rounded to the scale
   * of the column the way the field rounds.
   */
  static Binary decimalBytes(
      String fieldName,
      IValueMeta valueMeta,
      BigDecimal value,
      DecimalLogicalTypeAnnotation decimal) {
    BigDecimal scaled = value.setScale(decimal.getScale(), roundingMode(valueMeta));
    if (scaled.precision() - scaled.scale() > decimal.getPrecision() - decimal.getScale()) {
      throw new HopRuntimeException(
          "Value "
              + value.toPlainString()
              + " of field '"
              + fieldName
              + "' doesn't fit in DECIMAL("
              + decimal.getPrecision()
              + ","
              + decimal.getScale()
              + "): increase the length of the field");
    }
    return Binary.fromConstantByteArray(scaled.unscaledValue().toByteArray());
  }

  private static RoundingMode roundingMode(IValueMeta valueMeta) {
    String roundingType = valueMeta.getRoundingType();
    if (roundingType != null) {
      try {
        return RoundingMode.valueOf(roundingType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        // Not a rounding mode we know: use the default.
      }
    }
    return RoundingMode.HALF_EVEN;
  }

  /** A UUID as the 16 big-endian bytes of a UUID column. */
  static Binary uuidBytes(String uuid) {
    UUID value = UUID.fromString(uuid);
    return Binary.fromConstantByteArray(
        ByteBuffer.allocate(16)
            .putLong(value.getMostSignificantBits())
            .putLong(value.getLeastSignificantBits())
            .array());
  }
}
