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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class ParquetWriteSupport extends WriteSupport<RowMetaAndData> {

  private final MessageType messageType;
  private final Schema avroSchema;
  private RecordConsumer recordConsumer;
  private final List<Integer> sourceFieldIndexes;
  private final List<ParquetField> fields;
  private Map<Integer, Schema> fieldSchemas;
  private Map<Integer, LogicalType> fieldTypes;

  public ParquetWriteSupport(
      MessageType messageType,
      Schema avroSchema,
      List<Integer> sourceFieldIndexes,
      List<ParquetField> fields) {
    this.messageType = messageType;
    this.avroSchema = avroSchema;
    this.sourceFieldIndexes = sourceFieldIndexes;
    this.fields = fields;

    fieldSchemas = new HashMap<>();
    fieldTypes = new HashMap<>();
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

          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_INTEGER:
              recordConsumer.addLong(valueMeta.getInteger(valueData));
              break;
            case IValueMeta.TYPE_NUMBER:
              recordConsumer.addDouble(valueMeta.getNumber(valueData));
              break;
            case IValueMeta.TYPE_BOOLEAN:
              recordConsumer.addBoolean(valueMeta.getBoolean(valueData));
              break;
            case IValueMeta.TYPE_DATE:
              recordConsumer.addLong(valueMeta.getDate(valueData).getTime());
              break;
            case IValueMeta.TYPE_BINARY:
              byte[] bytes = valueMeta.getBinary(valueData);
              recordConsumer.addBinary(Binary.fromConstantByteArray(bytes));
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              // Convert to String for now...
              //
              String bigString = valueMeta.getString(valueData);
              recordConsumer.addBinary(Binary.fromString(bigString));
              break;
            case IValueMeta.TYPE_STRING:
            default:
              recordConsumer.addBinary(Binary.fromString(valueMeta.getString(valueData)));
              break;
          }
          recordConsumer.endField(field.getTargetFieldName(), i);
        }
      }
      recordConsumer.endMessage();
    } catch (HopException e) {
      throw new RuntimeException("Error writing row to Parquet", e);
    }
  }
}
