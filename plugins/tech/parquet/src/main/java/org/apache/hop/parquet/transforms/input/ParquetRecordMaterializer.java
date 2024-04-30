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

package org.apache.hop.parquet.transforms.input;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class ParquetRecordMaterializer extends RecordMaterializer<RowMetaAndData> {

  private final ParquetRowConverter root;
  private final MessageType messageType;
  private final List<ParquetField> fields;
  private final List<Integer> requestedFieldIndexes;
  private final IRowMeta schemaRowMeta;

  public ParquetRecordMaterializer(MessageType messageType, List<ParquetField> fields) {
    this.messageType = messageType;
    this.fields = fields;
    this.requestedFieldIndexes = new ArrayList<>();
    this.schemaRowMeta = new RowMeta();
    for (ParquetField field : fields) {
      int fieldIndex = messageType.getFieldIndex(field.getSourceField());
      if (fieldIndex < 0) {
        throw new RuntimeException(
            "Error finding source field '" + field.getSourceField() + "' in the input file");
      }
      requestedFieldIndexes.add(fieldIndex);
      try {
        schemaRowMeta.addValueMeta(field.createValueMeta());
      } catch (HopException e) {
        throw new RuntimeException(
            "Error creating value of type " + field.getTargetType() + "'", e);
      }
    }

    this.root = new ParquetRowConverter(messageType, schemaRowMeta, fields);
  }

  /*
   Convert to a record...
  */
  @Override
  public RowMetaAndData getCurrentRecord() {
    return root.getGroup();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
