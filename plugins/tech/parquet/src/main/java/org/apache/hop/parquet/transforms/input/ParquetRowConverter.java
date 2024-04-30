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

import java.util.List;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;

public class ParquetRowConverter extends GroupConverter {

  private final MessageType messageType;
  private RowMetaAndData group;
  private IRowMeta rowMeta;
  private List<ParquetField> fields;

  public ParquetRowConverter(MessageType messageType, IRowMeta rowMeta, List<ParquetField> fields) {
    this.messageType = messageType;
    this.group = new RowMetaAndData(rowMeta, null);
    this.rowMeta = rowMeta;
    this.fields = fields;
  }

  @Override
  public Converter getConverter(final int schemaIndex) {

    String sourceField = messageType.getFieldName(schemaIndex);

    // What is the index in the target row?
    //
    int rowIndex = -1;
    for (int i = 0; i < fields.size(); i++) {
      ParquetField field = fields.get(i);
      if (field.getSourceField().equalsIgnoreCase(sourceField)) {
        rowIndex = i;
        break;
      }
    }

    return new ParquetValueConverter(group, rowIndex);
  }

  @Override
  public void start() {
    Object[] rowData = RowDataUtil.allocateRowData(rowMeta.size());
    this.group.setData(rowData);
  }

  @Override
  public void end() {
    // Nothing to do here...
  }

  /**
   * Gets group
   *
   * @return value of group
   */
  public RowMetaAndData getGroup() {
    return group;
  }
}
