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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Parses a Parquet file from test resources and validates schema and row content. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetResourceFileTest {

  private static final String SAMPLE_FILE = "hello.parquet-00-0001.parquet";

  @Test
  @Disabled("testReadSampleParquetFileSchema")
  void testReadSampleParquetFileSchema() throws Exception {
    String filename = ParquetTestUtil.resourceFilePath(getClass(), SAMPLE_FILE);

    IRowMeta schema = ParquetTestUtil.readSchema(filename);

    assertEquals(4, schema.size());
    assertEquals("user_id", schema.getValueMeta(0).getName());
    assertEquals("json", schema.getValueMeta(1).getName());
    assertEquals("age", schema.getValueMeta(2).getName());
    assertEquals("sex", schema.getValueMeta(3).getName());
    assertEquals(IValueMeta.TYPE_STRING, schema.getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_STRING, schema.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, schema.getValueMeta(2).getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, schema.getValueMeta(3).getType());
  }

  @Test
  @Disabled("testReadSampleParquetFileRows")
  void testReadSampleParquetFileRows() throws Exception {
    String filename = ParquetTestUtil.resourceFilePath(getClass(), SAMPLE_FILE);

    IRowMeta schema = ParquetTestUtil.readSchema(filename);
    List<ParquetField> readFields = ParquetTestUtil.fieldsFromRowMeta(schema);
    List<RowMetaAndData> rows = ParquetTestUtil.readAllRows(filename, readFields);

    assertEquals(2, rows.size());

    rows.sort(
        (a, b) -> {
          try {
            return a.getRowMeta()
                .getString(a.getData(), indexOf(a.getRowMeta(), "user_id"))
                .compareTo(
                    b.getRowMeta().getString(b.getData(), indexOf(b.getRowMeta(), "user_id")));
          } catch (HopValueException e) {
            throw new RuntimeException(e);
          }
        });

    assertRow(rows.get(0), "item.100", "hello", 12L, true);
    assertRow(rows.get(1), "item.200", "world", 20L, false);
  }

  private static void assertRow(
      RowMetaAndData row, String userId, String json, long age, boolean sex) throws Exception {
    IRowMeta rowMeta = row.getRowMeta();
    Object[] rowData = row.getData();
    assertNotNull(rowData);

    assertEquals(userId, rowMeta.getString(rowData, indexOf(rowMeta, "user_id")));
    assertEquals(json, rowMeta.getString(rowData, indexOf(rowMeta, "json")));
    assertEquals(age, rowMeta.getInteger(rowData, indexOf(rowMeta, "age")));
    assertEquals(sex, rowMeta.getBoolean(rowData, indexOf(rowMeta, "sex")));
  }

  private static int indexOf(IRowMeta rowMeta, String fieldName) {
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new AssertionError("Field '" + fieldName + "' not found");
    }
    return index;
  }
}
