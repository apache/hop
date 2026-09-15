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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.junit.jupiter.api.Test;

class MySqlBulkLoaderTest {

  @Test
  void binaryFieldBytesForMysqlLoadData_usesLowercaseHex() throws HopValueException {
    ValueMetaBinary meta = new ValueMetaBinary("hash");
    byte[] bytes = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
    assertArrayEquals(
        "deadbeef".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
        MySqlBulkLoader.binaryFieldBytesForMysqlLoadData(meta, bytes));
    assertArrayEquals(
        new byte[0], MySqlBulkLoader.binaryFieldBytesForMysqlLoadData(meta, new byte[0]));
    assertNull(MySqlBulkLoader.binaryFieldBytesForMysqlLoadData(meta, null));
  }

  @Test
  void appendLoadColumns_bindsBinaryFieldsAsUnhexUserVariables() {
    StringBuilder sql = new StringBuilder();
    MySqlBulkLoader.appendLoadColumns(
        sql, new String[] {"id", "`hash`", "name"}, new boolean[] {false, true, false});

    String written = sql.toString();
    assertTrue(written.startsWith("(id,@col1,name) SET `hash` = UNHEX(@col1);"), written);
    assertFalse(written.contains("id = UNHEX"), written);
  }

  @Test
  void appendLoadColumns_leavesTheStatementAloneWhenNoFieldIsBinary() {
    StringBuilder sql = new StringBuilder();
    MySqlBulkLoader.appendLoadColumns(
        sql, new String[] {"id", "name"}, new boolean[] {false, false});

    assertEquals("(id,name);\n", sql.toString().replace("\r\n", "\n"));
  }
}
