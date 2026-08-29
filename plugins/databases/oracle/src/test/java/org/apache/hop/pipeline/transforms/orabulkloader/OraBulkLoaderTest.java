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

package org.apache.hop.pipeline.transforms.orabulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.junit.jupiter.api.Test;

class OraBulkLoaderTest {

  @Test
  void binarySqlLoaderField_usesHextorawAndAWideChar() {
    assertEquals(
        " CHAR(2000000) \"HEXTORAW(:HASH)\"", OraBulkLoader.binarySqlLoaderField("HASH", -1));
    assertEquals(" CHAR(255) \"HEXTORAW(:HASH)\"", OraBulkLoader.binarySqlLoaderField("HASH", 16));
    assertEquals(
        " CHAR(4000) \"HEXTORAW(:HASH)\"", OraBulkLoader.binarySqlLoaderField("HASH", 2000));
  }

  @Test
  void writesBinaryValuesAsQuotedHex() throws Exception {
    OraBulkLoaderMeta meta = new OraBulkLoaderMeta();
    meta.setMappings(List.of(new OraBulkLoaderMappingMeta("HASH", "hash", null)));

    OraBulkDataOutput output = new OraBulkDataOutput(meta, "\n");
    StringWriter writer = new StringWriter();
    output.initForTest(writer, "\"", new int[] {0});

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("hash"));
    output.writeLine(rowMeta, new Object[] {new byte[] {(byte) 0xde, (byte) 0xad}});

    assertEquals("\"dead\"\n", writer.toString());
    assertFalse(writer.toString().contains("startlob"));
    assertTrue(writer.toString().startsWith("\""));
  }
}
