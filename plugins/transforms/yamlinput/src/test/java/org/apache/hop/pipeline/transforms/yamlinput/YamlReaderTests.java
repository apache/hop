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

package org.apache.hop.pipeline.transforms.yamlinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileWriter;
import java.nio.file.Path;
import java.util.Date;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit test for {@link YamlReader} */
class YamlReaderTests {
  private YamlReader reader;

  @BeforeEach
  void setUp() {
    reader = new YamlReader();
  }

  @BeforeAll
  static void init() {
    try {
      HopEnvironment.init();
    } catch (HopException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testLoadString_MapStructure() {
    String yaml = """
				name: John
				age: 30
				active: true
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    assertEquals(3, rowMeta.size());

    Object[] row = reader.getRow(rowMeta);

    assertNotNull(row);
    assertEquals("John", row[0]);
    // Integer -> Long
    assertEquals(30L, row[1]);
    assertEquals(true, row[2]);
  }

  @Test
  void testLoadString_ListOfMap() {
    String yaml = """
				- name: Alice
				  age: 25
				  createTime: 2026-03-04 12:03:01
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    Object[] row = reader.getRow(rowMeta);

    assertNotNull(row);
    assertEquals("Alice", row[0]);
    assertEquals(25L, row[1]);
    assertInstanceOf(Date.class, row[2]);
  }

  @Test
  void testLoadString_ListMultipleValues() {
    String yaml = """
				- 1
				- 2
				- 3
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    // DEFAULT_LIST_VALUE_NAME
    assertEquals(1, rowMeta.size());

    Object[] row1 = reader.getRow(rowMeta);
    Object[] row2 = reader.getRow(rowMeta);
    Object[] row3 = reader.getRow(rowMeta);

    assertEquals(1L, row1[0]);
    assertEquals(2L, row2[0]);
    assertEquals(3L, row3[0]);
  }

  @Test
  void testMultipleDocuments() {
    String yaml = """
				---
				name: A
				---
				name: B
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    Object[] row1 = reader.getRow(rowMeta);
    Object[] row2 = reader.getRow(rowMeta);

    assertEquals("A", row1[0]);
    assertEquals("B", row2[0]);
  }

  @Test
  void testTypeConversion_Number() {
    String yaml = """
				value: 12.5
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    Object[] row = reader.getRow(rowMeta);

    assertInstanceOf(Double.class, row[0]);
    assertEquals(12.5, (Double) row[0]);
  }

  @Test
  void testNestedMapToString() {
    String yaml = """
				person:
				  name: Tom
				  age: 20
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    Object[] row = reader.getRow(rowMeta);

    assertNotNull(row[0]);
    assertTrue(row[0].toString().contains("name"));
    assertTrue(row[0].toString().contains("age"));
  }

  @Test
  void testEmptyYaml() {
    reader.loadString("");

    RowMeta rowMeta = reader.getFields();
    Object[] row = reader.getRow(rowMeta);

    assertEquals(0, row.length);
  }

  @Test
  void testGetFields_TypeInference() {
    String yaml =
        """
				- intField: 1
				  longField: 2
				  doubleField: 1.5
				  boolField: true
				  bigIntField: 99999999999999999999
				  bigDecField: 9999999999999999999999999.55
				  byteField: 1
				  dateField: 2026-03-04T12:03:01
				""";

    reader.loadString(yaml);

    RowMeta rowMeta = reader.getFields();
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.getValueMeta(2).getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, rowMeta.getValueMeta(3).getType());
    assertEquals(IValueMeta.TYPE_BIGNUMBER, rowMeta.getValueMeta(4).getType());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.getValueMeta(5).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(6).getType());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.getValueMeta(7).getType());
  }

  @Test
  void testLoadFile_singleDocument_map(@TempDir Path tempDir) throws Exception {
    Path tempFile = tempDir.resolve("test.yaml");
    try (FileWriter writer = new FileWriter(tempFile.toFile())) {
      writer.write("""
					name: Alice
					age: 30
					""");
    }

    FileObject fileObject = HopVfs.getFileObject(tempFile.toString());
    reader.loadFile(fileObject);

    assertNotNull(reader.getYaml());
    assertTrue(reader.isMapUsed());

    RowMeta rowMeta = reader.getFields();
    assertEquals(2, rowMeta.size());
    assertTrue(reader.getBytesReadFromFile() > 0);
  }
}
