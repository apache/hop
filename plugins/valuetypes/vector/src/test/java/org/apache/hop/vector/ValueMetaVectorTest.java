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

package org.apache.hop.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ValueMetaVectorTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testTypeAndNativeClass() {
    ValueMetaVector meta = new ValueMetaVector("embedding");
    assertEquals(ValueMetaVector.TYPE_VECTOR, meta.getType());
    assertEquals("embedding", meta.getName());
    assertEquals(float[].class, meta.getNativeDataTypeClass());
  }

  @Test
  void testParseCanonicalForm() throws Exception {
    assertArrayEquals(new float[] {0.1f, 0.2f, 0.3f}, ValueMetaVector.parse("[0.1,0.2,0.3]"));
  }

  @Test
  void testParseToleratesWhitespaceAndMissingBrackets() throws Exception {
    assertArrayEquals(new float[] {0.1f, 0.2f}, ValueMetaVector.parse("  [ 0.1 , 0.2 ] "));
    assertArrayEquals(new float[] {0.1f, 0.2f}, ValueMetaVector.parse("0.1, 0.2"));
  }

  @Test
  void testParseNegativeAndScientificNotation() throws Exception {
    assertArrayEquals(new float[] {-0.5f, 1.5e-3f}, ValueMetaVector.parse("[-0.5,1.5e-3]"));
  }

  @Test
  void testParseNullAndBlankAndEmpty() throws Exception {
    assertNull(ValueMetaVector.parse(null));
    assertNull(ValueMetaVector.parse("   "));
    assertArrayEquals(new float[0], ValueMetaVector.parse("[]"));
  }

  @Test
  void testParseRejectsNonNumbers() {
    assertThrows(HopValueException.class, () -> ValueMetaVector.parse("[0.1,abc]"));
    assertThrows(HopValueException.class, () -> ValueMetaVector.parse("[0.1,,0.2]"));
  }

  @Test
  void testRenderRoundTrip() throws Exception {
    float[] vector = {0.1f, -2.5f, 3.0f};
    String rendered = ValueMetaVector.render(vector);
    assertTrue(rendered.startsWith("[") && rendered.endsWith("]"));
    assertArrayEquals(vector, ValueMetaVector.parse(rendered));
    assertNull(ValueMetaVector.render(null));
  }

  @Test
  void testGetStringUsesCanonicalForm() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    assertEquals("[1.0,2.0]", meta.getString(new float[] {1f, 2f}));
    assertNull(meta.getString(null));
  }

  @Test
  void testConvertFromString() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    Object converted = meta.convertData(new ValueMetaString("s"), "[1,2,3]");
    assertInstanceOf(float[].class, converted);
    assertArrayEquals(new float[] {1f, 2f, 3f}, (float[]) converted);
  }

  @Test
  void testConvertPassesThroughVector() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    float[] vector = {1f, 2f};
    assertArrayEquals(vector, (float[]) meta.convertData(meta, vector));
  }

  @Test
  void testConvertNullStaysNull() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    assertNull(meta.convertData(new ValueMetaString("s"), null));
  }

  @Test
  void testCloneValueDataCopiesTheArray() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    float[] original = {1f, 2f, 3f};
    float[] copy = (float[]) meta.cloneValueData(original);
    assertArrayEquals(original, copy);
    assertNotSame(original, copy);
    copy[0] = 99f;
    assertEquals(1f, original[0], 0.0f);
  }

  @Test
  void testCompareOrdersByLengthThenElement() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    assertTrue(meta.compare(new float[] {1f}, new float[] {1f, 2f}) < 0);
    assertTrue(meta.compare(new float[] {1f, 3f}, new float[] {1f, 2f}) > 0);
    assertEquals(0, meta.compare(new float[] {1f, 2f}, new float[] {1f, 2f}));
  }

  @Test
  void testCompareHandlesNulls() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    assertEquals(0, meta.compare(null, null));
    assertTrue(meta.compare(null, new float[] {1f}) < 0);
    assertTrue(meta.compare(new float[] {1f}, null) > 0);
  }

  @Test
  void testHashCodeMatchesContent() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    assertEquals(meta.hashCode(new float[] {1f, 2f}), meta.hashCode(new float[] {1f, 2f}));
    assertEquals(0, meta.hashCode(null));
  }

  @Test
  void testBinaryStringRoundTrip() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    byte[] binary = meta.getBinaryString(new float[] {1f, 2f});
    assertEquals("[1.0,2.0]", new String(binary, StandardCharsets.UTF_8));
    assertNull(meta.getBinaryString(null));
  }

  @Test
  void testWriteAndReadDataRoundTrip() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    float[] vector = new float[1536];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = i * 0.001f;
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      meta.writeData(out, vector);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      assertArrayEquals(vector, (float[]) meta.readData(in));
    }
  }

  @Test
  void testWriteAndReadNull() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      meta.writeData(out, null);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      assertNull(meta.readData(in));
    }
  }

  @Test
  void testBinaryStringStorageConvertsBack() throws Exception {
    ValueMetaVector meta = new ValueMetaVector("v");
    meta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    meta.setStorageMetadata(new ValueMetaString("v"));
    byte[] binary = "[1,2,3]".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(new float[] {1f, 2f, 3f}, (float[]) meta.convertData(meta, binary));
  }
}
