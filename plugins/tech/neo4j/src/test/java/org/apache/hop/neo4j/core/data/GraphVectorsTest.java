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

package org.apache.hop.neo4j.core.data;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;

class GraphVectorsTest {

  private static final IValueMeta VECTOR =
      new ValueMetaBase("embedding", IValueMeta.TYPE_VECTOR) {};

  @Test
  void vectorFieldBecomesListOfNumbers() throws Exception {
    assertEquals(
        List.of(0.5, -1.0, 2.25), GraphVectors.toList(VECTOR, new float[] {0.5f, -1f, 2.25f}));
  }

  @Test
  void nullStaysNull() throws Exception {
    assertNull(GraphVectors.toList(VECTOR, null));
    assertNull(GraphVectors.toFloatArray(null));
  }

  @Test
  void textFromEmbedTextIsParsed() throws Exception {
    IValueMeta text = new ValueMetaString("embedding");
    assertEquals(List.of(0.1, 0.2, 0.3), GraphVectors.toList(text, "[0.1,0.2,0.3]"));
    assertEquals(List.of(0.1, 0.2), GraphVectors.toList(text, " 0.1, 0.2 "));
    assertEquals(List.of(), GraphVectors.toList(text, "[]"));
  }

  @Test
  void returnedListBecomesFloatArray() throws Exception {
    assertArrayEquals(
        new float[] {1f, 0.5f, 3f}, GraphVectors.toFloatArray(List.of(1L, 0.5d, 3)), 0f);
    assertArrayEquals(new float[] {0.25f}, GraphVectors.toFloatArray(new double[] {0.25d}), 0f);
    assertArrayEquals(new float[] {1.5f, 2f}, GraphVectors.toFloatArray("[1.5, 2]"), 0f);
  }

  @Test
  void nonNumbersAreRefused() {
    assertThrows(HopValueException.class, () -> GraphVectors.toFloatArray(List.of("a")));
    assertThrows(HopValueException.class, () -> GraphVectors.toList("[1, two]"));
  }

  @Test
  void nativeNeo4jVectorsAreConverted() throws Exception {
    Object float32 = Values.vector(new float[] {0.5f, -1f}).asObject();
    assertArrayEquals(new float[] {0.5f, -1f}, GraphVectors.toFloatArray(float32), 0f);
    Object float64 = Values.vector(new double[] {0.25d, 2d}).asObject();
    assertArrayEquals(new float[] {0.25f, 2f}, GraphVectors.toFloatArray(float64), 0f);
    Object int8 = Values.vector(new byte[] {1, -2}).asObject();
    assertEquals(List.of(1.0, -2.0), GraphVectors.toList(int8));
    assertEquals(
        GraphPropertyDataType.Vector, GraphPropertyDataType.getTypeFromNeo4jValue(float32));
  }

  @Test
  void numbersAreNotVectors() {
    assertThrows(HopValueException.class, () -> GraphVectors.toFloatArray(1.5d));
    assertThrows(HopValueException.class, () -> GraphVectors.toList(7L));
    assertThrows(
        HopValueException.class,
        () -> GraphVectors.toList(new ValueMetaBase("n", IValueMeta.TYPE_INTEGER) {}, 7L));
  }
}
