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
package org.apache.hop.pgvector.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EmbeddingJsonParserTest {

  @Test
  void parsesJsonEmbedding() {
    assertArrayEquals(new float[] {0.1f, 2.5f, -1f}, EmbeddingJsonParser.parse("[0.1,2.5,-1.0]"));
  }

  @Test
  void convertsToPgVectorLiteral() {
    assertEquals("[1.0,2.0]", EmbeddingJsonParser.toPgVectorLiteral("[1,2]"));
  }

  @Test
  void convertsAVectorValueWithoutGoingThroughText() {
    Object value = new float[] {1f, 2f};
    assertEquals("[1.0,2.0]", EmbeddingJsonParser.toPgVectorLiteral(value));
  }

  @Test
  void convertsADoubleArray() {
    Object value = new double[] {1d, 2d};
    assertEquals("[1.0,2.0]", EmbeddingJsonParser.toPgVectorLiteral(value));
  }

  @Test
  void convertsAStringValueThroughTheObjectOverload() {
    Object value = "[1,2]";
    assertEquals("[1.0,2.0]", EmbeddingJsonParser.toPgVectorLiteral(value));
  }

  @Test
  void convertsNullToAnEmptyLiteral() {
    assertEquals("[]", EmbeddingJsonParser.toPgVectorLiteral((Object) null));
  }

  @Test
  void detectsEmptyValues() {
    assertTrue(EmbeddingJsonParser.isEmpty(null));
    assertTrue(EmbeddingJsonParser.isEmpty(new float[0]));
    assertTrue(EmbeddingJsonParser.isEmpty(new double[0]));
    assertTrue(EmbeddingJsonParser.isEmpty("   "));
    assertFalse(EmbeddingJsonParser.isEmpty(new float[] {1f}));
    assertFalse(EmbeddingJsonParser.isEmpty("[1]"));
  }
}
