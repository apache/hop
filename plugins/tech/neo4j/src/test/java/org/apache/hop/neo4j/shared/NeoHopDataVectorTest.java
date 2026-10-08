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

package org.apache.hop.neo4j.shared;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

/** Native Neo4j VECTOR values returned over Bolt. */
class NeoHopDataVectorTest {

  private static final IValueMeta VECTOR =
      new ValueMetaBase("embedding", IValueMeta.TYPE_VECTOR) {};

  @Test
  void nativeVectorToVectorField() throws Exception {
    Value value = Values.vector(new float[] {0.5f, -1f});
    Object hopValue = NeoHopData.convertNeoToHopValue("embedding", value, null, VECTOR);
    assertArrayEquals(new float[] {0.5f, -1f}, (float[]) hopValue, 0f);
  }

  @Test
  void listOfNumbersToVectorField() throws Exception {
    Value value = Values.value(List.of(1.0d, 2.0d));
    Object hopValue = NeoHopData.convertNeoToHopValue("embedding", value, null, VECTOR);
    assertArrayEquals(new float[] {1f, 2f}, (float[]) hopValue, 0f);
  }

  @Test
  void nativeVectorToJson() throws Exception {
    IValueMeta string = new ValueMetaString("embedding");
    Value value = Values.vector(new double[] {0.5d, -1d});
    assertEquals("[0.5,-1.0]", NeoHopData.convertNeoToHopValue("embedding", value, null, string));
    Value map = Values.value(Map.of("v", Values.vector(new double[] {2d})));
    assertEquals(
        "{\"v\":[2.0]}",
        NeoHopData.convertNeoToHopValue("m", map, GraphPropertyDataType.Map, string));
  }
}
