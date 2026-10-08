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

import java.util.List;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.neo4j.shared.NeoHopData;
import org.junit.jupiter.api.Test;

class GraphPropertyDataTypeTest {

  @Test
  void numberImportTypeIsDouble() {
    assertEquals("double", GraphPropertyDataType.Number.getImportType());
  }

  @Test
  void vectorRoundTrip() throws Exception {
    IValueMeta vector = new ValueMetaBase("embedding", IValueMeta.TYPE_VECTOR) {};
    assertEquals(GraphPropertyDataType.Vector, GraphPropertyDataType.getTypeFromHop(vector));
    assertEquals(IValueMeta.TYPE_VECTOR, GraphPropertyDataType.Vector.getHopType());

    Object written = GraphPropertyDataType.Vector.convertFromHop(vector, new float[] {1f, 2f});
    assertEquals(List.of(1.0, 2.0), written);

    // A database returns the list; a Vector return field gets the float[] back
    assertArrayEquals(
        new float[] {1f, 2f}, (float[]) NeoHopData.convertToHopValue("e", written, vector), 0f);
  }

  @Test
  void vectorCsvValueUsesArrayDelimiter() {
    assertEquals("float[]", GraphPropertyDataType.Vector.getImportType());
    GraphPropertyData data =
        new GraphPropertyData("e", List.of(0.5, 1.0), GraphPropertyDataType.Vector, false);
    assertEquals("0.5;1.0", data.toString());
  }
}
