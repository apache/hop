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

package org.apache.hop.neo4j.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.junit.jupiter.api.Test;

class GraphPropertyTypeTest {

  private static final IValueMeta VECTOR =
      new ValueMetaBase("embedding", IValueMeta.TYPE_VECTOR) {};

  @Test
  void vectorFieldMapsToVectorProperty() {
    assertEquals(GraphPropertyType.Vector, GraphPropertyType.getTypeFromHop(VECTOR));
    assertEquals(GraphPropertyType.Vector, GraphPropertyType.parseCode("Vector"));
  }

  @Test
  void vectorIsPassedAsListOfNumbers() throws Exception {
    // The graph query and cypher builder parameters use this two-argument conversion
    assertEquals(
        List.of(1.0, 2.0), GraphPropertyType.Vector.convertFromHop(VECTOR, new float[] {1f, 2f}));
  }
}
