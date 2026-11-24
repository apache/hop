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

package org.apache.hop.core.diagram;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.junit.jupiter.api.Test;

class DiagramExportOptionsTest {
  @Test
  void testLayoutDefaultsForBothConstructors() {
    for (final var options :
        new DiagramExportOptions[] {
          new DiagramExportOptions(), new DiagramExportOptions("diagram.svg", "SVG")
        }) {
      assertEquals(DiagramLayoutPolicy.NONE, options.getLayoutPolicy());
      assertEquals(DiagramLayoutAlgorithm.LAYERED, options.getLayoutAlgorithm());
    }
  }

  @Test
  void testLayoutOptions() {
    final var options = new DiagramExportOptions();
    for (final var policy : DiagramLayoutPolicy.values()) {
      for (final var algorithm : DiagramLayoutAlgorithm.values()) {
        options.setLayoutPolicy(policy);
        options.setLayoutAlgorithm(algorithm);
        assertEquals(policy, options.getLayoutPolicy());
        assertEquals(algorithm, options.getLayoutAlgorithm());
      }
    }
  }

  @Test
  void testStableCodesAndLocalizedDescriptions() {
    for (final var policy : DiagramLayoutPolicy.values()) {
      assertEquals(policy.name(), policy.getCode());
      assertEquals(policy.name(), policy.toString());
      assertDescription(policy);
    }
    for (final var algorithm : DiagramLayoutAlgorithm.values()) {
      assertEquals(algorithm.name(), algorithm.getCode());
      assertEquals(algorithm.name(), algorithm.toString());
      assertDescription(algorithm);
    }
  }

  private void assertDescription(final IEnumHasCodeAndDescription value) {
    assertFalse(value.getDescription().isBlank());
    assertFalse(value.getDescription().contains("!"));
    assertNotEquals(value.getCode(), value.getDescription());
  }
}
