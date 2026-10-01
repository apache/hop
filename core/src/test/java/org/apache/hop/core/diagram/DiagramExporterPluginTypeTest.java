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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class DiagramExporterPluginTypeTest {

  @Test
  void testSingletonAndMetadata() {
    DiagramExporterPluginType pluginType = DiagramExporterPluginType.getInstance();
    assertNotNull(pluginType);
    assertEquals("DIAGRAM_EXPORTER", pluginType.getId());
    assertEquals("Diagram Exporter", pluginType.getName());
  }

  @Test
  void testAnnotationExtraction() {
    DiagramExporterPluginType pluginType = DiagramExporterPluginType.getInstance();

    @DiagramExporter(
        id = "mock-id",
        name = "Mock Exporter",
        description = "A mock exporter",
        format = "SVG",
        fileExtension = "svg")
    class MockExporter {}

    DiagramExporter annotation = MockExporter.class.getAnnotation(DiagramExporter.class);
    assertEquals("mock-id", pluginType.extractID(annotation));
    assertEquals("Mock Exporter", pluginType.extractName(annotation));
    assertEquals("A mock exporter", pluginType.extractDesc(annotation));
    assertEquals("SVG", pluginType.extractCategory(annotation));
  }
}
