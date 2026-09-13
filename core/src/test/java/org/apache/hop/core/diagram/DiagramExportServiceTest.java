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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DiagramExportServiceTest {

  private DiagramExportService service;
  private TestExporter testExporter;

  static class TestSubject {
    String name = "sample";
  }

  @DiagramExporter(
      id = "test-exporter",
      name = "Test Exporter",
      description = "Test Description",
      format = "MERMAID",
      fileExtension = "mmd",
      supportedSubjectTypes = {TestSubject.class})
  static class TestExporter extends BaseDiagramExporter<TestSubject> {
    @Override
    public DiagramExportResult export(
        TestSubject subject, DiagramExportOptions options, IExportContext context)
        throws HopException {
      String content = "graph LR\n  " + subject.name;
      writeToTarget(options.getTargetFilename(), content, context);
      return DiagramExportResult.success(options.getTargetFilename(), content, "text/vnd.mermaid");
    }
  }

  @BeforeEach
  void setUp() {
    service = DiagramExportService.getInstance();
    testExporter = new TestExporter();
    service.registerStaticExporter(testExporter);
  }

  @AfterEach
  void tearDown() {
    service.unregisterStaticExporter(testExporter);
  }

  @Test
  void testExporterLookupAndSubjectSupport() {
    TestSubject subject = new TestSubject();
    List<IDiagramExporter<?>> exporters = service.findExportersForSubject(subject);
    assertFalse(exporters.isEmpty());
    assertTrue(exporters.contains(testExporter));

    IDiagramExporter<?> byFormat = service.findExporter(subject, "MERMAID");
    assertNotNull(byFormat);
    assertEquals("test-exporter", byFormat.getId());

    IDiagramExporter<?> byExt = service.findExporter(subject, "mmd");
    assertNotNull(byExt);
    assertEquals("test-exporter", byExt.getId());

    IDiagramExporter<?> byId = service.getExporter("test-exporter");
    assertNotNull(byId);
  }

  @Test
  void testExportExecution() throws Exception {
    TestSubject subject = new TestSubject();
    DiagramExportOptions options = new DiagramExportOptions(null, "MERMAID");
    IExportContext context =
        new ExportContext(new Variables(), null, null, IExportContext.ExportEnvironment.TEST);

    DiagramExportResult result = service.export(subject, options, context);
    assertTrue(result.isSuccess());
    assertNotNull(result.getContent());
    assertTrue(result.getContent().contains("sample"));
    assertEquals("text/vnd.mermaid", result.getMimeType());
  }

  @Test
  void testSubjectLoader() throws Exception {
    IDiagramSubjectLoader loader =
        new IDiagramSubjectLoader() {
          @Override
          public boolean supportsFile(String filename) {
            return filename != null && filename.endsWith(".testfile");
          }

          @Override
          public Object loadSubject(
              String filename, IHopMetadataProvider metadataProvider, IVariables variables) {
            TestSubject s = new TestSubject();
            s.name = "loaded-from-file";
            return s;
          }
        };

    service.registerSubjectLoader(loader);
    try {
      assertNotNull(service.findSubjectLoader("demo.testfile"));

      DiagramExportOptions options = new DiagramExportOptions(null, "MERMAID");
      IExportContext context =
          new ExportContext(new Variables(), null, null, IExportContext.ExportEnvironment.TEST);

      DiagramExportResult result = service.exportFile("demo.testfile", options, context);
      assertTrue(result.isSuccess());
      assertTrue(result.getContent().contains("loaded-from-file"));
    } finally {
      service.unregisterSubjectLoader(loader);
    }
  }
}
