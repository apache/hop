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

package org.apache.hop.diagram.command;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.diagram.BaseDiagramExporter;
import org.apache.hop.core.diagram.DiagramExportFormat;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiagramExportCommandTest {

  @TempDir Path tempDir;

  /** A PlantUML exporter for pipelines only, like a plugin outside the built-in SVG and Mermaid. */
  static class PipelinePlantUmlExporter extends BaseDiagramExporter<PipelineMeta> {
    PipelinePlantUmlExporter() {
      id = "test-pipeline-plantuml";
      name = "Test PlantUML";
      format = DiagramExportFormat.PLANTUML;
      fileExtension = "puml";
      supportedSubjectTypes = new Class<?>[] {PipelineMeta.class};
    }

    @Override
    public DiagramExportResult export(
        PipelineMeta subject, DiagramExportOptions options, IExportContext context)
        throws HopException {
      String content = "@startuml\n@enduml\n";
      writeToTarget(options.getTargetFilename(), content, context);
      return DiagramExportResult.success(options.getTargetFilename(), content, "text/plain");
    }
  }

  @BeforeAll
  static void initHop() throws Exception {
    org.apache.hop.core.HopEnvironment.init();
  }

  @Test
  void testListExporters() {
    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setListExporters(true);
    assertDoesNotThrow(cmd::run);
  }

  @Test
  void testExportSinglePipeline() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cli-test-pipeline");
    String pipelineXml = pipelineMeta.getXml(new org.apache.hop.core.variables.Variables());

    Path pipelineFile = tempDir.resolve("test-pipeline.hpl");
    Files.writeString(pipelineFile, pipelineXml);

    Path outputFile = tempDir.resolve("output.mmd");

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setFilename(pipelineFile.toString());
    cmd.setOutputFilename(outputFile.toString());
    cmd.setFormat("MERMAID");

    assertDoesNotThrow(cmd::run);

    assertTrue(Files.exists(outputFile));
    String content = Files.readString(outputFile);
    assertTrue(content.contains("flowchart LR"));
  }

  @Test
  void testExportWithoutExporterForFormatFails() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cli-test-pipeline");
    Path pipelineFile = tempDir.resolve("test-pipeline.hpl");
    Files.writeString(
        pipelineFile, pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));

    // --format pdf, without -o: would be written next to the pipeline as test-pipeline.pdf
    DiagramExportCommand pdfFormat = new DiagramExportCommand();
    pdfFormat.setFilename(pipelineFile.toString());
    pdfFormat.setFormat("pdf");
    RuntimeException e = assertThrows(RuntimeException.class, pdfFormat::run);
    assertTrue(e.getCause().getMessage().contains("'PDF'"), e.getCause().getMessage());
    assertTrue(e.getCause().getMessage().contains("MERMAID (.mmd)"), e.getCause().getMessage());
    assertTrue(e.getCause().getMessage().contains("SVG (.svg)"), e.getCause().getMessage());
    assertFalse(Files.exists(tempDir.resolve("test-pipeline.pdf")));

    // -o x.pdf, without --format
    Path pdfFile = tempDir.resolve("x.pdf");
    DiagramExportCommand pdfOutput = new DiagramExportCommand();
    pdfOutput.setFilename(pipelineFile.toString());
    pdfOutput.setOutputFilename(pdfFile.toString());
    assertThrows(RuntimeException.class, pdfOutput::run);
    assertFalse(Files.exists(pdfFile));

    // a typo
    Path fooFile = tempDir.resolve("y.foo");
    DiagramExportCommand typo = new DiagramExportCommand();
    typo.setFilename(pipelineFile.toString());
    typo.setOutputFilename(fooFile.toString());
    typo.setFormat("foo");
    assertThrows(RuntimeException.class, typo::run);
    assertFalse(Files.exists(fooFile));
  }

  /** -o x.png, without --format, was written as SVG into x.png. */
  @Test
  void testExportToAnExtensionWithoutExporterFails() throws Exception {
    Path pipelineFile = writePipeline("test-pipeline.hpl");
    Path pngFile = tempDir.resolve("x.png");

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setFilename(pipelineFile.toString());
    cmd.setOutputFilename(pngFile.toString());
    RuntimeException e = assertThrows(RuntimeException.class, cmd::run);

    assertTrue(e.getCause().getMessage().contains("'PNG'"), e.getCause().getMessage());
    assertTrue(e.getCause().getMessage().contains("SVG (.svg)"), e.getCause().getMessage());
    assertFalse(Files.exists(pngFile));
  }

  @Test
  void testExportToAFileWithoutExtensionIsSvg() throws Exception {
    Path pipelineFile = writePipeline("test-pipeline.hpl");
    Path outputFile = tempDir.resolve("diagram");

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setFilename(pipelineFile.toString());
    cmd.setOutputFilename(outputFile.toString());
    assertDoesNotThrow(cmd::run);

    assertTrue(Files.readString(outputFile).contains("<svg"));
  }

  private Path writePipeline(String name) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cli-test-pipeline");
    Path pipelineFile = tempDir.resolve(name);
    Files.writeString(
        pipelineFile, pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));
    return pipelineFile;
  }

  @Test
  void testBatchExportWithoutExporterForFormatFails() throws Exception {
    Path sourceFolder = tempDir.resolve("source");
    Path targetFolder = tempDir.resolve("target");
    Files.createDirectories(sourceFolder);
    Files.createDirectories(targetFolder);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("batch-pipe");
    Files.writeString(
        sourceFolder.resolve("pipe.hpl"),
        pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setSourceFolder(sourceFolder.toString());
    cmd.setTargetFolder(targetFolder.toString());
    cmd.setFormat("pdf");

    assertThrows(RuntimeException.class, cmd::run);
    assertFalse(Files.exists(targetFolder.resolve("pipe.pdf")));
  }

  @Test
  void testBatchExport() throws Exception {
    Path sourceFolder = tempDir.resolve("source");
    Path targetFolder = tempDir.resolve("target");
    Files.createDirectories(sourceFolder);
    Files.createDirectories(targetFolder);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("batch-pipe");
    Files.writeString(
        sourceFolder.resolve("pipe.hpl"),
        pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));

    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("batch-flow");
    Files.writeString(
        sourceFolder.resolve("flow.hwf"),
        workflowMeta.getXml(new org.apache.hop.core.variables.Variables()));

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setSourceFolder(sourceFolder.toString());
    cmd.setTargetFolder(targetFolder.toString());
    cmd.setFormat("SVG");

    assertDoesNotThrow(cmd::run);

    assertTrue(Files.exists(targetFolder.resolve("pipe.svg")));
    assertTrue(Files.exists(targetFolder.resolve("flow.svg")));
  }

  /** -o x.puml, without --format, picks an installed exporter that writes .puml. */
  @Test
  void testExportToAnInstalledExtensionUsesItsExporter() throws Exception {
    PipelinePlantUmlExporter plantUml = new PipelinePlantUmlExporter();
    DiagramExportService.getInstance().registerStaticExporter(plantUml);
    try {
      Path pipelineFile = writePipeline("test-pipeline.hpl");
      Path pumlFile = tempDir.resolve("x.puml");

      DiagramExportCommand cmd = new DiagramExportCommand();
      cmd.setFilename(pipelineFile.toString());
      cmd.setOutputFilename(pumlFile.toString());
      assertDoesNotThrow(cmd::run);
      assertTrue(Files.readString(pumlFile).startsWith("@startuml"));

      // Without -o, the file gets the exporter's extension, not .plantuml
      DiagramExportCommand byFormat = new DiagramExportCommand();
      byFormat.setFilename(pipelineFile.toString());
      byFormat.setFormat("plantuml");
      assertDoesNotThrow(byFormat::run);
      assertTrue(Files.exists(tempDir.resolve("test-pipeline.puml")));
    } finally {
      DiagramExportService.getInstance().unregisterStaticExporter(plantUml);
    }
  }

  /**
   * A format that only pipelines have skips the workflows, and exports every pipeline, rather than
   * stopping at the first workflow.
   */
  @Test
  void testBatchExportSkipsFilesWithoutExporterForFormat() throws Exception {
    PipelinePlantUmlExporter plantUml = new PipelinePlantUmlExporter();
    DiagramExportService.getInstance().registerStaticExporter(plantUml);
    try {
      Path sourceFolder = tempDir.resolve("source");
      Path targetFolder = tempDir.resolve("target");
      Files.createDirectories(sourceFolder);
      Files.createDirectories(targetFolder);

      for (String name : new String[] {"a-pipe", "z-pipe"}) {
        PipelineMeta pipelineMeta = new PipelineMeta();
        pipelineMeta.setName(name);
        Files.writeString(
            sourceFolder.resolve(name + ".hpl"),
            pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));
      }
      WorkflowMeta workflowMeta = new WorkflowMeta();
      workflowMeta.setName("m-flow");
      Files.writeString(
          sourceFolder.resolve("m-flow.hwf"),
          workflowMeta.getXml(new org.apache.hop.core.variables.Variables()));

      DiagramExportCommand cmd = new DiagramExportCommand();
      cmd.setSourceFolder(sourceFolder.toString());
      cmd.setTargetFolder(targetFolder.toString());
      cmd.setFormat("plantuml");
      assertDoesNotThrow(cmd::run);

      assertTrue(Files.exists(targetFolder.resolve("a-pipe.puml")));
      assertTrue(Files.exists(targetFolder.resolve("z-pipe.puml")));
      try (var files = Files.list(targetFolder)) {
        assertEquals(2, files.count());
      }
    } finally {
      DiagramExportService.getInstance().unregisterStaticExporter(plantUml);
    }
  }
}
