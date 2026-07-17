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

package org.apache.hop.reflection.export;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.reflection.pipeline.meta.PipelineLog;
import org.apache.hop.reflection.pipeline.meta.PipelineToLogLocation;
import org.apache.hop.reflection.workflow.meta.WorkflowLog;
import org.apache.hop.resource.ResourceUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves that pipelines referenced from metadata objects (here a {@link PipelineLog}) are bundled
 * into the resource-export ZIP and that their references are rewritten to resolve inside the
 * archive on a remote Hop Server. See issue #3368.
 *
 * <p>Before the fix the referenced pipelines were missing from the ZIP and the metadata still
 * pointed at the original local path, so the server logged "... pipeline file couldn't be found to
 * execute."
 */
class MetadataResourceExportTest {

  private static final String MINIMAL_PIPELINE =
      "<pipeline><info><name>%s</name></info>"
          + "<order></order><transform_error_handling></transform_error_handling></pipeline>";

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        PipelineLog.class.getName(), MetadataPluginType.class, HopMetadata.class);
    registry.registerPluginClass(
        WorkflowLog.class.getName(), MetadataPluginType.class, HopMetadata.class);
    registry.registerPluginClass(
        HopFileReference.class.getName(), MetadataPluginType.class, HopMetadata.class);
  }

  /**
   * Minimal metadata type holding a {@link HopMetadataPropertyType#HOP_FILE} reference, used to
   * prove the export decides pipeline-vs-workflow from the file extension.
   */
  @HopMetadata(key = "test-hop-file-reference", name = "Test HOP_FILE reference")
  public static class HopFileReference extends HopMetadataBase implements IHopMetadata {
    @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.HOP_FILE)
    private String referencedFile;

    public String getReferencedFile() {
      return referencedFile;
    }

    public void setReferencedFile(String referencedFile) {
      this.referencedFile = referencedFile;
    }
  }

  private File writeWorkflow(File dir, String name) throws Exception {
    File file = new File(dir, name + ".hwf");
    Files.write(
        file.toPath(),
        ("<workflow><name>" + name + "</name><actions></actions><hops></hops></workflow>")
            .getBytes(StandardCharsets.UTF_8));
    return file;
  }

  private File writePipeline(File dir, String name) throws Exception {
    File file = new File(dir, name + ".hpl");
    Files.write(
        file.toPath(), String.format(MINIMAL_PIPELINE, name).getBytes(StandardCharsets.UTF_8));
    return file;
  }

  @Test
  void referencedPipelinesAreBundledAndRewritten(@TempDir File tempDir) throws Exception {
    IVariables variables = new Variables();

    // Two pipelines referenced from a single Pipeline Log metadata object:
    //  - the main logging pipeline (direct String field)
    //  - an extra logging location (String field inside a List<PipelineToLogLocation>)
    File logPipeline = writePipeline(tempDir, "pipeline-log");
    File extraLogPipeline = writePipeline(tempDir, "extra-log");
    File mainFile = writePipeline(tempDir, "main");

    IHopMetadataProvider provider = new MemoryMetadataProvider();
    PipelineLog pipelineLog = new PipelineLog("test-log");
    pipelineLog.setEnabled(true);
    pipelineLog.setPipelineFilename(logPipeline.getAbsolutePath());
    pipelineLog
        .getPipelinesToLog()
        .add(new PipelineToLogLocation(extraLogPipeline.getAbsolutePath()));
    provider.getSerializer(PipelineLog.class).save(pipelineLog);

    PipelineMeta mainMeta = new PipelineMeta(mainFile.getAbsolutePath(), provider, variables);

    File zip = new File(tempDir, "export.zip");
    ResourceUtil.serializeResourceExportInterface(
        zip.getAbsolutePath(),
        mainMeta,
        variables,
        provider,
        null,
        null,
        null,
        null,
        new HashMap<>());

    List<String> entries = new ArrayList<>();
    String metadataJson = null;
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip.toPath()))) {
      ZipEntry e;
      while ((e = zis.getNextEntry()) != null) {
        entries.add(e.getName());
        if ("metadata.json".equals(e.getName())) {
          metadataJson = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    }

    // The main pipeline plus BOTH referenced logging pipelines must be bundled in the ZIP.
    long hplCount = entries.stream().filter(name -> name.endsWith(".hpl")).count();
    assertEquals(3, hplCount, "Expected main + 2 referenced pipelines in the ZIP, got: " + entries);

    // The metadata references must be rewritten to resolve inside the archive on the server,
    // not left pointing at the original local paths.
    assertFalse(
        metadataJson.contains(logPipeline.getAbsolutePath()),
        "metadata.json still contains the original pipeline path: " + metadataJson);
    assertFalse(
        metadataJson.contains(extraLogPipeline.getAbsolutePath()),
        "metadata.json still contains the original list pipeline path: " + metadataJson);
    // (JSON escapes '/' as '\/', so match the variable name only.)
    assertTrue(
        metadataJson.contains("${Internal.Entry.Current.Folder}"),
        "metadata.json references were not rewritten to the archive folder: " + metadataJson);
  }

  @Test
  void referencedWorkflowIsBundledAndRewritten(@TempDir File tempDir) throws Exception {
    IVariables variables = new Variables();

    // A Workflow Log references a pipeline (PIPELINE_FILE String) and a workflow
    // (WORKFLOW_FILE inside a List<String>).
    File logPipeline = writePipeline(tempDir, "wf-log-pipeline");
    File loggedWorkflow = writeWorkflow(tempDir, "logged-workflow");
    File mainFile = writePipeline(tempDir, "main");

    IHopMetadataProvider provider = new MemoryMetadataProvider();
    WorkflowLog workflowLog = new WorkflowLog("test-workflow-log");
    workflowLog.setEnabled(true);
    workflowLog.setPipelineFilename(logPipeline.getAbsolutePath());
    workflowLog.getWorkflowToLog().add(loggedWorkflow.getAbsolutePath());
    provider.getSerializer(WorkflowLog.class).save(workflowLog);

    PipelineMeta mainMeta = new PipelineMeta(mainFile.getAbsolutePath(), provider, variables);

    File zip = new File(tempDir, "export.zip");
    ResourceUtil.serializeResourceExportInterface(
        zip.getAbsolutePath(),
        mainMeta,
        variables,
        provider,
        null,
        null,
        null,
        null,
        new HashMap<>());

    List<String> entries = new ArrayList<>();
    String metadataJson = null;
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip.toPath()))) {
      ZipEntry e;
      while ((e = zis.getNextEntry()) != null) {
        entries.add(e.getName());
        if ("metadata.json".equals(e.getName())) {
          metadataJson = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    }

    // The referenced pipeline must be bundled (main + log pipeline) and the workflow too.
    assertEquals(
        2,
        entries.stream().filter(name -> name.endsWith(".hpl")).count(),
        "Expected main + referenced log pipeline in the ZIP, got: " + entries);
    assertEquals(
        1,
        entries.stream().filter(name -> name.endsWith(".hwf")).count(),
        "Expected the referenced workflow to be bundled, got: " + entries);

    assertFalse(
        metadataJson.contains(logPipeline.getAbsolutePath()),
        "metadata.json still contains the original pipeline path: " + metadataJson);
    assertFalse(
        metadataJson.contains(loggedWorkflow.getAbsolutePath()),
        "metadata.json still contains the original workflow path: " + metadataJson);
    assertTrue(
        metadataJson.contains("${Internal.Entry.Current.Folder}"),
        "metadata.json references were not rewritten to the archive folder: " + metadataJson);
  }

  @Test
  void hopFileReferenceBundlesPipelineOrWorkflowByExtension(@TempDir File tempDir)
      throws Exception {
    IVariables variables = new Variables();

    File referencedPipeline = writePipeline(tempDir, "hopfile-pipeline");
    File referencedWorkflow = writeWorkflow(tempDir, "hopfile-workflow");
    File mainFile = writePipeline(tempDir, "main");

    IHopMetadataProvider provider = new MemoryMetadataProvider();
    HopFileReference pipelineRef = new HopFileReference();
    pipelineRef.setName("hopfile-pipeline-ref");
    pipelineRef.setReferencedFile(referencedPipeline.getAbsolutePath());
    provider.getSerializer(HopFileReference.class).save(pipelineRef);
    HopFileReference workflowRef = new HopFileReference();
    workflowRef.setName("hopfile-workflow-ref");
    workflowRef.setReferencedFile(referencedWorkflow.getAbsolutePath());
    provider.getSerializer(HopFileReference.class).save(workflowRef);

    PipelineMeta mainMeta = new PipelineMeta(mainFile.getAbsolutePath(), provider, variables);

    File zip = new File(tempDir, "export.zip");
    ResourceUtil.serializeResourceExportInterface(
        zip.getAbsolutePath(),
        mainMeta,
        variables,
        provider,
        null,
        null,
        null,
        null,
        new HashMap<>());

    List<String> entries = new ArrayList<>();
    String metadataJson = null;
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip.toPath()))) {
      ZipEntry e;
      while ((e = zis.getNextEntry()) != null) {
        entries.add(e.getName());
        if ("metadata.json".equals(e.getName())) {
          metadataJson = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    }

    // The .hpl HOP_FILE is bundled as a pipeline (main + it), the .hwf as a workflow.
    assertEquals(
        2,
        entries.stream().filter(name -> name.endsWith(".hpl")).count(),
        "Expected main + referenced pipeline in the ZIP, got: " + entries);
    assertEquals(
        1,
        entries.stream().filter(name -> name.endsWith(".hwf")).count(),
        "Expected the referenced workflow to be bundled, got: " + entries);
    assertFalse(
        metadataJson.contains(referencedPipeline.getAbsolutePath()),
        "metadata.json still contains the original HOP_FILE pipeline path: " + metadataJson);
    assertFalse(
        metadataJson.contains(referencedWorkflow.getAbsolutePath()),
        "metadata.json still contains the original HOP_FILE workflow path: " + metadataJson);
  }
}
