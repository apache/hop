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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.ram.RamFileProvider;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.vfs.HopVfsNamespaces;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A pipeline on the native Spark engine writes through a named VFS connection that only the
 * metadata of its run holds, the way a pipeline exported to a Hop Server carries its connections.
 * The process wide file system manager does not know the connection, so without a namespace of its
 * own the path silently becomes a local folder called {@code vfs9591:}.
 *
 * <p>Text File Output has no native Spark handler: it runs as Hop's own transform on the Spark
 * partitions, so this also covers the threads Spark runs its tasks on.
 */
class SparkPipelineEngineNamedVfsConnectionTest {

  private static final String CONNECTION = "vfs9591";

  /** Every file the named connection was asked for. */
  private static final List<String> RESOLVED = new CopyOnWriteArrayList<>();

  @TempDir Path tempDir;

  private IPlugin registeredPlugin;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @BeforeEach
  void registerTheConnection() throws Exception {
    RESOLVED.clear();
    registeredPlugin = namedConnectionPlugin();
    PluginRegistry.getInstance().registerPlugin(VfsPluginType.class, registeredPlugin);
    // The process wide manager is built without the run's metadata, like on a Hop Server
    HopMetadataInstance.setMetadataProvider(null);
    HopVfs.reset();
  }

  @AfterEach
  void removeTheConnection() {
    PluginRegistry.getInstance().removePlugin(VfsPluginType.class, registeredPlugin);
    HopVfsNamespaces.reset();
    HopVfs.reset();
  }

  @Test
  void aHopTransformOnSparkWritesThroughTheRunsNamedConnection() throws Exception {
    Path input = tempDir.resolve("input.csv");
    Files.writeString(input, "id,name\n1,one\n2,two\n3,three\n", StandardCharsets.UTF_8);

    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    PipelineRunConfiguration runConfiguration = sparkLocal();
    metadata.getSerializer(PipelineRunConfiguration.class).save(runConfiguration);

    PipelineMeta pipelineMeta =
        new PipelineMeta(
            new ByteArrayInputStream(
                pipelineXml(input.toUri().toString()).getBytes(StandardCharsets.UTF_8)),
            metadata,
            new Variables());

    SparkPipelineEngine engine =
        new SparkPipelineEngine(pipelineMeta, new LoggingObject("test"), new Variables());
    engine.setMetadataProvider(metadata);
    engine.setPipelineRunConfiguration(runConfiguration);
    engine.prepareExecution();
    engine.startThreads();
    engine.waitUntilFinished();

    assertEquals(
        0,
        engine.getErrors(),
        "The Spark run failed: without a namespace of its own, "
            + CONNECTION
            + ":///rows resolves to a local folder named '"
            + CONNECTION
            + ":'");
    assertFalse(
        Files.exists(Paths.get(CONNECTION + ":")),
        "The output went to a local folder named after the connection");
    assertTrue(
        RESOLVED.stream().anyMatch(uri -> uri.startsWith(CONNECTION + ":///rows")),
        "Text File Output never reached the named connection, it resolved " + RESOLVED);
    assertEquals(0, HopVfsNamespaces.size(), "The run did not let go of its namespace");
  }

  private static PipelineRunConfiguration sparkLocal() {
    SparkPipelineRunConfiguration spark = new SparkPipelineRunConfiguration();
    spark.setSparkMaster("local[2]");
    spark.setSparkAppName("hop-named-vfs-connection-test");
    spark.setSparkConfigs("spark.ui.enabled=false\nspark.driver.host=localhost");
    PipelineRunConfiguration runConfiguration = new PipelineRunConfiguration();
    runConfiguration.setName("spark-local");
    runConfiguration.setEngineRunConfiguration(spark);
    return runConfiguration;
  }

  /**
   * A VFS plugin serving one named connection, but only from metadata handed to it: the process
   * wide manager, built without the run's metadata, does not get it.
   */
  private static IPlugin namedConnectionPlugin() {
    IVfs vfs =
        new IVfs() {
          @Override
          public String[] getUrlSchemes() {
            return new String[] {};
          }

          @Override
          public FileProvider getProvider() {
            return null;
          }

          @Override
          public Map<String, FileProvider> getProviders(IVariables variables) {
            return Map.of();
          }

          @Override
          public Map<String, FileProvider> getProviders(
              IVariables variables, IHopMetadataProvider metadataProvider) {
            if (metadataProvider == null) {
              return Map.of();
            }
            return Map.of(CONNECTION, new RecordingRamFileProvider());
          }
        };

    IVfsPlugin plugin = mock(IVfsPlugin.class);
    when(plugin.getIds()).thenReturn(new String[] {"test-named-connection-9591"});
    when(plugin.getName()).thenReturn("Test named connection");
    when(plugin.getMainType()).thenReturn((Class) IVfs.class);
    when(plugin.loadClass(IVfs.class)).thenReturn(vfs);
    return plugin;
  }

  /** What {@link PluginRegistry} loads a class through. */
  private interface IVfsPlugin extends IClassLoadingPlugin, IPlugin {}

  /** An in-memory store that notes every file it is asked for. */
  private static final class RecordingRamFileProvider extends RamFileProvider {
    @Override
    public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions options)
        throws FileSystemException {
      RESOLVED.add(uri);
      return super.findFile(baseFile, uri, options);
    }
  }

  /** Text File Input reads a local file, Text File Output writes it to the named connection. */
  private static String pipelineXml(String inputUri) {
    return """
        <pipeline>
          <info>
            <name>named-vfs-connection</name>
            <pipeline_type>Normal</pipeline_type>
          </info>
          <order>
            <hop><from>read</from><to>write</to><enabled>Y</enabled></hop>
          </order>
          <transform>
            <name>read</name>
            <type>TextFileInput2</type>
            <file_type>CSV</file_type>
            <compression>None</compression>
            <separator>,</separator>
            <enclosure>"</enclosure>
            <header>Y</header>
            <nr_headerlines>1</nr_headerlines>
            <format>mixed</format>
            <encoding>UTF-8</encoding>
            <date_format_lenient>Y</date_format_lenient>
            <date_format_locale>en_US</date_format_locale>
            <files>
              <file>
                <name>%s</name>
                <file_required>Y</file_required>
                <include_subfolders>N</include_subfolders>
              </file>
            </files>
            <fields>
              <field>
                <name>id</name>
                <type>String</type>
                <position>-1</position>
                <length>-1</length>
                <precision>-1</precision>
                <trim_type>none</trim_type>
                <repeat>N</repeat>
              </field>
              <field>
                <name>name</name>
                <type>String</type>
                <position>-1</position>
                <length>-1</length>
                <precision>-1</precision>
                <trim_type>none</trim_type>
                <repeat>N</repeat>
              </field>
            </fields>
            <copies>1</copies>
          </transform>
          <transform>
            <name>write</name>
            <type>TextFileOutput</type>
            <separator>,</separator>
            <enclosure>"</enclosure>
            <header>Y</header>
            <format>UNIX</format>
            <encoding>UTF-8</encoding>
            <create_parent_folder>Y</create_parent_folder>
            <file>
              <name>%s:///rows</name>
              <extension>csv</extension>
              <do_not_open_new_file_init>Y</do_not_open_new_file_init>
              <append>N</append>
              <split>N</split>
              <haspartno>N</haspartno>
            </file>
            <fields>
              <field><name>id</name><type>String</type></field>
              <field><name>name</name><type>String</type></field>
            </fields>
            <copies>1</copies>
          </transform>
        </pipeline>
        """
        .formatted(inputUri, CONNECTION);
  }
}
