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
package org.apache.hop.vfs.sftp.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.MetadataRefactorUtil;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.refactor.MetadataReferenceFinder;
import org.apache.hop.metadata.refactor.MetadataReferenceResult;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Renaming an SFTP connection has to take the pipelines and workflows pointing at it along, in the
 * transform as well as in both actions.
 */
class SftpConnectionRenameTest {

  private static final String METADATA_KEY = "sftp-connection";

  private MemoryMetadataProvider metadataProvider;
  private MetadataReferenceFinder finder;

  @TempDir private Path projectFolder;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance().registerType(MetadataPluginType.getInstance());
  }

  @BeforeEach
  void setUp() {
    metadataProvider = new MemoryMetadataProvider();
    finder = new MetadataReferenceFinder(metadataProvider);
  }

  /** Without these two the rename dialog never offers to update anything. */
  @Test
  void testTheTypeIsWiredForGlobalReplace() {
    assertTrue(
        MetadataRefactorUtil.supportsGlobalReplace(metadataProvider, METADATA_KEY),
        "the SFTP connection should support global replace");
    assertEquals(
        HopMetadataPropertyType.VFS_SFTP_CONNECTION,
        MetadataRefactorUtil.getPropertyTypeForMetadataKey(metadataProvider, METADATA_KEY));
  }

  @Test
  void testReferencesAreFoundAndRenamedInAPipeline() throws Exception {
    Path pipeline = writeFile("upload.hpl", pipelineReferencing("prod-sftp"));

    List<MetadataReferenceResult> results =
        finder.findReferences(METADATA_KEY, "prod-sftp", List.of(projectFolder.toString()));
    assertEquals(1, results.size());
    assertEquals(1, results.get(0).getReferenceCount());

    finder.replaceReferences(METADATA_KEY, results, "prod-sftp", "acceptance-sftp");

    String xml = Files.readString(pipeline);
    assertTrue(xml.contains("<connection>acceptance-sftp</connection>"), xml);
    assertFalse(xml.contains("prod-sftp"), xml);
  }

  @Test
  void testReferencesAreFoundAndRenamedInBothActions() throws Exception {
    Path workflow = writeFile("transfer.hwf", workflowReferencing("prod-sftp"));

    List<MetadataReferenceResult> results =
        finder.findReferences(METADATA_KEY, "prod-sftp", List.of(projectFolder.toString()));
    assertEquals(1, results.size());
    // the put action and the get action both point at the connection
    assertEquals(2, results.get(0).getReferenceCount());

    finder.replaceReferences(METADATA_KEY, results, "prod-sftp", "acceptance-sftp");

    String xml = Files.readString(workflow);
    assertEquals(2, xml.split("<connection>acceptance-sftp</connection>", -1).length - 1, xml);
    assertFalse(xml.contains("prod-sftp"), xml);
  }

  /** A connection of another name in the same file has to stay as it is. */
  @Test
  void testOnlyTheRenamedConnectionIsTouched() throws Exception {
    Path pipeline = writeFile("upload.hpl", pipelineReferencing("prod-sftp"));
    Path other = writeFile("other.hpl", pipelineReferencing("dev-sftp"));

    List<MetadataReferenceResult> results =
        finder.findReferences(METADATA_KEY, "prod-sftp", List.of(projectFolder.toString()));
    finder.replaceReferences(METADATA_KEY, results, "prod-sftp", "acceptance-sftp");

    assertTrue(Files.readString(pipeline).contains("acceptance-sftp"));
    assertTrue(Files.readString(other).contains("<connection>dev-sftp</connection>"));
  }

  private Path writeFile(String name, String content) throws Exception {
    Path file = projectFolder.resolve(name);
    Files.writeString(file, content);
    return file;
  }

  private String pipelineReferencing(String connectionName) {
    return "<pipeline><info><name>upload</name></info><transform>"
        + "<name>SFTP Put</name><type>SFTPPut</type>"
        + "<connection>"
        + connectionName
        + "</connection>"
        + "<sourceFileFieldName>filename</sourceFileFieldName>"
        + "</transform></pipeline>";
  }

  private String workflowReferencing(String connectionName) {
    return "<workflow><name>transfer</name><actions>"
        + "<action><name>Put</name><type>SFTPPUT</type><connection>"
        + connectionName
        + "</connection></action>"
        + "<action><name>Get</name><type>SFTP</type><connection>"
        + connectionName
        + "</connection></action>"
        + "</actions></workflow>";
  }
}
