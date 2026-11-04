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

package org.apache.hop.metadata.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.memory.SharedMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SharedMetadataTest {

  @TempDir Path tempDir;

  private IVariables variables;

  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginType(MetadataPluginType.class);
    if (registry.findPluginWithId(MetadataPluginType.class, "shared-note") == null) {
      registry.registerPluginClass(
          SharedNote.class.getName(), MetadataPluginType.class, HopMetadata.class);
    }
    variables = new Variables();
    HopMetadataInstance.setSharedMetadataProvider(null);
  }

  @AfterEach
  void clearShared() {
    HopMetadataInstance.setSharedMetadataProvider(null);
  }

  /** The registry is process-wide. Leave shared-note behind and later tests count an extra type. */
  @AfterAll
  static void unregisterSharedNote() {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId(MetadataPluginType.class, "shared-note");
    if (plugin != null) {
      registry.removePlugin(MetadataPluginType.class, plugin);
    }
  }

  @Test
  void localFolderCheckRejectsANamedScheme() {
    assertThrows(HopException.class, () -> SharedMetadata.checkLocalFolder("git-project:///repo"));
    assertThrows(HopException.class, () -> SharedMetadata.checkLocalFolder(""));
  }

  @Test
  void localFolderCheckAllowsAFilePath() throws Exception {
    SharedMetadata.checkLocalFolder(tempDir.toString());
    SharedMetadata.checkLocalFolder("file:///tmp/hop-shared");
  }

  @Test
  void saveIsVisibleToASecondProviderAndSurvivesOnDisk() throws Exception {
    String folder = tempDir.resolve("metadata").toString();
    SharedMetadataProvider first =
        new SharedMetadataProvider(variables, folder, "${HOP_CONFIG_FOLDER}/metadata");
    first.getSerializer(SharedNote.class).save(new SharedNote("vault", "kubernetes"));

    assertTrue(
        Files.exists(tempDir.resolve("metadata").resolve("shared-note").resolve("vault.json")));

    SharedMetadataProvider second =
        new SharedMetadataProvider(variables, folder, "${HOP_CONFIG_FOLDER}/metadata");
    SharedNote loaded = second.getSerializer(SharedNote.class).load("vault");
    assertEquals("kubernetes", loaded.getText());
    assertEquals(SharedMetadataProvider.DESCRIPTION, loaded.getMetadataProviderName());

    second.getSerializer(SharedNote.class).delete("vault");
    assertNull(second.getSerializer(SharedNote.class).load("vault"));
    assertFalse(
        Files.exists(tempDir.resolve("metadata").resolve("shared-note").resolve("vault.json")));
    SharedMetadataProvider third =
        new SharedMetadataProvider(variables, folder, "${HOP_CONFIG_FOLDER}/metadata");
    assertNull(third.getSerializer(SharedNote.class).load("vault"));
  }

  @Test
  void standardProviderPutsSharedFirstAndSkipsTheSameFolder() throws Exception {
    String folder = tempDir.resolve("metadata").toString();
    SharedMetadataProvider shared =
        new SharedMetadataProvider(variables, folder, "${HOP_CONFIG_FOLDER}/metadata");
    HopMetadataInstance.setSharedMetadataProvider(shared);

    variables.setVariable(Const.HOP_METADATA_FOLDER, tempDir.resolve("project").toString());
    MultiMetadataProvider multi = HopMetadataUtil.getStandardHopMetadataProvider(variables);
    assertSame(shared, multi.getProviders().get(0));
    assertEquals(2, multi.getProviders().size());

    variables.setVariable(Const.HOP_METADATA_FOLDER, folder);
    MultiMetadataProvider once = HopMetadataUtil.getStandardHopMetadataProvider(variables);
    assertEquals(1, once.getProviders().size());
    assertSame(shared, once.getProviders().get(0));
  }

  @Test
  void blankSharedProviderLeavesTheProjectFolderAlone() throws Exception {
    variables.setVariable(Const.HOP_METADATA_FOLDER, tempDir.resolve("project").toString());
    MultiMetadataProvider multi = HopMetadataUtil.getStandardHopMetadataProvider(variables);
    assertFalse(multi.getProviders().get(0) instanceof SharedMetadataProvider);
  }

  @Test
  void projectObjectOverridesTheSharedOne() throws Exception {
    String sharedFolder = tempDir.resolve("shared").toString();
    String projectFolder = tempDir.resolve("project").toString();
    SharedMetadataProvider shared =
        new SharedMetadataProvider(variables, sharedFolder, sharedFolder);
    shared.getSerializer(SharedNote.class).save(new SharedNote("db", "from-shared"));

    JsonMetadataProvider project =
        new JsonMetadataProvider(new HopTwoWayPasswordEncoder(), projectFolder, variables);
    project.getSerializer(SharedNote.class).save(new SharedNote("db", "from-project"));

    MultiMetadataProvider multi =
        new MultiMetadataProvider(
            new HopTwoWayPasswordEncoder(), List.of(shared, project), variables);
    SharedNote loaded = multi.getSerializer(SharedNote.class).load("db");
    assertEquals("from-project", loaded.getText());
    assertEquals(project.getDescription(), loaded.getMetadataProviderName());
  }

  @HopMetadata(
      key = "shared-note",
      name = "Shared note",
      description = "Test metadata object",
      image = "org/apache/hop/images/dummy.svg")
  public static class SharedNote extends HopMetadataBase implements IHopMetadata {
    @HopMetadataProperty private String text;

    public SharedNote() {}

    public SharedNote(String name, String text) {
      super(name);
      this.text = text;
    }

    public String getText() {
      return text;
    }

    public void setText(String text) {
      this.text = text;
    }
  }
}
