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

package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A metadata file in a folder named after a legacy key of a renamed type (issue #5597) is still
 * linted: the folder name resolves to the renamed type.
 */
class HopMetadataFileLoaderLegacyKeysTest {

  /** A metadata type which was renamed from key "RenamedType" to "renamed-type". */
  @Getter
  @Setter
  @HopMetadata(
      name = "Renamed type",
      key = "renamed-type",
      legacyKeys = {"RenamedType"})
  public static class RenamedType extends HopMetadataBase implements IHopMetadata {
    @HopMetadataProperty private String description;

    public RenamedType() {}
  }

  private static IHopMetadataProvider provider(Path metadataFolder) {
    return new JsonMetadataProvider(
        new HopTwoWayPasswordEncoder(),
        metadataFolder.toString(),
        Variables.getADefaultVariableSpace()) {
      @Override
      public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
        return List.of((Class<T>) (Class<?>) RenamedType.class);
      }
    };
  }

  private static File write(Path metadataFolder, String key, String name, String description)
      throws Exception {
    Path typeFolder = metadataFolder.resolve(key);
    Files.createDirectories(typeFolder);
    Path file = typeFolder.resolve(name + ".json");
    Files.writeString(
        file,
        "{\"name\":\"" + name + "\",\"description\":\"" + description + "\"}",
        StandardCharsets.UTF_8);
    return file.toFile();
  }

  @Test
  void fileInLegacyFolderIsLoaded(@TempDir Path dir) throws Exception {
    Path metadataFolder = dir.resolve("metadata");
    File file = write(metadataFolder, "RenamedType", "old", "legacy");

    HopMetadataFileLoader.MetadataLoad load =
        HopMetadataFileLoader.read(file, provider(metadataFolder));

    assertFalse(load.isFailure(), load.error());
    RenamedType loaded = assertInstanceOf(RenamedType.class, load.object());
    assertEquals("old", loaded.getName());
    assertEquals("legacy", loaded.getDescription());
  }

  @Test
  void fileInCurrentFolderIsLoaded(@TempDir Path dir) throws Exception {
    Path metadataFolder = dir.resolve("metadata");
    write(metadataFolder, "RenamedType", "legacy-only", "legacy");
    File file = write(metadataFolder, "renamed-type", "new", "current");

    HopMetadataFileLoader.MetadataLoad load =
        HopMetadataFileLoader.read(file, provider(metadataFolder));

    RenamedType loaded = assertInstanceOf(RenamedType.class, load.object());
    assertEquals("current", loaded.getDescription());
  }
}
