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
package org.apache.hop.core.plugins.manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginManifestScannerTest {

  @TempDir Path hopHome;

  @Test
  void findsPluginAndEngineManifestsAndComputesOwners() throws Exception {
    write(
        manifest("hop-databases-cratedb", "plugins/databases/cratedb")
            .files(
                "plugins/databases/cratedb/hop-databases-cratedb-1.jar",
                "lib/core/shared-1.jar",
                "lib/jdbc/crate-jdbc-1.jar"));
    write(
        manifest("hop-databases-postgresql", "plugins/databases/postgresql")
            .files(
                "plugins/databases/postgresql/hop-databases-postgresql-1.jar",
                "lib/core/shared-1.jar"));
    write(manifest("hop-core", null).files("lib/core/hop-core-1.jar", "lib/core/shared-1.jar"));
    // Something in lib/core that is not a manifest, and a stray json deeper than a plugin folder
    Files.writeString(hopHome.resolve("lib/core/notes.json"), "{}");
    Files.createDirectories(hopHome.resolve("plugins/databases/cratedb/lib/deep/deeper/deepest"));
    Files.writeString(
        hopHome.resolve("plugins/databases/cratedb/lib/deep/deeper/deepest/plugin-manifest.json"),
        "{}");

    List<PluginManifest> manifests = PluginManifestScanner.scan(hopHome);

    assertEquals(
        List.of("hop-core", "hop-databases-cratedb", "hop-databases-postgresql"),
        manifests.stream().map(PluginManifest::getArtifactId).toList());

    Map<String, List<PluginManifest>> owners = PluginManifestScanner.owners(manifests);
    assertEquals(3, owners.get("lib/core/shared-1.jar").size());
    assertEquals(1, owners.get("lib/jdbc/crate-jdbc-1.jar").size());
    assertEquals(
        "hop-databases-cratedb", owners.get("lib/jdbc/crate-jdbc-1.jar").get(0).getArtifactId());
  }

  private static Builder manifest(String artifactId, String folder) {
    PluginManifest manifest = new PluginManifest();
    manifest.setGroupId("org.apache.hop");
    manifest.setArtifactId(artifactId);
    manifest.setVersion("1");
    manifest.setFolder(folder);
    manifest.setKind(folder == null ? PluginManifest.Kind.ENGINE : PluginManifest.Kind.PLUGIN);
    return new Builder(manifest);
  }

  private void write(Builder builder) throws IOException {
    Path file = hopHome.resolve(builder.manifest.getManifestPath());
    Files.createDirectories(file.getParent());
    Files.write(file, builder.manifest.toJson());
  }

  private record Builder(PluginManifest manifest) {
    Builder files(String... paths) {
      for (String path : paths) {
        manifest
            .getFiles()
            .add(
                new ManifestFile(path, PluginManifestGenerator.locate(path, manifest.getFolder())));
      }
      return this;
    }
  }
}
