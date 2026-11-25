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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hop.core.plugins.manifest.ManifestFile;
import org.apache.hop.core.plugins.manifest.PluginManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginOwnedFilesTest {

  @TempDir File install;

  @Test
  void onlyTheFilesNoOtherPluginClaimsAreLeftOut() throws Exception {
    plugin(
        "plugins/engines/beam",
        "plugins/engines/beam/hop-engines-beam-2.21.0.jar",
        "lib/core/beam-sdks-java-core-2.74.0.jar",
        "lib/core/flink-core-1.19.3.jar",
        "lib/core/grpc-api-1.76.0.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");
    plugin(
        "plugins/tech/google",
        "plugins/tech/google/hop-tech-google-2.21.0.jar",
        "lib/core/grpc-api-1.76.0.jar");
    plugin(
        "plugins/engines/spark",
        "plugins/engines/spark/hop-engines-spark-2.21.0.jar",
        "plugins/engines/spark/lib/spark-sql_2.13-4.1.3.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");

    PluginOwnedFiles beam = PluginOwnedFiles.onlyInstalledBy(install, "plugins/engines/beam");

    assertEquals(
        Set.of(
            "plugins/engines/beam/plugin-manifest.json",
            "plugins/engines/beam/hop-engines-beam-2.21.0.jar",
            "lib/core/beam-sdks-java-core-2.74.0.jar",
            "lib/core/flink-core-1.19.3.jar"),
        beam.getPaths());
    assertEquals(3, beam.getJarCount());
    assertTrue(beam.getFoldersWithoutManifest().isEmpty());

    Predicate<File> exclusion = beam.asExclusion();
    assertTrue(exclusion.test(new File(install, "plugins/engines/beam")));
    assertTrue(exclusion.test(new File(install, "lib/core/flink-core-1.19.3.jar")));
    // Shared with the Google plugin and with the Spark engine
    assertFalse(exclusion.test(new File(install, "lib/core/grpc-api-1.76.0.jar")));
    assertFalse(exclusion.test(new File(install, "lib/core/netty-buffer-4.2.19.Final.jar")));
    assertFalse(exclusion.test(new File(install, "plugins/engines/spark")));
    assertFalse(exclusion.test(new File(install, "lib/core")));
  }

  @Test
  void aPluginWithoutManifestOnlyHasItsFolderLeftOut() throws Exception {
    touch("plugins/engines/beam/hop-engines-beam-2.21.0.jar");
    touch("lib/core/flink-core-1.19.3.jar");
    plugin("plugins/tech/google", "plugins/tech/google/hop-tech-google-2.21.0.jar");

    PluginOwnedFiles beam = PluginOwnedFiles.onlyInstalledBy(install, "plugins/engines/beam");

    assertEquals(List.of("plugins/engines/beam"), beam.getFoldersWithoutManifest());
    assertTrue(beam.getPaths().isEmpty());
    Predicate<File> exclusion = beam.asExclusion();
    assertTrue(exclusion.test(new File(install, "plugins/engines/beam")));
    assertFalse(exclusion.test(new File(install, "lib/core/flink-core-1.19.3.jar")));
  }

  @Test
  void aPluginThatIsNotInstalledLeavesNothingOut() throws Exception {
    plugin(
        "plugins/tech/google",
        "plugins/tech/google/hop-tech-google-2.21.0.jar",
        "lib/core/grpc-api-1.76.0.jar");

    PluginOwnedFiles beam = PluginOwnedFiles.onlyInstalledBy(install, "plugins/engines/beam");

    assertTrue(beam.getPaths().isEmpty());
    assertTrue(beam.getFoldersWithoutManifest().isEmpty());
    assertFalse(beam.asExclusion().test(new File(install, "lib/core/grpc-api-1.76.0.jar")));
  }

  @Test
  void theInstallationCanBeTheWorkingDirectoryRelativeForm() throws Exception {
    plugin("plugins/engines/beam", "lib/core/flink-core-1.19.3.jar");

    Predicate<File> exclusion =
        PluginOwnedFiles.onlyInstalledBy(install, "plugins/engines/beam").asExclusion();

    // InstalledJars hands over files built from the installation folder, absolute or not
    File relativeToInstall =
        new File(install.getPath() + "/./lib/core/../core/flink-core-1.19.3.jar");
    assertTrue(exclusion.test(relativeToInstall));
  }

  /** A plugin folder with its manifest, claiming these files, which are created as well. */
  private void plugin(String folder, String... paths) throws IOException {
    PluginManifest manifest = new PluginManifest();
    manifest.setKind(PluginManifest.Kind.PLUGIN);
    manifest.setGroupId("org.apache.hop");
    manifest.setArtifactId(folder.substring(folder.lastIndexOf('/') + 1));
    manifest.setVersion("2.21.0");
    manifest.setFolder(folder);
    manifest
        .getFiles()
        .add(
            new ManifestFile(
                folder + "/" + PluginManifest.PLUGIN_FILE_NAME, ManifestFile.Location.PLUGIN));
    for (String path : paths) {
      touch(path);
      ManifestFile.Location location =
          path.startsWith("lib/core/")
              ? ManifestFile.Location.SHARED
              : path.startsWith(folder + "/lib/")
                  ? ManifestFile.Location.PRIVATE
                  : ManifestFile.Location.PLUGIN;
      manifest.getFiles().add(new ManifestFile(path, location));
    }
    File file = new File(install, folder + "/" + PluginManifest.PLUGIN_FILE_NAME);
    Files.createDirectories(file.getParentFile().toPath());
    try (OutputStream out = Files.newOutputStream(file.toPath())) {
      manifest.write(out);
    }
  }

  private void touch(String path) throws IOException {
    File file = new File(install, path);
    Files.createDirectories(file.getParentFile().toPath());
    if (!file.exists()) {
      Files.createFile(file.toPath());
    }
  }
}
