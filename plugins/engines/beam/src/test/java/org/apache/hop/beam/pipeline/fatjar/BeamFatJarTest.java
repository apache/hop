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

package org.apache.hop.beam.pipeline.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.function.Predicate;
import org.apache.hop.core.plugins.manifest.ManifestFile;
import org.apache.hop.core.plugins.manifest.PluginManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BeamFatJarTest {

  @TempDir File install;

  @Test
  void aBeamJarLeavesOutTheNativeSparkEngine() throws Exception {
    manifest(
        "plugins/engines/spark",
        "plugins/engines/spark/hop-engines-spark-2.21.0.jar",
        "plugins/engines/spark/lib/scala-library-2.13.17.jar",
        "lib/core/netty-codec-mqtt-4.2.19.Final.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");
    manifest(
        "plugins/engines/beam",
        "plugins/engines/beam/hop-engines-beam-2.21.0.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");

    Predicate<File> sparkEngine = BeamFatJar.sparkEngineFiles(install).asExclusion();

    assertTrue(sparkEngine.test(new File(install, "plugins/engines/spark")));
    assertTrue(sparkEngine.test(new File(install, "lib/core/netty-codec-mqtt-4.2.19.Final.jar")));
    // Beam needs it too
    assertFalse(sparkEngine.test(new File(install, "lib/core/netty-buffer-4.2.19.Final.jar")));
    assertFalse(sparkEngine.test(new File(install, "plugins/engines/beam")));
  }

  /** A plugin folder with a manifest that claims these files, which are created as well. */
  private void manifest(String folder, String... paths) throws Exception {
    PluginManifest manifest = new PluginManifest();
    manifest.setKind(PluginManifest.Kind.PLUGIN);
    manifest.setGroupId("org.apache.hop");
    manifest.setArtifactId(folder.substring(folder.lastIndexOf('/') + 1));
    manifest.setFolder(folder);
    for (String path : paths) {
      File file = new File(install, path);
      Files.createDirectories(file.getParentFile().toPath());
      if (!file.exists()) {
        Files.createFile(file.toPath());
      }
      manifest.getFiles().add(new ManifestFile(path, ManifestFile.Location.SHARED));
    }
    try (OutputStream out =
        Files.newOutputStream(new File(install, folder + "/plugin-manifest.json").toPath())) {
      manifest.write(out);
    }
  }

  @Test
  void nativeTokenDetection() {
    assertTrue(BeamFatJar.isNativeSparkClientVersion("native"));
    assertTrue(BeamFatJar.isNativeSparkClientVersion("NATIVE"));
    assertTrue(BeamFatJar.isNativeSparkClientVersion(" native "));
    assertTrue(BeamFatJar.isNativeSparkClientVersion("native-provided"));
    assertTrue(BeamFatJar.isNativeProvidedSparkClientVersion("native-provided"));
    assertFalse(BeamFatJar.isNativeProvidedSparkClientVersion("native"));
    assertFalse(BeamFatJar.isNativeSparkClientVersion(null));
    assertFalse(BeamFatJar.isNativeSparkClientVersion(""));
    assertFalse(BeamFatJar.isNativeSparkClientVersion("3.5.8"));
  }

  @Test
  void aBeamJarIgnoresTheFormerNativeTokens() {
    String before = System.getProperty("HOP_SPARK_CLIENT_VERSION");
    try {
      // Older setups set HOP_SPARK_CLIENT_VERSION=native-provided for a native Spark jar
      System.setProperty("HOP_SPARK_CLIENT_VERSION", "native-provided");
      assertNull(BeamFatJar.beamSparkClientVersion(null));
      assertNull(BeamFatJar.beamSparkClientVersion("native"));
      assertEquals("3.5.8", BeamFatJar.beamSparkClientVersion("3.5.8"));
      System.setProperty("HOP_SPARK_CLIENT_VERSION", "3.4.4");
      assertEquals("3.4.4", BeamFatJar.beamSparkClientVersion(null));
    } finally {
      if (before == null) {
        System.clearProperty("HOP_SPARK_CLIENT_VERSION");
      } else {
        System.setProperty("HOP_SPARK_CLIENT_VERSION", before);
      }
    }
  }
}
