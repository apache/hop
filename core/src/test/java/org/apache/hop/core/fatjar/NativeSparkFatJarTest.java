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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import org.apache.hop.core.plugins.manifest.ManifestFile;
import org.apache.hop.core.plugins.manifest.PluginManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeSparkFatJarTest {

  @TempDir File tempDir;

  @Test
  void providedModeExcludesSparkAndScalaRuntime() {
    assertTrue(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("spark-core_2.13-4.1.2.jar")));
    assertTrue(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("spark-sql_2.13-4.1.2.jar")));
    assertTrue(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("scala-library-2.13.17.jar")));
    assertTrue(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("chill_2.13-0.10.0.jar")));
    assertTrue(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("hadoop-client-api-3.4.2.jar")));
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("hop-engines-spark-2.19.0-SNAPSHOT.jar")));
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("beam-runners-direct-java-2.74.0.jar")));
    // Lakehouse connectors ship in engines-spark lib and must ride the native-provided fat jar
    // so cluster submit does not require a separate --packages step for Delta/Iceberg.
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("delta-spark_4.1_2.13-4.3.1.jar")));
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("delta-storage-4.3.1.jar")));
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("iceberg-spark-runtime-4.1_2.13-1.11.0.jar")));
    assertFalse(
        NativeSparkFatJar.isExcludedForNativeProvidedSparkFatJar(
            new File("unitycatalog-client-0.5.0.jar")));
  }

  @Test
  void leavesOutTheSparkClientPacksOfTheBeamSparkRunner() throws Exception {
    File install = new File(tempDir, "packs");
    String[] kept = {
      "lib/core/hop-core-2.21.0.jar",
      "plugins/engines/spark/hop-engines-spark-2.21.0.jar",
      "plugins/engines/spark/lib/spark-sql_2.13-4.1.3.jar",
      "plugins/engines/spark/lib/scala-library-2.13.17.jar"
    };
    String[] dropped = {
      "lib/spark-client/spark-core_2.12-3.5.8.jar",
      // Made by materialize-pack.sh: no manifest claims it
      "lib/spark-clients/3.4.4/spark-core_2.12-3.4.4.jar"
    };
    for (String path : concat(kept, dropped)) {
      File jar = new File(install, path);
      assertTrue(jar.getParentFile().mkdirs() || jar.getParentFile().isDirectory());
      assertTrue(jar.createNewFile());
    }

    java.util.List<String> jars = NativeSparkFatJar.findJarFilenames(install, false);

    for (String path : kept) {
      assertTrue(jars.stream().anyMatch(j -> j.endsWith(path)), path + " in " + jars);
    }
    for (String path : dropped) {
      assertFalse(jars.stream().anyMatch(j -> j.endsWith(path)), path + " in " + jars);
    }
  }

  @Test
  void aBeamEngineWithoutManifestOnlyHasItsFolderLeftOut() throws Exception {
    File install = new File(tempDir, "hop");
    File[] jars = {
      new File(install, "plugins/engines/beam/hop-engines-beam-2.21.0.jar"),
      new File(install, "plugins/engines/beam/lib/beam-sdks-java-core-2.74.0.jar"),
      new File(install, "lib/core/beam-runners-flink-1.19-2.74.0.jar"),
      new File(install, "lib/core/hop-core-2.21.0.jar"),
      new File(install, "plugins/engines/spark/hop-engines-spark-2.21.0.jar")
    };
    for (File jar : jars) {
      assertTrue(jar.getParentFile().mkdirs() || jar.getParentFile().isDirectory());
      assertTrue(jar.createNewFile());
    }

    java.util.List<String> selected = NativeSparkFatJar.findJarFilenames(install, true);

    assertFalse(selected.stream().anyMatch(f -> f.contains("/engines/beam/")), selected::toString);
    // Nothing says this jar is Beam's
    assertTrue(selected.stream().anyMatch(f -> f.endsWith("beam-runners-flink-1.19-2.74.0.jar")));
    assertTrue(selected.stream().anyMatch(f -> f.endsWith("hop-core-2.21.0.jar")));
    assertTrue(selected.stream().anyMatch(f -> f.endsWith("hop-engines-spark-2.21.0.jar")));
  }

  @Test
  void leavesOutTheJarsTheBeamManifestClaimsAndNoOtherPluginDoes() throws Exception {
    File install = new File(tempDir, "manifests");
    manifest(
        install,
        "plugins/engines/beam",
        "plugins/engines/beam/hop-engines-beam-2.21.0.jar",
        "lib/core/beam-sdks-java-core-2.74.0.jar",
        "lib/core/flink-core-1.19.3.jar",
        "lib/core/google-cloud-bigtable-2.70.0.jar",
        "lib/core/grpc-api-1.76.0.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");
    manifest(
        install,
        "plugins/tech/google",
        "plugins/tech/google/hop-tech-google-2.21.0.jar",
        "lib/core/grpc-api-1.76.0.jar");
    manifest(
        install,
        "plugins/engines/spark",
        "plugins/engines/spark/hop-engines-spark-2.21.0.jar",
        "lib/core/netty-buffer-4.2.19.Final.jar");

    java.util.List<String> jars = NativeSparkFatJar.findJarFilenames(install, false);

    assertFalse(jars.stream().anyMatch(j -> j.endsWith("flink-core-1.19.3.jar")), jars::toString);
    assertFalse(
        jars.stream().anyMatch(j -> j.endsWith("beam-sdks-java-core-2.74.0.jar")), jars::toString);
    assertFalse(
        jars.stream().anyMatch(j -> j.endsWith("google-cloud-bigtable-2.70.0.jar")),
        jars::toString);
    assertFalse(jars.stream().anyMatch(j -> j.contains("/engines/beam/")), jars::toString);
    // Shared with the Google plugin and the Spark engine
    assertTrue(jars.stream().anyMatch(j -> j.endsWith("grpc-api-1.76.0.jar")), jars::toString);
    assertTrue(
        jars.stream().anyMatch(j -> j.endsWith("netty-buffer-4.2.19.Final.jar")), jars::toString);
    assertTrue(
        jars.stream().anyMatch(j -> j.endsWith("hop-engines-spark-2.21.0.jar")), jars::toString);
  }

  /** A plugin folder with a manifest that claims these files, which are created as well. */
  private static void manifest(File install, String folder, String... paths) throws Exception {
    PluginManifest manifest = new PluginManifest();
    manifest.setKind(PluginManifest.Kind.PLUGIN);
    manifest.setGroupId("org.apache.hop");
    manifest.setArtifactId(folder.substring(folder.lastIndexOf('/') + 1));
    manifest.setFolder(folder);
    for (String path : paths) {
      File jar = new File(install, path);
      assertTrue(jar.getParentFile().mkdirs() || jar.getParentFile().isDirectory());
      assertTrue(jar.createNewFile() || jar.isFile());
      manifest
          .getFiles()
          .add(
              new ManifestFile(
                  path,
                  path.startsWith("lib/core/")
                      ? ManifestFile.Location.SHARED
                      : ManifestFile.Location.PLUGIN));
    }
    try (OutputStream out =
        java.nio.file.Files.newOutputStream(
            new File(install, folder + "/plugin-manifest.json").toPath())) {
      manifest.write(out);
    }
  }

  private static final File SPARK_LIB = new File("plugins/engines/spark/lib");

  private static final java.util.List<String> TREE =
      java.util.List.of(
          "org.apache.hop:hop-engines-spark:jar:2.21.0-SNAPSHOT",
          "+- jakarta.servlet:jakarta.servlet-api:jar:5.0.0:compile",
          "+- org.apache.hadoop:hadoop-client-runtime:jar:3.4.2:compile",
          "|  +- org.slf4j:slf4j-api:jar:2.0.20:runtime",
          "|  \\- commons-logging:commons-logging:jar:1.4.0:runtime",
          "+- org.apache.spark:spark-sql_2.13:jar:4.1.3:compile",
          "|  +- org.rocksdb:rocksdbjni:jar:9.8.4:compile",
          "|  +- org.tukaani:xz:jar:1.10:compile",
          "|  +- com.ibm.icu:icu4j:jar:77.1:compile",
          "|  +- com.google.protobuf:protobuf-java:jar:4.29.3:compile",
          "|  \\- io.netty:netty-transport-native-epoll:jar:linux-x86_64:4.2.19.Final:compile",
          "|     \\- io.netty:netty-transport:jar:4.2.19.Final:compile",
          "\\- io.delta:delta-spark_4.1_2.13:jar:4.3.1:compile",
          "   \\- io.delta:delta-storage:jar:4.3.1:compile");

  @Test
  void theSparkDistributionIsWhatTheSparkAndHadoopDependenciesBringIn() {
    java.util.Set<String> artifacts = NativeSparkFatJar.parseDistributionArtifacts(TREE);
    assertTrue(
        artifacts.containsAll(
            java.util.Set.of(
                "hadoop-client-runtime",
                "slf4j-api",
                "commons-logging",
                "spark-sql_2.13",
                "rocksdbjni",
                "netty-transport-native-epoll",
                "netty-transport")));
    // Not shipped by Spark: a direct dependency of the plugin, and the lakehouse connectors
    assertFalse(artifacts.contains("jakarta.servlet-api"));
    assertFalse(artifacts.contains("delta-spark_4.1_2.13"));
    assertFalse(artifacts.contains("delta-storage"));
    assertFalse(artifacts.contains("hop-engines-spark"));
  }

  @Test
  void jarsAreMatchedOnTheirArtifactWhateverTheVersion() {
    java.util.Set<String> artifacts = NativeSparkFatJar.parseDistributionArtifacts(TREE);
    assertTrue(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "rocksdbjni-8.3.2.jar"), artifacts));
    assertTrue(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "netty-transport-native-epoll-4.2.19.Final-linux-x86_64.jar"),
            artifacts));
    assertTrue(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "slf4j-api-2.0.17.jar"), artifacts));
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "netty-transport-sctp-4.2.19.Final.jar"), artifacts));
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "hop-core-2.21.0.jar"), artifacts));
    // Outside the engine's Spark runtime a matching name is not a copy of what the cluster has
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File("lib/core/rocksdbjni-8.3.2.jar"), artifacts));
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File("plugins/transforms/script/lib/xz-25.0.4.jar"), artifacts));
    // The connectors stay even when a name would match
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File("plugins/engines/spark/lib/delta/slf4j-api-2.0.20.jar"), artifacts));
    assertFalse(
        NativeSparkFatJar.isSparkDistributionJar(
            new File(SPARK_LIB, "rocksdbjni-9.8.4.jar"), java.util.Set.of()));
  }

  @Test
  void theProvidedJarLeavesOutWhatSparkShipsAndAllButOneSwtJar() throws Exception {
    File install = new File(tempDir, "install");
    String[] kept = {
      "lib/core/hop-core-2.21.0.jar",
      "lib/core/commons-csv-1.14.0.jar",
      // Named like what Spark ships, but not in the engine's Spark runtime: GraalVM's shadowed
      // copies relocate their classes and Spark shades protobuf, so the cluster has none of these
      "lib/core/slf4j-api-2.0.17.jar",
      "lib/core/protobuf-java-4.34.1.jar",
      "plugins/transforms/script/lib/xz-25.0.4.jar",
      "plugins/transforms/script/lib/icu4j-25.0.4.jar",
      "plugins/engines/spark/hop-engines-spark-2.21.0.jar",
      "plugins/engines/spark/lib/delta/delta-storage-4.3.1.jar",
      // SWT classes for the plugin classes that reference them: one platform is enough
      "lib/swt/linux/x86_64/swt.jar",
      "lib/core/org.eclipse.jface-3.40.0.jar"
    };
    String[] dropped = {
      "lib/swt/osx/arm64/swt.jar",
      "lib/swt/win64/swt.jar",
      "plugins/engines/spark/lib/rocksdbjni-9.8.4.jar",
      "plugins/engines/spark/lib/spark-sql_2.13-4.1.3.jar"
    };
    for (String path : concat(kept, dropped)) {
      File jar = new File(install, path);
      assertTrue(jar.getParentFile().mkdirs() || jar.getParentFile().isDirectory());
      assertTrue(jar.createNewFile());
    }
    File tree = new File(install, NativeSparkFatJar.SPARK_DEPENDENCIES_FILE);
    java.nio.file.Files.write(tree.toPath(), TREE);

    java.util.List<String> jars = NativeSparkFatJar.findJarFilenames(install, true);
    for (String path : kept) {
      assertTrue(jars.stream().anyMatch(j -> j.endsWith(path)), path + " in " + jars);
    }
    for (String path : dropped) {
      assertFalse(jars.stream().anyMatch(j -> j.endsWith(path)), path + " in " + jars);
    }
  }

  private static String[] concat(String[] a, String[] b) {
    String[] all = java.util.Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, all, a.length, b.length);
    return all;
  }
}
