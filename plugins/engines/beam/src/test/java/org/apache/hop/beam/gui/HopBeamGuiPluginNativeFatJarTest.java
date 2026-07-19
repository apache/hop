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

package org.apache.hop.beam.gui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopBeamGuiPluginNativeFatJarTest {

  @TempDir File tempDir;

  @Test
  void nativeTokenDetection() {
    assertTrue(HopBeamGuiPlugin.isNativeSparkClientVersion("native"));
    assertTrue(HopBeamGuiPlugin.isNativeSparkClientVersion("NATIVE"));
    assertTrue(HopBeamGuiPlugin.isNativeSparkClientVersion(" native "));
    assertTrue(HopBeamGuiPlugin.isNativeSparkClientVersion("native-provided"));
    assertTrue(HopBeamGuiPlugin.isNativeProvidedSparkClientVersion("native-provided"));
    assertFalse(HopBeamGuiPlugin.isNativeProvidedSparkClientVersion("native"));
    assertFalse(HopBeamGuiPlugin.isNativeSparkClientVersion(null));
    assertFalse(HopBeamGuiPlugin.isNativeSparkClientVersion(""));
    assertFalse(HopBeamGuiPlugin.isNativeSparkClientVersion("3.5.8"));
  }

  @Test
  void providedModeExcludesSparkAndScalaRuntime() {
    assertTrue(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("spark-core_2.13-4.1.2.jar")));
    assertTrue(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("spark-sql_2.13-4.1.2.jar")));
    assertTrue(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("scala-library-2.13.17.jar")));
    assertTrue(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(new File("chill_2.13-0.10.0.jar")));
    assertTrue(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("hadoop-client-api-3.4.2.jar")));
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("hop-engines-spark-2.19.0-SNAPSHOT.jar")));
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("beam-runners-direct-java-2.74.0.jar")));
    // Lakehouse connectors ship in engines-spark lib and must ride the native-provided fat jar
    // so cluster submit does not require a separate --packages step for Delta/Iceberg.
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("delta-spark_4.1_2.13-4.3.1.jar")));
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("delta-storage-4.3.1.jar")));
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("iceberg-spark-runtime-4.1_2.13-1.11.0.jar")));
    assertFalse(
        HopBeamGuiPlugin.isExcludedForNativeProvidedSparkFatJar(
            new File("unitycatalog-client-0.5.0.jar")));
  }

  @Test
  void excludesBeamSpark3AndScala212Basenames() {
    assertTrue(excluded("spark-core_2.12-3.5.8.jar"));
    assertTrue(excluded("spark-sql-api_2.12-3.5.0.jar"));
    assertTrue(excluded("scala-library-2.12.21.jar"));
    assertTrue(excluded("chill_2.12-0.10.0.jar"));
    assertTrue(excluded("json4s-core_2.12-3.7.0-M11.jar"));
    assertTrue(excluded("jackson-module-scala_2.12-2.21.0.jar"));
    assertTrue(excluded("beam-runners-spark-3-2.74.0.jar"));
  }

  @Test
  void keepsNativeSpark4AndUnrelatedJars() {
    assertFalse(excluded("spark-core_2.13-4.1.2.jar"));
    assertFalse(excluded("spark-sql_2.13-4.1.2.jar"));
    assertFalse(excluded("scala-library-2.13.17.jar"));
    assertFalse(excluded("hop-engines-spark-2.19.0-SNAPSHOT.jar"));
    assertFalse(excluded("beam-runners-direct-java-2.74.0.jar"));
    assertFalse(excluded("beam-runners-flink-1.19-2.74.0.jar"));
    assertFalse(excluded("hadoop-client-api-3.4.2.jar"));
  }

  @Test
  void excludesJarsUnderSparkClientPackPaths() throws Exception {
    File packJar = new File(tempDir, "lib/spark-client/spark-core_2.12-3.5.8.jar");
    assertTrue(packJar.getParentFile().mkdirs());
    assertTrue(packJar.createNewFile());
    assertTrue(HopBeamGuiPlugin.isExcludedForNativeSparkFatJar(packJar));

    File versioned = new File(tempDir, "lib/spark-clients/3.4.4/spark-core_2.12-3.4.4.jar");
    assertTrue(versioned.getParentFile().mkdirs());
    assertTrue(versioned.createNewFile());
    assertTrue(HopBeamGuiPlugin.isExcludedForNativeSparkFatJar(versioned));
  }

  @Test
  void keepsSpark4UnderNativePluginLibPath() throws Exception {
    File pluginJar = new File(tempDir, "plugins/engines/spark/lib/spark-sql_2.13-4.1.2.jar");
    assertTrue(pluginJar.getParentFile().mkdirs());
    assertTrue(pluginJar.createNewFile());
    assertFalse(HopBeamGuiPlugin.isExcludedForNativeSparkFatJar(pluginJar));
  }

  private static boolean excluded(String basename) {
    return HopBeamGuiPlugin.isExcludedForNativeSparkFatJar(new File(basename));
  }
}
