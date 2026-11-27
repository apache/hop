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

package org.apache.hop.spark.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkHadoopConfigurationTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  private static String site(String... keyValues) {
    StringBuilder xml = new StringBuilder("<?xml version=\"1.0\"?>\n<configuration>\n");
    for (int i = 0; i < keyValues.length; i += 2) {
      xml.append("  <property><name>")
          .append(keyValues[i])
          .append("</name><value>")
          .append(keyValues[i + 1])
          .append("</value></property>\n");
    }
    return xml.append("</configuration>\n").toString();
  }

  @Test
  void configuredFolderIsUsedForEveryMaster() {
    List<String> folders =
        SparkHadoopConfiguration.resolveFolders("/etc/hadoop/conf", false, name -> "/other");
    assertEquals(List.of("/etc/hadoop/conf"), folders);
  }

  @Test
  void environmentIsOnlyUsedForYarn() {
    Map<String, String> env =
        Map.of("HADOOP_CONF_DIR", "/etc/hadoop/conf", "YARN_CONF_DIR", "/etc/hadoop/conf");
    assertTrue(SparkHadoopConfiguration.resolveFolders("", false, env::get).isEmpty());
    assertEquals(
        List.of("/etc/hadoop/conf"), SparkHadoopConfiguration.resolveFolders("", true, env::get));
  }

  @Test
  void yarnMasters() {
    assertTrue(SparkHadoopConfiguration.isYarnMaster("yarn"));
    assertTrue(SparkHadoopConfiguration.isYarnMaster(" YARN "));
    assertFalse(SparkHadoopConfiguration.isYarnMaster("spark://host:7077"));
    assertFalse(SparkHadoopConfiguration.isYarnMaster(null));
  }

  @Test
  void siteFilesBecomeSparkHadoopProperties() throws Exception {
    Files.writeString(
        tempDir.resolve("core-site.xml"),
        site("fs.defaultFS", "hdfs://namenode:8020", "hop.test.host", "namenode"));
    Files.writeString(
        tempDir.resolve("yarn-site.xml"),
        site(
            "yarn.resourcemanager.hostname",
            "${hop.test.host}",
            "yarn.resourcemanager.address",
            "namenode:8050"));

    Map<String, String> properties =
        SparkHadoopConfiguration.load(
            List.of(tempDir.toString()), new LogChannel("SparkHadoopConfigurationTest"));

    assertEquals("hdfs://namenode:8020", properties.get("spark.hadoop.fs.defaultFS"));
    assertEquals("namenode:8050", properties.get("spark.hadoop.yarn.resourcemanager.address"));
    // Hadoop variable expansion is applied
    assertEquals("namenode", properties.get("spark.hadoop.yarn.resourcemanager.hostname"));
  }

  @Test
  void missingFolderOrFilesGiveNoProperties() throws Exception {
    LogChannel log = new LogChannel("SparkHadoopConfigurationTest");
    assertTrue(
        SparkHadoopConfiguration.load(List.of(tempDir.resolve("nope").toString()), log).isEmpty());
    assertTrue(SparkHadoopConfiguration.load(List.of(tempDir.toString()), log).isEmpty());
  }

  @Test
  void noYarnJarsOutsideThePluginFolder() {
    // Test classes are not loaded from plugins/engines/spark, so there is no lib folder next to
    // them
    assertNull(SparkHadoopConfiguration.defaultYarnJars(SparkHadoopConfigurationTest.class));
  }

  @Test
  void theYarnClassPathTakesSharedLibrariesButNotHopBeamSparkOrHadoop() {
    org.junit.jupiter.api.Assertions.assertTrue(
        SparkHadoopConfiguration.isSharedRuntimeJar(
            new java.io.File("netty-transport-4.2.19.Final.jar")));
    org.junit.jupiter.api.Assertions.assertTrue(
        SparkHadoopConfiguration.isSharedRuntimeJar(
            new java.io.File("commons-collections4-4.5.0.jar")));
    org.junit.jupiter.api.Assertions.assertTrue(
        SparkHadoopConfiguration.isSharedRuntimeJar(new java.io.File("log4j-core-2.25.2.jar")));
    for (String name :
        new String[] {
          "hop-core-2.21.0.jar",
          "spark-network-common_2.12-3.5.8.jar",
          "scala-library-2.12.21.jar",
          "spark-core_2.13-4.1.3.jar",
          "hadoop-common-3.4.2.jar",
          "hadoop-client-api-3.4.2.jar"
        }) {
      org.junit.jupiter.api.Assertions.assertFalse(
          SparkHadoopConfiguration.isSharedRuntimeJar(new java.io.File(name)), name);
    }
    // Beam's jars by the plugin manifests: what only the Beam engine installs
    java.io.File beamSdk = new java.io.File("beam-sdks-java-core-2.74.0.jar");
    org.junit.jupiter.api.Assertions.assertFalse(
        SparkHadoopConfiguration.isSharedRuntimeJar(beamSdk, beamSdk::equals));
    org.junit.jupiter.api.Assertions.assertTrue(
        SparkHadoopConfiguration.isSharedRuntimeJar(beamSdk, file -> false));
  }

  @Test
  void theYarnRuntimeArchiveHoldsTheJarsAtItsRoot(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path archiveDir) throws Exception {
    java.nio.file.Path a = archiveDir.resolve("spark-core_2.13-4.1.3.jar");
    java.nio.file.Path b = archiveDir.resolve("scala-library-2.13.17.jar");
    java.nio.file.Files.write(a, new byte[] {1, 2, 3});
    java.nio.file.Files.write(b, new byte[] {4, 5});
    java.nio.file.Path archive = archiveDir.resolve("runtime.zip");

    SparkHadoopConfiguration.writeArchive(archive, java.util.List.of(a.toString(), b.toString()));

    try (java.util.zip.ZipFile zip = new java.util.zip.ZipFile(archive.toFile())) {
      org.junit.jupiter.api.Assertions.assertEquals(2, zip.size());
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          new byte[] {1, 2, 3},
          zip.getInputStream(zip.getEntry("spark-core_2.13-4.1.3.jar")).readAllBytes());
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          new byte[] {4, 5},
          zip.getInputStream(zip.getEntry("scala-library-2.13.17.jar")).readAllBytes());
    }
  }
}
