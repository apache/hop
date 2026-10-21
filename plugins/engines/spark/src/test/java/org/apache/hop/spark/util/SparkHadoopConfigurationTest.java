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
}
