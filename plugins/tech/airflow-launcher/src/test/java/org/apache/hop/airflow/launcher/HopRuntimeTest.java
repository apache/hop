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
package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopRuntimeTest {

  private static final List<String> FOLDERS =
      List.of(
          "HOP_CONFIG_FOLDER",
          "HOP_AUDIT_FOLDER",
          "HOP_SHARED_JDBC_FOLDERS",
          "HOP_PLUGIN_BASE_FOLDERS");

  @TempDir Path hopHome;

  /**
   * The hop script starts Java in the Hop installation, so relative folders are relative to it. The
   * task JVM runs where Airflow starts it.
   */
  @Test
  void resolvesRelativeFoldersAgainstTheHopInstallation() throws Exception {
    Files.createDirectories(hopHome.resolve("lib/core"));
    Files.writeString(hopHome.resolve("lib/core/hop-core.jar"), "");
    Map<String, String> environment = new HashMap<>();
    environment.put("HOP_CONFIG_FOLDER", "config/custom");
    environment.put("HOP_AUDIT_FOLDER", "./audit");
    environment.put("HOP_SHARED_JDBC_FOLDERS", "lib/jdbc, /opt/drivers,extra/../jdbc2");
    environment.put("HOP_PLUGIN_BASE_FOLDERS", "plugins,s3://bucket/plugins");
    Map<String, String> previous = new HashMap<>();
    for (String name : FOLDERS) {
      previous.put(name, System.getProperty(name));
      System.clearProperty(name);
    }
    try {
      new HopRuntime(hopHome).applySystemProperties(environment);

      assertEquals(
          hopHome.resolve("config/custom").toString(), System.getProperty("HOP_CONFIG_FOLDER"));
      assertEquals(hopHome.resolve("audit").toString(), System.getProperty("HOP_AUDIT_FOLDER"));
      assertEquals(
          hopHome.resolve("lib/jdbc") + ",/opt/drivers," + hopHome.resolve("jdbc2"),
          System.getProperty("HOP_SHARED_JDBC_FOLDERS"));
      assertEquals(
          hopHome.resolve("plugins") + ",s3://bucket/plugins",
          System.getProperty("HOP_PLUGIN_BASE_FOLDERS"));
    } finally {
      for (String name : FOLDERS) {
        if (previous.get(name) == null) {
          System.clearProperty(name);
        } else {
          System.setProperty(name, previous.get(name));
        }
      }
    }
  }

  @Test
  void keepsAbsoluteFoldersAndUrls() {
    assertEquals(
        "/opt/config,file:///opt/hop/config,s3://bucket",
        HopRuntime.resolveFolders(
            Path.of("/opt/hop"), "/opt/config, file:///opt/hop/config,s3://bucket"));
  }
}
