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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkerProjectsTest {

  @TempDir Path hopHome;

  private void writeConfig(String projects) throws Exception {
    Files.createDirectories(hopHome.resolve("config"));
    Files.writeString(
        hopHome.resolve("config/hop-config.json"),
        "{\"projectsConfig\":{\"projectConfigurations\":[" + projects + "]}}");
  }

  private static String project(String name, String home, String configFilename) {
    return "{\"projectName\":\""
        + name
        + "\",\"projectHome\":\""
        + home
        + "\",\"configFilename\":\""
        + configFilename
        + "\"}";
  }

  /** The Hop client ships hop-config.json with project homes relative to the installation. */
  @Test
  void makesRelativeHomesAbsoluteToTheInstallation() throws Exception {
    writeConfig(
        String.join(
            ",",
            project("default", "config/projects/default", "project-config.json"),
            project("samples", "./config/projects/samples", "hop.config")));

    assertEquals(
        List.of(
            "default=" + hopHome.resolve("config/projects/default"),
            "samples=" + hopHome.resolve("config/projects/samples") + ":hop.config"),
        WorkerProjects.relativeProjectLocations(hopHome, hopHome.resolve("config"), "sales"));
  }

  @Test
  void leavesOtherHomesAndTheBundleProjectToHop() throws Exception {
    writeConfig(
        String.join(
            ",",
            project("absolute", "/data/projects/absolute", "project-config.json"),
            project("variable", "${HOP_CONFIG_FOLDER}/projects/variable", "project-config.json"),
            project("archive", "zip:file:///data/p.zip!/", "project-config.json"),
            project("sales", "projects/sales", "project-config.json")));

    assertTrue(
        WorkerProjects.relativeProjectLocations(hopHome, hopHome.resolve("config"), "sales")
            .isEmpty());
  }

  @Test
  void needsNoConfiguration() throws Exception {
    assertTrue(
        WorkerProjects.relativeProjectLocations(hopHome, hopHome.resolve("config"), "sales")
            .isEmpty());
  }

  @Test
  void knowsWhatAPlainRelativePathIs() {
    assertTrue(WorkerProjects.isPlainRelativePath("config/projects/default"));
    assertFalse(WorkerProjects.isPlainRelativePath("/opt/hop/config"));
    assertFalse(WorkerProjects.isPlainRelativePath("${PROJECT_HOME}/x"));
    assertFalse(WorkerProjects.isPlainRelativePath("C:/projects/x"));
    assertFalse(WorkerProjects.isPlainRelativePath(""));
  }
}
