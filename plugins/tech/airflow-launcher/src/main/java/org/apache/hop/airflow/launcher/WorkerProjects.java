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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The projects of the worker's Hop configuration with a relative home folder. The hop script runs
 * Hop from its installation folder, so such a home is relative to that folder. Airflow starts the
 * launcher elsewhere, so these projects are registered again with the folder made absolute; else a
 * project that has one of them as parent project, such as 'default', would not find it.
 */
final class WorkerProjects {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String DEFAULT_CONFIG_FILENAME = "project-config.json";

  private WorkerProjects() {}

  /**
   * @param hopHome the Hop installation
   * @param configFolder the Hop configuration folder of the worker
   * @param bundleProject the project of the bundle, which is registered separately
   * @return {@code name=folder} entries for {@code --project-locations}
   */
  static List<String> relativeProjectLocations(
      Path hopHome, Path configFolder, String bundleProject) throws IOException {
    List<String> locations = new ArrayList<>();
    Path configFile = configFolder.resolve("hop-config.json");
    if (!Files.isRegularFile(configFile)) {
      return locations;
    }
    JsonNode projects =
        JSON.readTree(configFile.toFile()).path("projectsConfig").path("projectConfigurations");
    for (JsonNode project : projects) {
      String name = project.path("projectName").asText("");
      String home = project.path("projectHome").asText("");
      String configFilename = project.path("configFilename").asText(DEFAULT_CONFIG_FILENAME);
      if (name.isBlank() || name.equals(bundleProject) || !isPlainRelativePath(home)) {
        continue;
      }
      String folder = hopHome.resolve(home).normalize().toString();
      // --project-locations splits on commas, and name=folder[:file] needs these as they are
      if (containsAny(name, ",=")
          || containsAny(folder, ",")
          || containsAny(configFilename, ",:")) {
        continue;
      }
      String entry = name + "=" + folder;
      if (!configFilename.isBlank() && !DEFAULT_CONFIG_FILENAME.equals(configFilename)) {
        entry += ":" + configFilename;
      }
      locations.add(entry);
    }
    return locations;
  }

  /** Not absolute, and no variable, URI or drive letter that Hop resolves itself. */
  static boolean isPlainRelativePath(String path) {
    if (path.isBlank() || path.contains("$") || path.contains(":") || path.startsWith("~")) {
      return false;
    }
    try {
      return !Path.of(path).isAbsolute();
    } catch (InvalidPathException e) {
      return false;
    }
  }

  private static boolean containsAny(String value, String characters) {
    for (char c : characters.toCharArray()) {
      if (value.indexOf(c) >= 0) {
        return true;
      }
    }
    return false;
  }
}
