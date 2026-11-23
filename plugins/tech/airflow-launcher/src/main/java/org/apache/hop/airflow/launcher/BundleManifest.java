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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.airflow.sdk.Server;

/**
 * The Hop attributes of a DAG bundle's stub jar.
 *
 * @param folder the project folder: the folder of the stub
 * @param schemaVersion the supervisor schema version the stub announces to Airflow
 * @param hopVersion the Hop version that wrote the stub, or empty
 * @param projectName the name to register the project under
 * @param runConfiguration the workflow run configuration
 * @param logLevel the log level
 * @param workflowPatterns glob patterns of the workflows that become DAGs; empty for all
 * @param environmentFiles environment configuration files, relative to the project
 * @param dagIdPrefix the start of every DAG id
 */
public record BundleManifest(
    Path folder,
    String schemaVersion,
    String hopVersion,
    String projectName,
    String runConfiguration,
    String logLevel,
    List<String> workflowPatterns,
    List<String> environmentFiles,
    String dagIdPrefix) {

  /** Points to the stub jar or the project folder, instead of looking on the class path. */
  public static final String PROPERTY_BUNDLE = "hop.airflow.bundle";

  /**
   * Finds the stub: the Airflow coordinator puts it first on the class path. The launcher itself is
   * on the class path through the stub's Class-Path, so it cannot look next to its own jar.
   */
  public static BundleManifest locate() throws IOException {
    String configured = System.getProperty(PROPERTY_BUNDLE);
    if (configured != null && !configured.isBlank()) {
      Path path = Path.of(configured);
      return read(Files.isDirectory(path) ? path.resolve(HopAirflowBundle.STUB_FILE_NAME) : path);
    }
    String classPath = System.getProperty("java.class.path", "");
    for (String entry : classPath.split(File.pathSeparator)) {
      if (!entry.endsWith(".jar")) {
        continue;
      }
      Path jar = Path.of(entry);
      if (Files.isRegularFile(jar) && hasProjectName(jar)) {
        return read(jar);
      }
    }
    throw new IOException(
        "No "
            + HopAirflowBundle.STUB_FILE_NAME
            + " with a "
            + HopAirflowBundle.ATTRIBUTE_PROJECT_NAME
            + " attribute on the class path: "
            + classPath);
  }

  private static boolean hasProjectName(Path jar) throws IOException {
    try (JarFile jarFile = new JarFile(jar.toFile())) {
      Manifest manifest = jarFile.getManifest();
      return manifest != null
          && manifest.getMainAttributes().getValue(HopAirflowBundle.ATTRIBUTE_PROJECT_NAME) != null;
    }
  }

  /** Reads the Hop attributes of a stub jar. */
  public static BundleManifest read(Path stubJar) throws IOException {
    Manifest manifest;
    try (JarFile jarFile = new JarFile(stubJar.toFile())) {
      manifest = jarFile.getManifest();
    }
    if (manifest == null) {
      throw new IOException(stubJar + " has no manifest");
    }
    return of(stubJar.toAbsolutePath().getParent(), manifest.getMainAttributes());
  }

  static BundleManifest of(Path folder, Attributes attributes) throws IOException {
    String projectName = value(attributes, HopAirflowBundle.ATTRIBUTE_PROJECT_NAME, "");
    if (projectName.isEmpty()) {
      throw new IOException(
          "The stub in " + folder + " has no " + HopAirflowBundle.ATTRIBUTE_PROJECT_NAME);
    }
    return new BundleManifest(
        folder,
        value(attributes, HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION, ""),
        value(attributes, HopAirflowBundle.ATTRIBUTE_HOP_VERSION, ""),
        projectName,
        value(
            attributes,
            HopAirflowBundle.ATTRIBUTE_RUN_CONFIGURATION,
            HopAirflowBundle.DEFAULT_RUN_CONFIGURATION),
        value(attributes, HopAirflowBundle.ATTRIBUTE_LOG_LEVEL, HopAirflowBundle.DEFAULT_LOG_LEVEL),
        split(value(attributes, HopAirflowBundle.ATTRIBUTE_WORKFLOWS, ""), ","),
        split(value(attributes, HopAirflowBundle.ATTRIBUTE_ENVIRONMENT_FILES, ""), ";"),
        value(
            attributes, HopAirflowBundle.ATTRIBUTE_DAG_ID_PREFIX, defaultDagIdPrefix(projectName)));
  }

  /** The project name, with the characters Airflow does not allow in a DAG id replaced. */
  public static String defaultDagIdPrefix(String projectName) {
    return projectName.replaceAll("[^A-Za-z0-9_.-]", "_") + "__";
  }

  /**
   * The stub must announce the schema of the SDK the launcher uses. A stub written by another Hop
   * version can announce another one, and then the two sides would not understand each other.
   */
  public void checkSchemaVersion(String sdkSchemaVersion) {
    if (!sdkSchemaVersion.equals(schemaVersion)) {
      throw new IllegalStateException(
          "The "
              + HopAirflowBundle.STUB_FILE_NAME
              + " in "
              + folder
              + " announces Airflow supervisor schema '"
              + schemaVersion
              + "' (written by Hop "
              + (hopVersion.isEmpty() ? "?" : hopVersion)
              + "), but the Hop installation on this worker uses '"
              + sdkSchemaVersion
              + "'. Write the stub again with 'hop airflow bundle' from the Hop version on the worker.");
    }
  }

  /** The supervisor schema version of the Airflow SDK on the class path, from its manifest. */
  public static String sdkSchemaVersion() throws IOException {
    Path sdkJar;
    try {
      sdkJar = Path.of(Server.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (Exception e) {
      throw new IOException("Unable to find the Airflow SDK jar", e);
    }
    return schemaVersionOf(sdkJar);
  }

  /** The supervisor schema version in the manifest of an Airflow SDK jar. */
  public static String schemaVersionOf(Path sdkJar) throws IOException {
    try (JarFile jarFile = new JarFile(sdkJar.toFile())) {
      Manifest manifest = jarFile.getManifest();
      String version =
          manifest == null
              ? null
              : manifest.getMainAttributes().getValue(HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      if (version == null || version.isBlank()) {
        throw new IOException(sdkJar + " has no " + HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      }
      return version.trim();
    }
  }

  private static String value(Attributes attributes, String name, String defaultValue) {
    String value = attributes.getValue(name);
    return value == null || value.isBlank() ? defaultValue : value.trim();
  }

  private static List<String> split(String value, String separator) {
    List<String> parts = new ArrayList<>();
    for (String part : value.split(separator)) {
      if (!part.isBlank()) {
        parts.add(part.trim());
      }
    }
    return List.copyOf(parts);
  }
}
