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
package org.apache.hop.core.plugins.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.json.HopJson;

/**
 * Machine-readable record of everything one plugin (or engine module) zip installs into a Hop home:
 * its own files, the private libraries under its {@code lib/} folder, the shared jars it
 * contributes to {@code lib/core} and {@code lib/jdbc}, the plugin ids it registers and the other
 * plugins it needs at runtime.
 *
 * <p>The build generates it by inspecting the finished plugin zip ({@link
 * PluginManifestGenerator}), so it can never disagree with what actually ships. A plugin manifest
 * lives next to {@code version.xml} as {@value #PLUGIN_FILE_NAME}; an engine module's manifest
 * lives in {@code lib/core} as {@code <artifactId>-manifest.json}.
 *
 * <p>Consumers use it to uninstall a plugin completely (a shared jar goes when its last owner
 * goes), to build minimal distributions for a project, and to check the distribution for
 * conflicting shared jars.
 */
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "schema",
  "kind",
  "groupId",
  "artifactId",
  "version",
  "ga",
  "folder",
  "manifestPath",
  "pluginIds",
  "requiresArtifacts",
  "requiresFolders",
  "notShipped",
  "files"
})
public class PluginManifest {

  public static final String SCHEMA = "hop-plugin-manifest/1";
  public static final String PLUGIN_FILE_NAME = "plugin-manifest.json";
  public static final String ENGINE_FILE_SUFFIX = "-manifest.json";

  /** A loadable plugin folder, or an engine/library module that only fills {@code lib/core}. */
  public enum Kind {
    PLUGIN,
    ENGINE
  }

  private String schema = SCHEMA;
  private Kind kind;

  private String groupId;
  private String artifactId;
  private String version;

  /** Plugin folder relative to the Hop home, e.g. {@code plugins/transforms/json}; engine: null. */
  private String folder;

  /**
   * Plugin ids registered by this module, keyed by the simple name of the registering annotation
   * ({@code Transform}, {@code Action}, {@code DatabaseMetaPlugin}, {@code HopMetadata}, ...).
   */
  private Map<String, List<String>> pluginIds = new TreeMap<>();

  /** Other Hop modules this one compiles against, as {@code groupId:artifactId}. */
  private List<String> requiresArtifacts = new ArrayList<>();

  /** Plugin folders listed in this plugin's {@code dependencies.xml}, relative to the Hop home. */
  private List<String> requiresFolders = new ArrayList<>();

  /**
   * Direct dependencies the plugin declares but the zip does not contain: the user (or another
   * module) has to supply them, e.g. a proprietary JDBC driver.
   */
  private List<ManifestFile> notShipped = new ArrayList<>();

  private List<ManifestFile> files = new ArrayList<>();

  public String getGa() {
    return groupId + ":" + artifactId;
  }

  public void setGa(String ignored) {
    // Derived from groupId/artifactId; present in JSON for readability only.
  }

  /** The path of the manifest file itself inside a zip or Hop home. */
  public String getManifestPath() {
    if (kind == Kind.ENGINE || folder == null) {
      return "lib/core/" + artifactId + ENGINE_FILE_SUFFIX;
    }
    return folder + "/" + PLUGIN_FILE_NAME;
  }

  public void setManifestPath(String ignored) {
    // Derived; see getManifestPath()
  }

  public List<ManifestFile> getFiles(ManifestFile.Location location) {
    List<ManifestFile> result = new ArrayList<>();
    for (ManifestFile file : files) {
      if (file.getLocation() == location) {
        result.add(file);
      }
    }
    return result;
  }

  public static PluginManifest read(InputStream in) throws IOException {
    return HopJson.newMapper().readValue(in, PluginManifest.class);
  }

  public static PluginManifest read(Path file) throws IOException {
    try (InputStream in = Files.newInputStream(file)) {
      return read(in);
    }
  }

  public void write(OutputStream out) throws IOException {
    writer().writeValue(out, this);
  }

  public byte[] toJson() throws IOException {
    return writer().writeValueAsBytes(this);
  }

  /** Pretty printed with \n line ends: the manifest is part of a reproducible zip, on any OS. */
  private static ObjectWriter writer() {
    return HopJson.newMapper()
        .writer(new DefaultPrettyPrinter().withObjectIndenter(new DefaultIndenter("  ", "\n")));
  }
}
