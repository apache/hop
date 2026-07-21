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

package org.apache.hop.projects.util;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;

/**
 * Writes a single-file JSON export of hop metadata (connections, run configurations, etc.) for a
 * project. Used by {@code hop-conf --export-metadata}, optional project auto-export, and similar
 * packaging flows.
 */
public final class ProjectsMetadataExporter {

  private ProjectsMetadataExporter() {
    // utility
  }

  /**
   * If the currently active project has auto-export enabled, write the metadata export file. Fail
   * soft: log errors and never throw to the caller (so metadata saves are not blocked).
   *
   * @param log log channel
   * @param variables variable space (for resolving paths)
   */
  public static void autoExportActiveProjectIfEnabled(ILogChannel log, IVariables variables) {
    try {
      String projectName = resolveActiveProjectName(variables);
      if (StringUtils.isEmpty(projectName)) {
        return;
      }

      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      ProjectConfig projectConfig = config.findProjectConfig(projectName);
      if (projectConfig == null) {
        return;
      }
      if (projectConfig.isReadOnly()) {
        if (log != null && log.isDetailed()) {
          log.logDetailed(
              "Skipping metadata auto-export for read-only project '" + projectName + "'");
        }
        return;
      }

      Project project = projectConfig.loadProject(variables);
      if (project == null || !project.isAutoExportMetadata()) {
        return;
      }

      IHopMetadataProvider metadataProvider = HopMetadataInstance.getMetadataProvider();
      if (metadataProvider == null && project.getMetadataProvider() != null) {
        metadataProvider = project.getMetadataProvider();
      }
      if (metadataProvider == null) {
        if (log != null) {
          log.logError(
              "Cannot auto-export metadata for project '"
                  + projectName
                  + "': no metadata provider is available");
        }
        return;
      }

      String relativeOrAbsolute =
          Const.NVL(
              project.getAutoExportMetadataFilename(),
              Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME);
      if (Utils.isEmpty(relativeOrAbsolute)) {
        relativeOrAbsolute = Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME;
      }

      String projectHome = variables.resolve(projectConfig.getProjectHome());
      String targetFilename = resolveExportFilename(projectHome, relativeOrAbsolute, variables);

      exportToFile(metadataProvider, targetFilename);

      if (log != null && log.isBasic()) {
        log.logBasic(
            "Auto-exported project metadata for '" + projectName + "' to: " + targetFilename);
      }
    } catch (Exception e) {
      if (log != null) {
        log.logError(
            "Error auto-exporting project metadata (continuing without failing the save)", e);
      }
    }
  }

  /**
   * Export all metadata from the provider to a JSON file.
   *
   * @param metadataProvider source metadata
   * @param filename resolved absolute or VFS path of the target file
   * @throws HopException on serialization or write errors
   */
  public static void exportToFile(IHopMetadataProvider metadataProvider, String filename)
      throws HopException {
    if (metadataProvider == null) {
      throw new HopException("Metadata provider is required for export");
    }
    if (Utils.isEmpty(filename)) {
      throw new HopException("Export filename is required");
    }

    SerializableMetadataProvider serializable = new SerializableMetadataProvider(metadataProvider);
    String jsonString = serializable.toJson();

    try {
      FileObject file = HopVfs.getFileObject(filename);
      FileObject parent = file.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      try (OutputStream outputStream = HopVfs.getOutputStream(file, false)) {
        outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      throw new HopException("There was an error exporting metadata to file: " + filename, e);
    }
  }

  /**
   * Resolve the export target path. Absolute paths and VFS URIs are used as-is (after variable
   * resolution); relative paths are rooted at the project home.
   */
  public static String resolveExportFilename(
      String projectHome, String filename, IVariables variables) throws HopException {
    String resolved = variables != null ? variables.resolve(filename) : filename;
    if (Utils.isEmpty(resolved)) {
      resolved = Defaults.DEFAULT_AUTO_EXPORT_METADATA_FILENAME;
    }

    // Absolute filesystem path or VFS scheme (e.g. file://, s3://)
    if (resolved.startsWith("/")
        || resolved.startsWith("\\")
        || (resolved.length() > 2 && resolved.charAt(1) == ':')
        || resolved.contains("://")) {
      return resolved;
    }

    String home = variables != null ? variables.resolve(projectHome) : projectHome;
    if (Utils.isEmpty(home)) {
      throw new HopException("Project home is required to resolve relative export filename");
    }

    try {
      FileObject homeObject = HopVfs.getFileObject(home);
      FileObject target = homeObject.resolveFile(resolved);
      // Prefer a clean path string
      FileName name = target.getName();
      return name.getURI();
    } catch (Exception e) {
      // Fallback to simple join
      String separator = home.endsWith("/") || home.endsWith("\\") ? "" : "/";
      return home + separator + resolved;
    }
  }

  private static String resolveActiveProjectName(IVariables variables) {
    String projectName = HopNamespace.getNamespace();
    if (StringUtils.isNotEmpty(projectName)) {
      return projectName;
    }
    if (variables != null) {
      projectName = variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME);
      if (StringUtils.isNotEmpty(projectName)) {
        return projectName;
      }
    }
    return System.getProperty(Defaults.VARIABLE_HOP_PROJECT_NAME);
  }
}
