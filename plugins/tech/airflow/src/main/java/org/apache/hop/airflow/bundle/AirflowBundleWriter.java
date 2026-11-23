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

package org.apache.hop.airflow.bundle;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Writes a Hop DAG bundle: the stub jar Airflow needs to list and run the workflows of a project.
 *
 * <p>Without a target folder, the stub goes into the project folder, to commit to the Git
 * repository Airflow reads as a Git DAG bundle. With a target folder, the project is copied there
 * first, for Airflow to read as a folder bundle.
 */
@Getter
@Setter
public class AirflowBundleWriter {

  /**
   * Marks a folder that holds a copy of a project: only such a folder is emptied before a publish.
   * Hidden, so it is never copied from a project.
   */
  public static final String BUNDLE_MARKER = ".hop-airflow-bundle";

  /** Where the Hop container image installs Hop, and so where the workers usually have it. */
  public static final String DEFAULT_WORKER_HOP_HOME = "/opt/hop";

  private String projectName;
  private String projectHome;
  private String targetFolder;
  private String workerHopHome = DEFAULT_WORKER_HOP_HOME;
  private String runConfiguration = HopAirflowBundle.DEFAULT_RUN_CONFIGURATION;
  private String logLevel = HopAirflowBundle.DEFAULT_LOG_LEVEL;
  private List<String> workflowPatterns = new ArrayList<>();
  private List<String> environmentFiles = new ArrayList<>();
  private String dagIdPrefix;
  private String hopVersion;

  /** The supervisor schema version for the stub; read from the installed launcher when null. */
  private String schemaVersion;

  /**
   * What was written.
   *
   * @param bundleFolder the folder Airflow should read
   * @param stubFile the stub jar
   * @param copied true when the project was copied to the bundle folder
   */
  public record Result(String bundleFolder, String stubFile, boolean copied) {}

  public Result write() throws HopException {
    if (StringUtils.isBlank(projectName) || StringUtils.containsAny(projectName, ",=")) {
      throw new HopException(
          "A DAG bundle needs a project name without a comma or an equals sign, not '"
              + projectName
              + "'");
    }
    try {
      FileObject project = HopVfs.getFileObject(projectHome);
      if (!project.exists() || !project.isFolder()) {
        throw new HopException("The project folder " + projectHome + " does not exist");
      }
      checkEnvironmentFiles(project);
      String schema = schemaVersion != null ? schemaVersion : installedSchemaVersion();

      FileObject bundle = project;
      boolean copied = false;
      if (StringUtils.isNotBlank(targetFolder)) {
        FileObject target = HopVfs.getFileObject(targetFolder);
        if (!sameFolder(target, project)) {
          copyProject(project, target);
          bundle = target;
          copied = true;
        }
      }

      FileObject stub = bundle.resolveFile(HopAirflowBundle.STUB_FILE_NAME);
      try (OutputStream out = HopVfs.getOutputStream(stub, false);
          JarOutputStream jar = new JarOutputStream(out, manifest(schema))) {
        jar.flush();
      }
      return new Result(HopVfs.getFilename(bundle), HopVfs.getFilename(stub), copied);
    } catch (IOException e) {
      throw new HopException("Unable to write the Airflow DAG bundle of " + projectName, e);
    }
  }

  Manifest manifest(String schema) throws HopException {
    Manifest manifest = new Manifest();
    Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
    attributes.put(Attributes.Name.MAIN_CLASS, HopAirflowBundle.MAIN_CLASS);
    attributes.put(Attributes.Name.CLASS_PATH, launcherClassPath());
    attributes.putValue(HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION, schema);
    if (StringUtils.isNotBlank(hopVersion)) {
      attributes.putValue(HopAirflowBundle.ATTRIBUTE_HOP_VERSION, hopVersion);
    }
    attributes.putValue(HopAirflowBundle.ATTRIBUTE_PROJECT_NAME, projectName);
    attributes.putValue(
        HopAirflowBundle.ATTRIBUTE_RUN_CONFIGURATION,
        StringUtils.defaultIfBlank(runConfiguration, HopAirflowBundle.DEFAULT_RUN_CONFIGURATION));
    attributes.putValue(
        HopAirflowBundle.ATTRIBUTE_LOG_LEVEL,
        StringUtils.defaultIfBlank(logLevel, HopAirflowBundle.DEFAULT_LOG_LEVEL));
    if (!workflowPatterns.isEmpty()) {
      attributes.putValue(HopAirflowBundle.ATTRIBUTE_WORKFLOWS, String.join(",", workflowPatterns));
    }
    if (!environmentFiles.isEmpty()) {
      attributes.putValue(
          HopAirflowBundle.ATTRIBUTE_ENVIRONMENT_FILES, String.join(";", environmentFiles));
    }
    if (StringUtils.isNotBlank(dagIdPrefix)) {
      if (!dagIdPrefix.matches("[A-Za-z0-9_.-]+")) {
        throw new HopException(
            "A DAG id prefix can hold letters, digits, '_', '.' and '-' only, not '"
                + dagIdPrefix
                + "'");
      }
      attributes.putValue(HopAirflowBundle.ATTRIBUTE_DAG_ID_PREFIX, dagIdPrefix);
    }
    return manifest;
  }

  /** The launcher in the Hop installation of the workers, as a URL relative to the stub. */
  String launcherClassPath() throws HopException {
    String home = StringUtils.removeEnd(workerHopHome.replace('\\', '/'), "/");
    if (!home.startsWith("/")) {
      throw new HopException(
          "The Hop installation on the Airflow workers must be an absolute path, not '"
              + workerHopHome
              + "'");
    }
    String path =
        home + "/" + HopAirflowBundle.LAUNCHER_FOLDER + "/" + HopAirflowBundle.LAUNCHER_FILE_NAME;
    try {
      return new URI(null, null, path, null).getRawPath();
    } catch (URISyntaxException e) {
      throw new HopException("Not a valid path: " + path, e);
    }
  }

  private void checkEnvironmentFiles(FileObject project) throws IOException, HopException {
    for (String file : environmentFiles) {
      if (StringUtils.containsAny(file, ",;")
          || file.startsWith("/")
          || !project.resolveFile(file).isFile()) {
        throw new HopException(
            "Environment files must be files in the project, given relative to it, without a comma"
                + " or a semicolon: '"
                + file
                + "'");
      }
    }
  }

  /**
   * Copies the project to the target folder. A folder that holds something else than an earlier
   * bundle is left alone: replacing it would delete files the user wants to keep.
   */
  private static void copyProject(FileObject project, FileObject target)
      throws IOException, HopException {
    if (target.exists()) {
      if (!target.isFolder()) {
        throw new HopException(HopVfs.getFilename(target) + " is not a folder");
      }
      FileObject[] children = target.getChildren();
      if (children.length > 0) {
        // Only a folder this writer filled: a project with a committed stub has a stub too
        if (!target.resolveFile(BUNDLE_MARKER).exists()) {
          throw new HopException(
              HopVfs.getFilename(target)
                  + " is not empty and is not a DAG bundle that Hop copied there (it holds no "
                  + BUNDLE_MARKER
                  + "): choose an empty folder or an earlier DAG bundle");
        }
        // An earlier bundle: start again, so that removed workflows do not stay DAGs
        for (FileObject child : children) {
          child.deleteAll();
        }
      }
    }
    target.createFolder();
    target.copyFrom(project, new ProjectSelector(project, target));
    try (OutputStream out = HopVfs.getOutputStream(target.resolveFile(BUNDLE_MARKER), false)) {
      out.write(
          ("A copy of Hop project "
                  + HopVfs.getFilename(project)
                  + ", written by hop"
                  + " airflow-bundle. Every publish replaces everything in this folder.\n")
              .getBytes(StandardCharsets.UTF_8));
    }
  }

  private static boolean sameFolder(FileObject a, FileObject b) {
    return a.getName().getURI().equals(b.getName().getURI());
  }

  /** Everything but hidden files and folders (.git), an old stub and the target itself. */
  private static final class ProjectSelector implements FileSelector {
    private final FileObject project;
    private final FileObject target;

    ProjectSelector(FileObject project, FileObject target) {
      this.project = project;
      this.target = target;
    }

    @Override
    public boolean includeFile(FileSelectInfo info) throws Exception {
      FileObject file = info.getFile();
      if (sameFolder(file, project) || !traverseDescendents(info)) {
        return false;
      }
      boolean oldStub =
          file.getParent() != null
              && sameFolder(file.getParent(), project)
              && HopAirflowBundle.STUB_FILE_NAME.equals(file.getName().getBaseName());
      return !oldStub;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) throws Exception {
      FileObject file = info.getFile();
      if (sameFolder(file, project)) {
        return true;
      }
      return !file.getName().getBaseName().startsWith(".") && !sameFolder(file, target);
    }
  }

  /**
   * The supervisor schema version of the Airflow SDK this Hop installation ships with the launcher.
   * Airflow uses it to talk to the launcher, so it must match the workers' installation.
   */
  static String installedSchemaVersion() throws HopException {
    File launcherLib;
    try {
      File pluginJar =
          new File(
              AirflowBundleWriter.class
                  .getProtectionDomain()
                  .getCodeSource()
                  .getLocation()
                  .toURI());
      launcherLib = new File(pluginJar.getParentFile(), "launcher/lib");
    } catch (Exception e) {
      throw new HopException("Unable to find the Airflow plugin folder", e);
    }
    File[] sdkJars =
        launcherLib.listFiles(
            (folder, name) -> name.startsWith("airflow-sdk-") && name.endsWith(".jar"));
    if (sdkJars == null || sdkJars.length != 1) {
      throw new HopException(
          "Expected one Airflow SDK jar in "
              + launcherLib
              + ": the Airflow launcher is not installed with this plugin");
    }
    return schemaVersionOf(sdkJars[0]);
  }

  static String schemaVersionOf(File sdkJar) throws HopException {
    try (JarFile jar = new JarFile(sdkJar)) {
      Manifest manifest = jar.getManifest();
      String version =
          manifest == null
              ? null
              : manifest.getMainAttributes().getValue(HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      if (StringUtils.isBlank(version)) {
        throw new HopException(sdkJar + " has no " + HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      }
      return version.trim();
    } catch (IOException e) {
      throw new HopException("Unable to read " + sdkJar, e);
    }
  }
}
