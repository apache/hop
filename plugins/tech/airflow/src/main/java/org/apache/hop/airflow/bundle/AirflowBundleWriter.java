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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.airflow.engine.AirflowDagIds;
import org.apache.hop.airflow.engine.AirflowDagTasks;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.action.IAction;

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

  /** The tasks of the DAGs; null for {@link AirflowDagTasks#PER_ACTION}. */
  private AirflowDagTasks tasks;

  private String hopVersion;

  /** The supervisor schema version for the stub; read from the installed launcher when null. */
  private String schemaVersion;

  /**
   * The plugin ids of the actions that don't evaluate their result; read from the plugin registry
   * when null.
   */
  private List<String> notEvaluatingActions;

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
      checkDagIds(project);
      String schema = schemaVersion != null ? schemaVersion : installedSchemaVersion();
      // Every check before the copy: a publish that fails leaves the bundle as it was
      Manifest manifest = manifest(schema);

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
          JarOutputStream jar = new JarOutputStream(out, manifest)) {
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
    attributes.putValue(
        HopAirflowBundle.ATTRIBUTE_TASKS,
        (tasks == null ? AirflowDagTasks.PER_ACTION : tasks).getCode());
    List<String> notEvaluating =
        notEvaluatingActions != null ? notEvaluatingActions : registeredNotEvaluatingActions();
    if (notEvaluating != null) {
      attributes.putValue(
          HopAirflowBundle.ATTRIBUTE_NOT_EVALUATING_ACTIONS, String.join(",", notEvaluating));
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

  /**
   * The actions whose {@link IAction#isEvaluation()} is false: Hop follows only the unconditional
   * hops that leave them, and so must the DAG. The launcher can't ask Hop, so the stub lists them.
   * An action Hop can't load is left out: it can't run on the workers either.
   *
   * @return the plugin ids, or null without action plugins: the launcher then uses its own list
   */
  static List<String> registeredNotEvaluatingActions() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(ActionPluginType.class);
    if (plugins.isEmpty()) {
      return null;
    }
    TreeSet<String> ids = new TreeSet<>();
    for (IPlugin plugin : plugins) {
      try {
        if (!registry.loadClass(plugin, IAction.class).isEvaluation()) {
          ids.addAll(Arrays.asList(plugin.getIds()));
        }
      } catch (Exception | LinkageError e) {
        // Not loadable here
      }
    }
    return new ArrayList<>(ids);
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

  /**
   * Two workflows whose paths only differ in characters a DAG id cannot hold, such as {@code a
   * b.hwf} and {@code a_b.hwf}, would get the same DAG id.
   */
  private void checkDagIds(FileObject project) throws IOException, HopException {
    List<PathMatcher> matchers = new ArrayList<>();
    for (String pattern : workflowPatterns) {
      matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
    }
    Map<String, String> workflows = new LinkedHashMap<>();
    FileObject[] files = project.findFiles(new WorkflowSelector(project));
    if (files == null) {
      return;
    }
    List<String> relativePaths = new ArrayList<>();
    for (FileObject file : files) {
      relativePaths.add(AirflowDagIds.relativePath(project, file));
    }
    relativePaths.sort(null);
    for (String relative : relativePaths) {
      if (!matchers.isEmpty() && matchers.stream().noneMatch(m -> m.matches(Path.of(relative)))) {
        continue;
      }
      String dagId = AirflowDagIds.dagId(dagIdPrefix, projectName, relative);
      String other = workflows.putIfAbsent(dagId, relative);
      if (other != null) {
        throw new HopException(
            "Workflows "
                + other
                + " and "
                + relative
                + " would both become DAG "
                + dagId
                + ": rename one of them");
      }
    }
  }

  /** The workflows of a project, outside hidden folders such as .git. */
  private static final class WorkflowSelector implements FileSelector {
    private final FileObject project;

    WorkflowSelector(FileObject project) {
      this.project = project;
    }

    @Override
    public boolean includeFile(FileSelectInfo info) {
      return info.getFile().getName().getBaseName().endsWith(".hwf");
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      FileObject folder = info.getFile();
      return sameFolder(folder, project) || !folder.getName().getBaseName().startsWith(".");
    }
  }

  private void checkEnvironmentFiles(FileObject project) throws IOException, HopException {
    if (!environmentFiles.isEmpty()
        && (projectName.contains(":")
            || projectName.length() == 1
            || List.of("file", "zip").contains(projectName.toLowerCase()))) {
      // The workers pass the files on as name=project:files, which reads "c:" as a drive and
      // "file:" or "zip:" as a URL
      throw new HopException(
          "Hop cannot pass the environment files of project '"
              + projectName
              + "' on to the workers: use a project name of two or more characters, without a"
              + " colon, other than 'file' or 'zip'");
    }
    for (String file : environmentFiles) {
      FileObject resolved = StringUtils.isBlank(file) ? null : project.resolveFile(file);
      // Only files in the project are copied into the bundle
      boolean inProject =
          resolved != null
              && project.getName().isDescendent(resolved.getName(), NameScope.DESCENDENT);
      if (StringUtils.containsAny(file, ",;")
          || file.startsWith("/")
          || !inProject
          || !resolved.isFile()) {
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
      // Only a folder this writer filled: a project with a committed stub has a stub too
      if (target.getChildren().length > 0 && !target.resolveFile(BUNDLE_MARKER).exists()) {
        throw new HopException(
            HopVfs.getFilename(target)
                + " is not empty and is not a DAG bundle that Hop copied there (it holds no "
                + BUNDLE_MARKER
                + "): choose an empty folder or an earlier DAG bundle");
      }
    }
    target.createFolder();
    // The marker first, so a copy that fails half way leaves a folder Hop may replace again
    try (OutputStream out = HopVfs.getOutputStream(target.resolveFile(BUNDLE_MARKER), false)) {
      out.write(
          ("A copy of Hop project "
                  + HopVfs.getFilename(project)
                  + ", written by hop airflow-bundle. Every publish replaces everything in this"
                  + " folder.\n")
              .getBytes(StandardCharsets.UTF_8));
    }
    // Replace the files in place, then remove what the project no longer has: DAG runs that
    // read the bundle meanwhile never find it empty
    target.copyFrom(project, new ProjectSelector(project, target));
    removeLeftovers(project, target);
  }

  /** Removes what an earlier publish left in the bundle and the project no longer holds. */
  private static void removeLeftovers(FileObject project, FileObject target) throws IOException {
    for (FileObject file : target.findFiles(Selectors.EXCLUDE_SELF)) {
      if (!file.exists()) {
        // In a folder removed before
        continue;
      }
      String relative = target.getName().getRelativeName(file.getName());
      if (BUNDLE_MARKER.equals(relative) || HopAirflowBundle.STUB_FILE_NAME.equals(relative)) {
        continue;
      }
      FileObject source = project.resolveFile(relative);
      boolean hidden = Arrays.stream(relative.split("/")).anyMatch(part -> part.startsWith("."));
      if (hidden || !source.exists() || source.isFolder() != file.isFolder()) {
        file.deleteAll();
      }
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
    FileObject launcherLib;
    try {
      FileObject pluginJar =
          HopVfs.getFileObject(
              AirflowBundleWriter.class
                  .getProtectionDomain()
                  .getCodeSource()
                  .getLocation()
                  .toURI()
                  .toString());
      launcherLib = pluginJar.getParent().resolveFile("launcher/lib");
    } catch (Exception e) {
      throw new HopException("Unable to find the Airflow plugin folder", e);
    }
    List<FileObject> sdkJars = new ArrayList<>();
    try {
      if (launcherLib.isFolder()) {
        for (FileObject child : launcherLib.getChildren()) {
          String name = child.getName().getBaseName();
          if (name.startsWith("airflow-sdk-") && name.endsWith(".jar")) {
            sdkJars.add(child);
          }
        }
      }
    } catch (IOException e) {
      throw new HopException("Unable to list " + HopVfs.getFilename(launcherLib), e);
    }
    if (sdkJars.size() != 1) {
      throw new HopException(
          "Expected one Airflow SDK jar in "
              + HopVfs.getFilename(launcherLib)
              + ": the Airflow launcher is not installed with this plugin");
    }
    return schemaVersionOf(sdkJars.get(0));
  }

  static String schemaVersionOf(FileObject sdkJar) throws HopException {
    String name = HopVfs.getFilename(sdkJar);
    try (InputStream in = HopVfs.getInputStream(sdkJar);
        JarInputStream jar = new JarInputStream(in)) {
      Manifest manifest = jar.getManifest();
      // JarInputStream only finds a manifest among the first entries
      for (JarEntry entry = jar.getNextJarEntry();
          manifest == null && entry != null;
          entry = jar.getNextJarEntry()) {
        if (JarFile.MANIFEST_NAME.equalsIgnoreCase(entry.getName())) {
          manifest = new Manifest(jar);
        }
      }
      String version =
          manifest == null
              ? null
              : manifest.getMainAttributes().getValue(HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      if (StringUtils.isBlank(version)) {
        throw new HopException(name + " has no " + HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION);
      }
      return version.trim();
    } catch (IOException e) {
      throw new HopException("Unable to read " + name, e);
    }
  }
}
