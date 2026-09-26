/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ParentProjectFolderSynchronizer;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;

public class ProjectsUtil {

  private static final Class<?> PKG = Project.class;

  public static final String VARIABLE_PROJECT_HOME = "PROJECT_HOME";
  public static final String VARIABLE_PARENT_PROJECT_HOME = "PARENT_PROJECT_HOME";
  public static final String VARIABLE_PARENT_PROJECT_NAME = "PARENT_PROJECT_NAME";
  public static final String VARIABLE_HOP_DATASETS_FOLDER = "HOP_DATASETS_FOLDER";
  public static final String VARIABLE_HOP_UNIT_TESTS_FOLDER = "HOP_UNIT_TESTS_FOLDER";

  public static final String STRING_PROJECTS_AUDIT_GROUP = "projects";
  public static final String STRING_PROJECT_AUDIT_TYPE = "project";
  public static final String STRING_ENVIRONMENT_AUDIT_TYPE = "environment";

  /**
   * Enable the specified project Force reload of a number of settings
   *
   * @param log the log channel to log to
   * @param projectName
   * @param project
   * @param variables
   * @param configurationFiles
   * @throws HopException
   * @throws HopException
   */
  public static void enableProject(
      ILogChannel log,
      String projectName,
      Project project,
      IVariables variables,
      List<String> configurationFiles,
      String environmentName,
      IHasHopMetadataProvider hasHopMetadataProvider)
      throws HopException {

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException("Error enabling project " + projectName + ": it is not configured.");
    }

    // Clear the database cache when switching?
    if (config.isClearingDbCacheWhenSwitching()) {
      if (log.isDetailed()) {
        log.logDetailed(
            "Clearing the database cache when switching between projects or environments.");
      }
      DbCache.clearAll();
    }

    // Variable system variables but also apply them to variables
    // We'll use those to change the loaded variables in HopGui
    //
    project.modifyVariables(variables, projectConfig, configurationFiles, environmentName);

    ProjectsConfigHelper.applyProjectExportFiles(
        log, projectConfig.getProjectHome(), variables, null, true, false);

    // Re-bind the process-global two-way password encoder from project/environment variables
    // (HOP_PASSWORD_ENCODER_PLUGIN, HOP_AES_ENCODER_KEY / HOP_AES_ENCODER_KEY_FILE). This resets
    // AES keys between projects and allows falling back to Hop obfuscation when unset.
    //
    try {
      Encr.initFromVariables(variables);
      String encoderPluginId =
          Const.NVL(
              variables.getVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN),
              Const.NVL(System.getProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop"));
      if (log.isBasic()) {
        log.logBasic(
            "Two-way password encoder initialized with plugin ID '"
                + encoderPluginId
                + "' for project '"
                + projectName
                + "'");
      }
    } catch (HopException e) {
      throw new HopException(
          "Error initializing the two-way password encoder for project '" + projectName + "'", e);
    }

    // Point metadata at the project's metadataBaseFolder (HOP_METADATA_FOLDER). Do this even when
    // the caller has no IHasHopMetadataProvider (root CLI mixins): HopRun and other subcommands
    // pick the provider up from HopMetadataInstance.
    //
    MultiMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(variables);
    if (StringUtils.isNotEmpty(project.getParentProjectName())) {
      ProjectConfig parentPc = config.findProjectConfig(project.getParentProjectName());
      if (parentPc != null) {
        ProjectsConfigHelper.applyProjectExportFiles(
            log, parentPc.getProjectHome(), variables, metadataProvider, false, true);
      }
    }
    ProjectsConfigHelper.applyProjectExportFiles(
        log, projectConfig.getProjectHome(), variables, metadataProvider, false, true);
    if (hasHopMetadataProvider != null) {
      hasHopMetadataProvider.setMetadataProvider(metadataProvider);
    }
    HopMetadataInstance.setMetadataProvider(metadataProvider);
    project.setMetadataProvider(metadataProvider);
    if (log.isBasic()) {
      log.logBasic(
          "Project '"
              + projectName
              + "' metadata folder: "
              + Const.NVL(variables.getVariable(Const.HOP_METADATA_FOLDER), ""));
    }

    // The named VFS connections live in the metadata of this project, so hand HopVfs the variables
    // to find them with. This also resets the file system manager: the providers of the previous
    // project are gone and those of this one are registered the next time VFS is used.
    //
    HopVfs.setBootstrapVariables(variables);

    // We store the project in the namespace singleton (used mainly in the GUI)
    //
    HopNamespace.setNamespace(projectName);

    // Copy configured parent-project folders into this project home. Do not abort enabling the
    // project when a template folder is missing or a file cannot be copied.
    //
    try {
      ParentProjectFolderSynchronizer.synchronize(log, project, projectConfig, variables);
    } catch (Exception e) {
      log.logError(
          "Error synchronizing parent project folders for project '" + projectName + "'", e);
    }

    // Save some history concerning the usage of the project
    // but only in case Hop was started by HopGui because that is the only case
    // where this info is valuable. Audit I/O must not block enabling a project (e.g. Docker audit
    // folder permission issues).
    //
    if (Const.getHopPlatformRuntime() != null && Const.getHopPlatformRuntime().equals("GUI")) {
      try {
        AuditManager.registerEvent(
            HopGui.DEFAULT_HOP_GUI_NAMESPACE, STRING_PROJECT_AUDIT_TYPE, projectName, "open");
      } catch (Exception e) {
        log.logError(
            "Unable to register project open audit event for '"
                + projectName
                + "' (continuing enable): "
                + e.getMessage());
      }
    }

    // Signal others that we have a new active project
    //
    ExtensionPointHandler.callExtensionPoint(
        log, variables, Defaults.EXTENSION_POINT_PROJECT_ACTIVATED, projectName);

    // Plugin-agnostic attributes context for marketplace, resource checks, etc.
    // Thrown HopException from listeners aborts environment enablement.
    //
    AttributesContext attributesContext =
        buildAttributesContext(config, projectConfig, projectName, environmentName, variables);
    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.HopProjectEnvironmentAfterEnabled.id, attributesContext);
    ProjectsConfigHelper.markEnabled(projectName, environmentName);
  }

  /**
   * Build a core {@link AttributesContext} for the enabled project/environment so optional plugins
   * can read identity fields and namespaced {@link org.apache.hop.core.IAttributes} groups without
   * depending on Projects classes.
   */
  public static AttributesContext buildAttributesContext(
      ProjectsConfig config,
      ProjectConfig projectConfig,
      String projectName,
      String environmentName,
      IVariables variables)
      throws HopException {
    AttributesContext context = new AttributesContext();
    context.setProjectName(projectName);
    context.setEnvironmentName(environmentName);

    if (projectConfig != null) {
      try {
        String home = projectConfig.getProjectHome();
        if (variables != null && StringUtils.isNotEmpty(home)) {
          home = variables.resolve(home);
        }
        context.setProjectHome(home);
      } catch (Exception e) {
        // best-effort project home
        context.setProjectHome(projectConfig.getProjectHome());
      }
    }

    LifecycleEnvironment environment =
        StringUtils.isNotEmpty(environmentName) && config != null
            ? config.findEnvironment(environmentName)
            : null;
    if (environment != null) {
      context.setPurpose(environment.getPurpose());
      if (environment.getConfigurationFiles() != null) {
        context.setConfigurationFiles(new ArrayList<>(environment.getConfigurationFiles()));
      }
      context.copyAttributesFrom(environment);
    }
    return context;
  }

  public static void validateFileInProject(
      ILogChannel log, String filename, ProjectConfig projectConfig, IVariables variables)
      throws HopException, FileSystemException {
    String projectHome = projectConfig.getProjectHome();
    if (StringUtils.isNotEmpty(filename)) {
      // See that this filename is located under the environment home folder
      //
      log.logBasic(
          "Validation against environment '"
              + projectConfig.getProjectName()
              + "' in home folder : "
              + projectHome);

      FileObject envHome = HopVfs.getFileObject(projectHome);
      FileObject transFile = HopVfs.getFileObject(filename);
      if (!isInSubDirectory(transFile, envHome)) {
        throw new HopException(
            "File '"
                + filename
                + "' does not live in the configured environment home folder : '"
                + projectHome
                + "'");
      }
    }
  }

  private static boolean isInSubDirectory(FileObject file, FileObject directory)
      throws FileSystemException {

    String filePath = file.getName().getURI();
    String directoryPath = directory.getName().getURI();

    // Same?
    if (filePath.equals(directoryPath)) {
      System.out.println("Found " + filePath + " in directory " + directoryPath);
      return true;
    }

    if (filePath.startsWith(directoryPath)) {
      return true;
    }

    FileObject parent = file.getParent();

    return parent != null && isInSubDirectory(parent, directory);
  }

  public static void validateFileInProject(
      ILogChannel log, String executableFilename, IVariables variables)
      throws FileSystemException, HopException {

    if (StringUtils.isEmpty(executableFilename)) {
      // Repo or remote
      return;
    }

    // What is the active project?
    //
    String activeProjectName = System.getProperty(Defaults.VARIABLE_HOP_PROJECT_NAME);
    if (StringUtils.isEmpty(activeProjectName)) {
      // Nothing to be done here...
      //
      return;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    log.logBasic("Validating active project '" + activeProjectName + "'");
    ProjectConfig projectConfig = config.findProjectConfig(activeProjectName);

    if (projectConfig == null) {
      throw new HopException("Project '" + activeProjectName + "' is not defined");
    }

    Project project = projectConfig.loadProject(variables);
    if (project.isEnforcingExecutionInHome()) {
      ProjectsUtil.validateFileInProject(log, executableFilename, projectConfig, variables);
    }
  }

  /**
   * Returns true if given project exists
   *
   * @param projectName
   * @return
   */
  public static boolean projectExists(String projectName) {
    return ProjectsConfigSingleton.getConfig().findProjectConfig(projectName) != null;
  }

  /**
   * Find the registered projects which have the given project as their parent project.
   *
   * @param projectName the name of the parent project
   * @return the names of the child projects
   */
  public static List<String> getParentProjectReferences(String projectName) throws HopException {
    HopGui hopGui = HopGui.getInstance();
    return getParentProjectReferences(projectName, hopGui.getVariables(), hopGui.getLog());
  }

  /**
   * Find the registered projects which have the given project as their parent project. Projects
   * which can't be loaded are logged and skipped.
   *
   * @param projectName the name of the parent project
   * @param variables the variables to resolve the project locations with
   * @param log the log channel to report projects which can't be loaded
   * @return the names of the child projects
   */
  public static List<String> getParentProjectReferences(
      String projectName, IVariables variables, ILogChannel log) {
    List<String> references = new ArrayList<>();
    if (StringUtils.isEmpty(projectName)) {
      return references;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    for (String name : config.listProjectConfigNames()) {
      if (name.equalsIgnoreCase(projectName)) {
        continue;
      }
      Project project = loadProject(config.findProjectConfig(name), variables, log);
      if (project != null && projectName.equalsIgnoreCase(project.getParentProjectName())) {
        references.add(name);
      }
    }
    return references;
  }

  /** Saves the projects configuration in hop-config.json after a project registration changed. */
  @FunctionalInterface
  public interface IProjectsConfigSaver {
    void save() throws HopException;
  }

  /** A project which uses the renamed project as its parent project. */
  private record ChildProject(String name, Project project) {}

  /**
   * Verify that a project can be renamed. Every project which uses it as its parent project must be
   * writable, and every registered project must be readable to know whether it's one of them.
   * Nothing is changed. Projects with a home folder which doesn't exist are skipped: they have no
   * configuration to update.
   *
   * @param currentName the current name of the project
   * @param newName the new name of the project
   * @param variables the variables to resolve the project locations with
   * @param log the log channel
   * @return the names of the projects which will get the new parent project name
   * @throws ProjectRenameBlockedException naming every project which blocks the rename and why
   */
  public static List<String> checkProjectRename(
      String currentName, String newName, IVariables variables, ILogChannel log)
      throws ProjectRenameBlockedException {
    return findChildProjects(currentName, newName, variables, log).stream()
        .map(ChildProject::name)
        .toList();
  }

  /**
   * Save a project registration, renaming the project when its name changed. A rename is all or
   * nothing: the default project, standard parent project and lifecycle environments follow the new
   * name, the registration is saved and then the projects which use it as their parent project are
   * updated and saved. The registration is saved first so that no project ever points to an
   * unregistered parent project. If a project can't be saved, everything is restored to the
   * previous name.
   *
   * @param currentName the name the project is registered with
   * @param projectConfig the updated registration, possibly with a new name. This can be the
   *     registered instance itself.
   * @param variables the variables to resolve the project locations with
   * @param log the log channel
   * @param saver saves the projects configuration
   * @return the names of the projects which now use the new name as their parent project
   * @throws ProjectRenameBlockedException when the rename can't be done, nothing was changed
   * @throws HopException when saving failed, the rename was undone
   */
  public static List<String> saveProjectConfig(
      String currentName,
      ProjectConfig projectConfig,
      IVariables variables,
      ILogChannel log,
      IProjectsConfigSaver saver)
      throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    String newName = projectConfig.getProjectName();
    if (StringUtils.isEmpty(currentName) || currentName.equals(newName)) {
      config.updateProjectConfig(currentName, projectConfig);
      saver.save();
      return List.of();
    }

    List<ChildProject> children;
    try {
      children = findChildProjects(currentName, newName, variables, log);
    } catch (ProjectRenameBlockedException e) {
      projectConfig.setProjectName(currentName);
      throw e;
    }

    config.updateProjectConfig(currentName, projectConfig);
    try {
      saver.save();
    } catch (HopException e) {
      revertRename(config, projectConfig, currentName, newName);
      throw e;
    }

    List<ChildProject> updated = new ArrayList<>();
    for (ChildProject child : children) {
      try {
        child.project().setParentProjectName(newName);
        child.project().saveToFile();
        updated.add(child);
      } catch (Exception e) {
        // The failed save may have left a partial file behind: write the old name back if we can
        restoreParentProjectName(List.of(child), currentName, log);
        List<String> notRestored = restoreParentProjectName(updated, currentName, log);
        revertRename(config, projectConfig, currentName, newName);
        try {
          saver.save();
        } catch (HopException saveException) {
          log.logError(
              "Unable to save the restored registration of project '" + currentName + "'",
              saveException);
        }
        String message =
            notRestored.isEmpty()
                ? BaseMessages.getString(
                    PKG, "ProjectRename.SaveFailed.Message", currentName, newName, child.name())
                : BaseMessages.getString(
                    PKG,
                    "ProjectRename.SaveFailed.NotRestored",
                    currentName,
                    newName,
                    child.name(),
                    String.join(", ", notRestored));
        throw new HopException(message, e);
      }
    }
    return updated.stream().map(ChildProject::name).toList();
  }

  private static List<ChildProject> findChildProjects(
      String currentName, String newName, IVariables variables, ILogChannel log)
      throws ProjectRenameBlockedException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<ChildProject> children = new ArrayList<>();
    List<String> blocking = new ArrayList<>();
    List<String> reasons = new ArrayList<>();
    for (String name : config.listProjectConfigNames()) {
      if (name.equalsIgnoreCase(currentName)) {
        continue;
      }
      ProjectConfig projectConfig = config.findProjectConfig(name);
      String home = variables.resolve(projectConfig.getProjectHome());
      Project project;
      try {
        if (StringUtils.isEmpty(home) || !HopVfs.getFileObject(home).exists()) {
          log.logDetailed(
              "Project '" + name + "' is skipped, its home folder '" + home + "' doesn't exist");
          continue;
        }
        project = projectConfig.loadProject(variables);
      } catch (Exception e) {
        blocking.add(name);
        reasons.add(
            BaseMessages.getString(
                PKG, "ProjectRename.Blocked.Unreadable", name, currentName, describeCause(e)));
        continue;
      }
      if (!currentName.equalsIgnoreCase(project.getParentProjectName())) {
        continue;
      }
      if (projectConfig.isReadOnly()) {
        blocking.add(name);
        reasons.add(
            BaseMessages.getString(PKG, "ProjectRename.Blocked.ReadOnly", name, currentName));
      } else if (ProjectConfig.isArchiveUri(home)) {
        blocking.add(name);
        reasons.add(
            BaseMessages.getString(PKG, "ProjectRename.Blocked.Archive", name, currentName, home));
      } else {
        children.add(new ChildProject(name, project));
      }
    }
    if (!blocking.isEmpty()) {
      throw new ProjectRenameBlockedException(
          BaseMessages.getString(
              PKG,
              "ProjectRename.Blocked.Message",
              currentName,
              newName,
              String.join(Const.CR, reasons)),
          blocking);
    }
    return children;
  }

  /**
   * @return the names of the projects which couldn't be restored
   */
  private static List<String> restoreParentProjectName(
      List<ChildProject> children, String parentProjectName, ILogChannel log) {
    List<String> notRestored = new ArrayList<>();
    for (ChildProject child : children) {
      try {
        child.project().setParentProjectName(parentProjectName);
        child.project().saveToFile();
      } catch (Exception e) {
        log.logError(
            "Unable to restore parent project '"
                + parentProjectName
                + "' of project '"
                + child.name()
                + "'",
            e);
        notRestored.add(child.name());
      }
    }
    return notRestored;
  }

  private static void revertRename(
      ProjectsConfig config, ProjectConfig projectConfig, String currentName, String newName) {
    ProjectConfig registered = config.findProjectConfig(newName);
    if (registered != null) {
      registered.setProjectName(currentName);
    }
    projectConfig.setProjectName(currentName);
    config.renameProjectReferences(newName, currentName);
  }

  /** The first line of the root cause message: enough to tell the user what's wrong. */
  private static String describeCause(Exception e) {
    Throwable root = ExceptionUtils.getRootCause(e);
    String message = root == null ? null : root.getMessage();
    if (StringUtils.isBlank(message)) {
      return (root == null ? e : root).getClass().getSimpleName();
    }
    return message.strip().lines().findFirst().orElse(message).strip();
  }

  private static Project loadProject(
      ProjectConfig projectConfig, IVariables variables, ILogChannel log) {
    try {
      return projectConfig.loadProject(variables);
    } catch (Exception e) {
      log.logError(
          "Unable to load project '" + projectConfig.getProjectName() + "' from its configuration",
          e);
      return null;
    }
  }
}
