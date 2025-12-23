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
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditManager;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;

public class ProjectsUtil {

  public static final String VARIABLE_PROJECT_HOME = "PROJECT_HOME";
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

    // Change the metadata provider in the GUI
    //
    if (hasHopMetadataProvider != null) {
      MultiMetadataProvider metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(variables);
      hasHopMetadataProvider.setMetadataProvider(metadataProvider);
      HopMetadataInstance.setMetadataProvider(metadataProvider);
      project.setMetadataProvider(metadataProvider);
    }

    // We store the project in the namespace singleton (used mainly in the GUI)
    //
    HopNamespace.setNamespace(projectName);

    // Save some history concerning the usage of the project
    // but only in case Hop was started by HopGui because that is the only case
    // where this info is valuable.
    //
    if (Const.getHopPlatformRuntime() != null && Const.getHopPlatformRuntime().equals("GUI")) {
      AuditManager.registerEvent(
          HopGui.DEFAULT_HOP_GUI_NAMESPACE, STRING_PROJECT_AUDIT_TYPE, projectName, "open");
    }

    // Signal others that we have a new active project
    //
    ExtensionPointHandler.callExtensionPoint(
        log, variables, Defaults.EXTENSION_POINT_PROJECT_ACTIVATED, projectName);
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

    boolean prjFound = false;

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<String> prjs = config.listProjectConfigNames();
    Iterator<String> iPrj = prjs.iterator();

    while (!prjFound && iPrj.hasNext()) {
      String p = iPrj.next();
      prjFound = p.equals(projectName);
    }

    return prjFound;
  }

  public static List<String> getParentProjectReferences(String projectName) throws HopException {

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<String> prjs = config.listProjectConfigNames();

    HopGui hopGui = HopGui.getInstance();
    List<String> parentProjectReferences = new ArrayList<>();
    ProjectConfig currentProjectConfig = config.findProjectConfig(projectName);

    if (currentProjectConfig == null) {
      parentProjectReferences = List.of();
    } else {
      for (String prj : prjs) {
        if (!prj.equals(projectName)) {
          ProjectConfig prjCfg = config.findProjectConfig(prj);
          Project thePrj = prjCfg.loadProject(hopGui.getVariables());
          if (thePrj != null) {
            if (thePrj.getParentProjectName() != null
                && thePrj.getParentProjectName().equals(projectName)) {
              parentProjectReferences.add(prj);
            }
          } else {
            hopGui.getLog().logError("Unable to load project '" + prj + "' from its configuration");
          }
        }
      }
    }
    return parentProjectReferences;
  }

  public static List<String> changeParentProjectReferences(String currentName, String newName)
      throws HopException {

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<String> prjs = config.listProjectConfigNames();

    HopGui hopGui = HopGui.getInstance();
    List<String> parentProjectReferences = new ArrayList<>();
    ProjectConfig currentProjectConfig = config.findProjectConfig(currentName);

    if (currentProjectConfig == null) {
      parentProjectReferences = List.of();
    } else {
      for (String prj : prjs) {
        if (!prj.equals(currentName)) {
          ProjectConfig prjCfg = config.findProjectConfig(prj);
          Project thePrj = prjCfg.loadProject(hopGui.getVariables());
          if (thePrj != null) {
            if (thePrj.getParentProjectName() != null
                && thePrj.getParentProjectName().equals(currentName)) {
              thePrj.setParentProjectName(newName);
            }
          } else {
            hopGui.getLog().logError("Unable to load project '" + prj + "' from its configuration");
          }
        }
      }
    }
    return parentProjectReferences;
  }
}
