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

package org.apache.hop.projects.project;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.util.ProjectsUtil;

/**
 * Copies configured folders from a parent project into the child project home using Hop VFS.
 * Invoked when a project is enabled. Never copies a whole tree with {@code copyFrom} (that empties
 * an existing destination).
 */
public final class ParentProjectFolderSynchronizer {

  private ParentProjectFolderSynchronizer() {
    // utility
  }

  /**
   * Apply {@link Project#getParentProjectFolders()} for the given child project. Failures on a
   * single mapping are logged and the rest continue; this method does not throw for copy problems.
   *
   * @param log log channel (may be null)
   * @param project child project
   * @param projectConfig child project registration
   * @param variables variable space with {@code PROJECT_HOME} and {@code PARENT_PROJECT_HOME} set
   */
  public static void synchronize(
      ILogChannel log, Project project, ProjectConfig projectConfig, IVariables variables) {
    if (project == null || projectConfig == null || variables == null) {
      return;
    }
    if (projectConfig.isReadOnly()) {
      logDetailed(log, "Skipping parent folder sync for read-only project");
      return;
    }
    List<ParentProjectFolder> folders = project.getParentProjectFolders();
    if (folders == null || folders.isEmpty()) {
      return;
    }
    if (StringUtils.isEmpty(project.getParentProjectName())) {
      return;
    }

    String childHomePath = variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME);
    String parentHomePath = variables.getVariable(ProjectsUtil.VARIABLE_PARENT_PROJECT_HOME);
    if (StringUtils.isEmpty(childHomePath) || StringUtils.isEmpty(parentHomePath)) {
      logError(
          log,
          "Cannot synchronize parent project folders: PROJECT_HOME or PARENT_PROJECT_HOME is not set");
      return;
    }

    try (FileObject childHome = HopVfs.getFileObject(childHomePath);
        FileObject parentHome = HopVfs.getFileObject(parentHomePath)) {
      if (!parentHome.exists()) {
        logError(log, "Parent project home does not exist: " + parentHomePath);
        return;
      }

      FileObject childConfigFile = resolveChildConfigFile(projectConfig, variables, childHome);
      try {
        for (ParentProjectFolder mapping : folders) {
          if (mapping == null) {
            continue;
          }
          try {
            synchronizeMapping(log, mapping, parentHome, childHome, childConfigFile);
          } catch (Exception e) {
            logError(
                log,
                "Error synchronizing parent folder '"
                    + Const.NVL(mapping.getFolder(), "")
                    + "' into project '"
                    + projectConfig.getProjectName()
                    + "'",
                e);
          }
        }
      } finally {
        closeQuietly(childConfigFile);
      }
    } catch (Exception e) {
      logError(log, "Error opening project homes for parent folder synchronization", e);
    }
  }

  static void synchronizeMapping(
      ILogChannel log,
      ParentProjectFolder mapping,
      FileObject parentHome,
      FileObject childHome,
      FileObject childConfigFile)
      throws FileSystemException, HopException {

    if (!mapping.isCopyOnce() && !mapping.isCopyOnEnable()) {
      return;
    }

    Pattern exclusion = compileExclusion(log, mapping);
    if (StringUtils.isNotBlank(mapping.getExclusionWildcard()) && exclusion == null) {
      // Invalid regex: skip the mapping so files the user meant to exclude are not copied.
      return;
    }

    FileObject source = null;
    FileObject dest = null;
    try {
      source = resolveUnderHome(parentHome, mapping.getFolder());
      dest = resolveUnderHome(childHome, mapping.getFolder());

      if (!isUnder(dest, childHome)) {
        logError(
            log,
            "Refusing to copy parent folder '"
                + Const.NVL(mapping.getFolder(), "")
                + "': destination is outside the child project home");
        return;
      }

      if (!source.exists()) {
        logError(log, "Parent project folder does not exist: " + source.getName().getURI());
        return;
      }

      boolean destEmpty = isMissingOrEmpty(dest, childConfigFile);
      boolean runCopyOnce = mapping.isCopyOnce() && destEmpty;
      boolean runOnEnable = mapping.isCopyOnEnable();
      if (!runOnEnable && !runCopyOnce) {
        logDetailed(
            log,
            "Skipping copy-once parent folder '"
                + Const.NVL(mapping.getFolder(), "")
                + "': destination already has files");
        return;
      }

      copyTree(log, source, dest, "", mapping.isOverwrite(), exclusion, childConfigFile);
    } catch (FileSystemException e) {
      logError(
          log,
          "Parent folder '"
              + Const.NVL(mapping.getFolder(), "")
              + "' is outside the project home (path traversal rejected)",
          e);
    } finally {
      if (source != parentHome) {
        closeQuietly(source);
      }
      if (dest != childHome) {
        closeQuietly(dest);
      }
    }
  }

  static FileObject resolveUnderHome(FileObject home, String folder) throws FileSystemException {
    if (home == null) {
      throw new FileSystemException("Project home is not set");
    }
    String relative = folder == null ? "" : folder.trim();
    if (relative.isEmpty() || ".".equals(relative) || "./".equals(relative)) {
      return home;
    }
    // NameScope.DESCENDENT_OR_SELF rejects ".." and absolute paths that leave home.
    return home.resolveFile(relative, NameScope.DESCENDENT_OR_SELF);
  }

  static boolean isMissingOrEmpty(FileObject dest, FileObject childConfigFile)
      throws FileSystemException {
    if (dest == null || !dest.exists()) {
      return true;
    }
    if (dest.getType() != FileType.FOLDER) {
      return false;
    }
    FileObject[] children = dest.getChildren();
    try {
      if (children == null || children.length == 0) {
        return true;
      }
      for (FileObject child : children) {
        if (".git".equals(child.getName().getBaseName())) {
          continue;
        }
        if (isProtectedConfigFile(child, childConfigFile)) {
          continue;
        }
        return false;
      }
      return true;
    } finally {
      closeQuietly(children);
    }
  }

  static boolean isUnder(FileObject file, FileObject directory) throws FileSystemException {
    if (file == null || directory == null) {
      return false;
    }
    String filePath = file.getName().getURI();
    String directoryPath = directory.getName().getURI();
    if (filePath.equals(directoryPath)) {
      return true;
    }
    String prefix = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";
    return filePath.startsWith(prefix);
  }

  static Pattern compileExclusion(ILogChannel log, ParentProjectFolder mapping) {
    String wildcard = mapping.getExclusionWildcard();
    if (StringUtils.isBlank(wildcard)) {
      return null;
    }
    try {
      return Pattern.compile(wildcard.trim());
    } catch (PatternSyntaxException e) {
      logError(
          log,
          "Invalid exclusion regular expression '"
              + wildcard
              + "' for parent folder '"
              + Const.NVL(mapping.getFolder(), "")
              + "': "
              + e.getMessage());
      return null;
    }
  }

  /**
   * True when the file should be skipped because of the user regular expression. Matches the base
   * name and the slash-separated path relative to the copied folder (not a VFS URI), same idea as
   * other Hop file wildcards.
   */
  static boolean matchesExclusion(Pattern exclusion, String baseName, String relativePath) {
    if (exclusion == null) {
      return false;
    }
    if (baseName != null && exclusion.matcher(baseName).matches()) {
      return true;
    }
    if (relativePath != null
        && !relativePath.isEmpty()
        && exclusion.matcher(relativePath).matches()) {
      return true;
    }
    return false;
  }

  static boolean isGitPath(String baseName, String relativePath) {
    if (".git".equals(baseName)) {
      return true;
    }
    if (relativePath == null || relativePath.isEmpty()) {
      return false;
    }
    return ".git".equals(relativePath)
        || relativePath.startsWith(".git/")
        || relativePath.contains("/.git/")
        || relativePath.endsWith("/.git");
  }

  static boolean isProtectedConfigFile(FileObject dest, FileObject childConfigFile)
      throws FileSystemException {
    if (dest == null || childConfigFile == null) {
      return false;
    }
    return dest.getName().getURI().equals(childConfigFile.getName().getURI());
  }

  private static void copyTree(
      ILogChannel log,
      FileObject source,
      FileObject dest,
      String relativePath,
      boolean overwrite,
      Pattern exclusion,
      FileObject childConfigFile)
      throws FileSystemException, HopException {

    String baseName = source.getName().getBaseName();
    if (isGitPath(baseName, relativePath)) {
      return;
    }

    if (source.getType() == FileType.FOLDER) {
      if (!dest.exists()) {
        dest.createFolder();
      }
      FileObject[] children = source.getChildren();
      try {
        if (children == null) {
          return;
        }
        for (FileObject child : children) {
          String childRelative =
              relativePath.isEmpty()
                  ? child.getName().getBaseName()
                  : relativePath + "/" + child.getName().getBaseName();
          FileObject destChild = dest.resolveFile(child.getName().getBaseName(), NameScope.CHILD);
          try {
            copyTree(log, child, destChild, childRelative, overwrite, exclusion, childConfigFile);
          } finally {
            closeQuietly(destChild);
          }
        }
      } finally {
        closeQuietly(children);
      }
      return;
    }

    if (isProtectedConfigFile(dest, childConfigFile)) {
      logDetailed(log, "Skipping child project configuration file: " + dest.getName().getURI());
      return;
    }
    if (matchesExclusion(exclusion, baseName, relativePath)) {
      logDetailed(log, "Skipping excluded file: " + relativePath);
      return;
    }
    if (dest.exists() && !overwrite) {
      logDetailed(log, "Keeping existing file: " + dest.getName().getURI());
      return;
    }

    FileObject destParent = dest.getParent();
    if (destParent != null && !destParent.exists()) {
      destParent.createFolder();
    }

    try (InputStream in = HopVfs.getInputStream(source);
        OutputStream out = HopVfs.getOutputStream(dest, false)) {
      in.transferTo(out);
    } catch (Exception e) {
      throw new HopException("Error copying '" + source.getName().getURI() + "'", e);
    }
    logDetailed(log, "Copied parent project file: " + relativePath);
  }

  private static FileObject resolveChildConfigFile(
      ProjectConfig projectConfig, IVariables variables, FileObject childHome) {
    try {
      String configPath = projectConfig.getActualProjectConfigFilename(variables);
      if (StringUtils.isNotEmpty(configPath)) {
        return HopVfs.getFileObject(configPath);
      }
    } catch (Exception e) {
      // Fall through to home + relative config filename
    }
    try {
      String configFilename = projectConfig.getConfigFilename();
      if (StringUtils.isEmpty(configFilename)) {
        return null;
      }
      return childHome.resolveFile(configFilename, NameScope.DESCENDENT_OR_SELF);
    } catch (Exception e) {
      return null;
    }
  }

  private static void logError(ILogChannel log, String message) {
    if (log != null) {
      log.logError(message);
    }
  }

  private static void logError(ILogChannel log, String message, Exception e) {
    if (log != null) {
      log.logError(message, e);
    }
  }

  private static void logDetailed(ILogChannel log, String message) {
    if (log != null && log.isDetailed()) {
      log.logDetailed(message);
    }
  }

  private static void closeQuietly(FileObject file) {
    if (file != null) {
      try {
        file.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private static void closeQuietly(FileObject[] files) {
    if (files == null) {
      return;
    }
    for (FileObject file : files) {
      closeQuietly(file);
    }
  }
}
