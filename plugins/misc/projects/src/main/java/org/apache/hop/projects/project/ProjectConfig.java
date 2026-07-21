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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

public class ProjectConfig {

  /**
   * VFS schemes that provide read-only access to archive contents (Zip, Jar and Tar family). See
   * https://commons.apache.org/proper/commons-vfs/filesystems.html#Zip.2C_Jar_and_Tar
   */
  private static final String[] ARCHIVE_URI_SCHEMES = {
    "zip:", "jar:", "tar:", "tgz:", "tbz2:",
  };

  protected String projectName;
  protected String projectHome;
  protected String configFilename;

  /**
   * When true, Hop will not write to the project's configuration file (project-config.json). Useful
   * for projects opened from archives (zip/jar/tar), HTTP locations, or other read-only folders.
   */
  protected boolean readOnly;

  /**
   * Optional organization group for this project registration (stored in hop-config.json), e.g.
   * "Clients" or "Legacy". Empty means ungrouped.
   */
  protected String group;

  /**
   * Optional tags for filtering and organization (stored in hop-config.json). A project may have
   * multiple tags.
   */
  protected List<String> tags = new ArrayList<>();

  public ProjectConfig() {
    super();
  }

  public ProjectConfig(String projectName, String projectHome, String configFilename) {
    super();
    this.projectName = projectName;
    this.projectHome = projectHome;
    this.configFilename = configFilename;
  }

  /**
   * Returns true when the given path is a Commons VFS archive URI (zip, jar, tar, tgz, tbz2),
   * including nested forms such as {@code jar:zip:outer.zip!/nested.jar!/dir}.
   *
   * @param path project home path or URI (may be null/empty)
   * @return true if the path uses an archive scheme
   */
  public static boolean isArchiveUri(String path) {
    if (StringUtils.isEmpty(path)) {
      return false;
    }
    String lower = path.trim().toLowerCase(Locale.ROOT);
    for (String scheme : ARCHIVE_URI_SCHEMES) {
      if (lower.startsWith(scheme)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectConfig that = (ProjectConfig) o;
    return Objects.equals(projectName, that.projectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectName);
  }

  /**
   * The full path to the project config filename
   *
   * @param variables
   * @return The path to the project config filename
   * @throws HopException In case the home folder doesn't exist or an invalid filename/path is being
   *     used.
   */
  public String getActualProjectConfigFilename(IVariables variables) throws HopException {
    try {
      String actualHomeFolder = variables.resolve(getProjectHome());
      FileObject actualHome = HopVfs.getFileObject(actualHomeFolder);
      if (!actualHome.exists()) {
        throw new HopException(
            "Project '"
                + getProjectName()
                + "' home folder '"
                + actualHomeFolder
                + "' does not exist");
      }
      String actualConfigFilename = variables.resolve(getConfigFilename());
      // Use VFS resolve so archive/HTTP URIs work (FilenameUtils.concat mangles schemes).
      // For plain local paths keep the previous FilenameUtils behaviour for compatibility.
      //
      String scheme = actualHome.getName().getScheme();
      if (scheme != null && !"file".equalsIgnoreCase(scheme)) {
        FileObject configFile = actualHome.resolveFile(actualConfigFilename);
        return configFile.getName().getURI();
      }
      String fullFilename = FilenameUtils.concat(actualHome.toString(), actualConfigFilename);
      if (fullFilename == null) {
        throw new HopException(
            "Unable to determine full path to the configuration file '"
                + actualConfigFilename
                + "' in home folder '"
                + actualHomeFolder);
      }
      return fullFilename;
    } catch (Exception e) {
      throw new HopException("Error calculating actual project config filename", e);
    }
  }

  public Project loadProject(IVariables variables) throws HopException {
    String configFilename = getActualProjectConfigFilename(variables);
    if (configFilename == null) {
      String projHome = variables.resolve(getProjectHome());
      String confFile = variables.resolve(getConfigFilename());
      throw new HopException(
          "Invalid project folder provided: home folder: '"
              + projHome
              + "', config file: '"
              + confFile
              + "'");
    }
    Project project = new Project(configFilename);
    try {
      if (HopVfs.getFileObject(configFilename).exists()) {
        project.readFromFile();
      }
    } catch (Exception e) {
      throw new HopException(
          "Error checking config filename '" + configFilename + "' existence while loading project",
          e);
    }
    return project;
  }

  /**
   * Gets projectName
   *
   * @return value of projectName
   */
  public String getProjectName() {
    return projectName;
  }

  /**
   * @param projectName The projectName to set
   */
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  /**
   * Gets projectHome
   *
   * @return value of projectHome
   */
  public String getProjectHome() {
    return projectHome;
  }

  /**
   * @param projectHome The projectHome to set
   */
  public void setProjectHome(String projectHome) {
    this.projectHome = projectHome;
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  public String getConfigFilename() {
    return configFilename;
  }

  /**
   * @param configFilename The configFilename to set
   */
  public void setConfigFilename(String configFilename) {
    this.configFilename = configFilename;
  }

  /**
   * Gets readOnly
   *
   * @return value of readOnly
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * @param readOnly The readOnly flag to set
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Gets group
   *
   * @return value of group
   */
  public String getGroup() {
    return group;
  }

  /**
   * @param group The group to set
   */
  public void setGroup(String group) {
    this.group = group;
  }

  /**
   * Gets tags
   *
   * @return value of tags (never null)
   */
  public List<String> getTags() {
    if (tags == null) {
      tags = new ArrayList<>();
    }
    return tags;
  }

  /**
   * @param tags The tags to set
   */
  public void setTags(List<String> tags) {
    this.tags = tags != null ? new ArrayList<>(tags) : new ArrayList<>();
  }

  /**
   * Comma-separated display form of tags (for tables and text fields).
   *
   * @return joined tags, or empty string
   */
  public String getTagsAsDisplayString() {
    if (tags == null || tags.isEmpty()) {
      return "";
    }
    return String.join(", ", tags);
  }

  /**
   * Parse a comma/semicolon-separated tag string into a list of trimmed non-empty tags.
   *
   * @param text free-form tags text
   * @return list of tags (never null)
   */
  public static List<String> parseTags(String text) {
    if (StringUtils.isEmpty(text)) {
      return new ArrayList<>();
    }
    return Arrays.stream(text.split("[,;]"))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Whether this project registration matches a free-text filter (name, group, home, tags).
   *
   * @param filter filter text (null/empty matches all)
   * @return true if the project should be shown
   */
  public boolean matchesFilter(String filter) {
    if (StringUtils.isEmpty(filter)) {
      return true;
    }
    String needle = filter.trim().toLowerCase(Locale.ROOT);
    if (containsIgnoreCase(projectName, needle)
        || containsIgnoreCase(group, needle)
        || containsIgnoreCase(projectHome, needle)
        || containsIgnoreCase(configFilename, needle)
        || containsIgnoreCase(getTagsAsDisplayString(), needle)) {
      return true;
    }
    if (tags != null) {
      for (String tag : tags) {
        if (containsIgnoreCase(tag, needle)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsIgnoreCase(String value, String needleLower) {
    return value != null && value.toLowerCase(Locale.ROOT).contains(needleLower);
  }
}
