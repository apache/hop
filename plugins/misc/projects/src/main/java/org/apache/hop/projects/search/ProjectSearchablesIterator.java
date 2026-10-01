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

package org.apache.hop.projects.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.search.HopGuiDescribedVariableSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiMetadataSearchable;

// TODO: implement lazy loading of the searchables.
//
public class ProjectSearchablesIterator implements Iterator<ISearchable> {

  private ProjectConfig projectConfig;
  private List<ISearchable> searchables;
  private Iterator<ISearchable> iterator;

  /**
   * Searchables of one project. Configuration files come from the active environment only ({@code
   * HOP_ENVIRONMENT_NAME}). All-projects search passes {@code searchAllEnvironments} so every
   * environment of that project is included.
   */
  public ProjectSearchablesIterator(
      IHopMetadataProvider metadataProvider, IVariables variables, ProjectConfig projectConfig)
      throws HopException {
    this(metadataProvider, variables, projectConfig, false);
  }

  public ProjectSearchablesIterator(
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      ProjectConfig projectConfig,
      boolean searchAllEnvironments)
      throws HopException {
    this.projectConfig = projectConfig;
    this.searchables = new ArrayList<>();

    try {
      List<String> configurationFiles =
          environmentConfigurationFiles(
              projectConfig.getProjectName(), variables, searchAllEnvironments);

      // Discover files via registered hop file types that opt into search.
      //
      HopFileTypeRegistry fileTypeRegistry = HopFileTypeRegistry.getInstance();
      fileTypeRegistry.ensureLoaded();

      Set<String> searchableExtensions = new HashSet<>();
      boolean hasSearchableTypes = false;
      for (IHopFileType fileType : fileTypeRegistry.getFileTypes()) {
        if (!fileType.hasCapability(IHopFileType.CAPABILITY_SEARCH)) {
          continue;
        }
        hasSearchableTypes = true;
        for (String filterExtension : fileType.getFilterExtensions()) {
          if (filterExtension == null || filterExtension.isEmpty()) {
            continue;
          }
          // filter extensions look like "*.hpl" or "*.sql;*.SQL"
          for (String part : filterExtension.split(";")) {
            String ext = part.trim();
            if (ext.startsWith("*.")) {
              ext = ext.substring(2);
            } else if (ext.startsWith(".")) {
              ext = ext.substring(1);
            }
            if (!ext.isEmpty() && !"*".equals(ext)) {
              searchableExtensions.add(ext.toLowerCase(Locale.ROOT));
            }
          }
        }
      }

      FileObject homeFolderFile = HopVfs.getFileObject(projectConfig.getProjectHome());
      if (hasSearchableTypes) {
        Collection<FileObject> projectFiles = HopVfs.findFiles(homeFolderFile, null, true);
        for (FileObject projectFile : projectFiles) {
          String extension = projectFile.getName().getExtension();
          if (extension == null
              || extension.isEmpty()
              || !searchableExtensions.contains(extension.toLowerCase(Locale.ROOT))) {
            continue;
          }
          String filePath = projectFile.getName().getURI();
          try {
            IHopFileType fileType = fileTypeRegistry.findHopFileType(filePath);
            if (fileType == null || !fileType.hasCapability(IHopFileType.CAPABILITY_SEARCH)) {
              continue;
            }
            ISearchable searchable =
                fileType.createSearchable(
                    filePath,
                    "Project " + projectConfig.getProjectName(),
                    variables,
                    metadataProvider);
            if (searchable != null) {
              searchables.add(
                  ProjectScopedSearchable.wrap(searchable, projectConfig.getProjectName()));
            }
          } catch (Exception e) {
            LogChannel.GENERAL.logError("Error loading searchable file: " + filePath, e);
          }
        }
      }

      // Add the available metadata objects
      //
      for (Class<IHopMetadata> metadataClass : metadataProvider.getMetadataClasses()) {
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        for (final String metadataName : serializer.listObjectNames()) {
          IHopMetadata hopMetadata = serializer.load(metadataName);
          HopGuiMetadataSearchable searchable =
              new HopGuiMetadataSearchable(
                  metadataProvider, serializer, hopMetadata, serializer.getManagedClass());
          searchables.add(ProjectScopedSearchable.wrap(searchable, projectConfig.getProjectName()));
        }
      }

      // the described variables in HopConfig...
      //
      List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
      for (DescribedVariable describedVariable : describedVariables) {
        searchables.add(new HopGuiDescribedVariableSearchable(describedVariable, null));
      }

      // Now the described variables in the configuration files...
      //
      for (String configurationFile : configurationFiles) {
        String realConfigurationFile = variables.resolve(configurationFile);

        if (HopVfs.fileExists(realConfigurationFile)) {
          DescribedVariablesConfigFile configFile =
              new DescribedVariablesConfigFile(realConfigurationFile);
          configFile.readFromFile();
          for (DescribedVariable describedVariable : configFile.getDescribedVariables()) {
            // The resolved path, not the raw ${PROJECT_HOME}/... value. The click handler resolves
            // again with whichever project is active, and the searchable key uses this filename.
            searchables.add(
                ProjectScopedSearchable.wrap(
                    new HopGuiDescribedVariableSearchable(describedVariable, realConfigurationFile),
                    projectConfig.getProjectName()));
          }
        }
      }

      iterator = searchables.iterator();
    } catch (Exception e) {
      throw new HopException(
          "Error loading list of project '" + projectConfig.getProjectName() + "' searchables", e);
    }
  }

  /**
   * Configuration files to search. All-projects search includes every environment of the project.
   * The active project and hop-search include only the active environment ({@code
   * HOP_ENVIRONMENT_NAME}), and only when that environment belongs to this project. Duplicate paths
   * are skipped. Order follows the environment list.
   */
  static List<String> environmentConfigurationFiles(
      String projectName, IVariables variables, boolean allEnvironments) {
    List<String> configurationFiles = new ArrayList<>();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    if (config == null || projectName == null) {
      return configurationFiles;
    }
    List<LifecycleEnvironment> environments;
    if (allEnvironments) {
      environments = config.findEnvironmentsOfProject(projectName);
    } else {
      environments = new ArrayList<>();
      String environmentName = activeEnvironmentName(variables);
      if (environmentName != null) {
        LifecycleEnvironment environment = config.findEnvironment(environmentName);
        if (environment != null
            && projectName.equalsIgnoreCase(
                StringUtils.defaultString(environment.getProjectName()))) {
          environments.add(environment);
        }
      }
    }
    for (LifecycleEnvironment environment : environments) {
      addConfigurationFiles(configurationFiles, environment);
    }
    return configurationFiles;
  }

  private static String activeEnvironmentName(IVariables variables) {
    if (variables == null) {
      return null;
    }
    String environmentName = variables.getVariable(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME);
    return StringUtils.isEmpty(environmentName) ? null : environmentName;
  }

  private static void addConfigurationFiles(
      List<String> configurationFiles, LifecycleEnvironment environment) {
    if (environment == null || environment.getConfigurationFiles() == null) {
      return;
    }
    for (String configurationFile : environment.getConfigurationFiles()) {
      if (configurationFile == null
          || configurationFile.isEmpty()
          || configurationFiles.contains(configurationFile)) {
        continue;
      }
      configurationFiles.add(configurationFile);
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ISearchable next() {
    return iterator.next();
  }
}
