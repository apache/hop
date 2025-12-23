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

package org.apache.hop.projects.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.lifecycle.ProjectLifecycle;
import org.apache.hop.projects.project.ProjectConfig;

@Getter
@Setter
@JsonIgnoreProperties(value = {"openingLastProjectAtStartup"})
public class ProjectsConfig {

  public static final String HOP_CONFIG_PROJECTS_CONFIG_KEY = "projectsConfig";
  public static final String DEFAULT_PROJECT_CONFIG_FILENAME = "project-config.json";

  private boolean enabled;

  private boolean projectMandatory;
  private boolean environmentMandatory;
  private boolean environmentsForActiveProject;
  private boolean sortByNameLastUsedProjects;
  private boolean clearingDbCacheWhenSwitching;
  private String defaultProject;
  private String defaultEnvironment;
  private String standardParentProject;
  private String standardProjectsFolder;
  private String defaultProjectConfigFile;

  private List<ProjectConfig> projectConfigurations;
  private List<LifecycleEnvironment> lifecycleEnvironments;
  private List<ProjectLifecycle> projectLifecycles;

  public ProjectsConfig() {
    enabled = true;
    defaultProjectConfigFile = DEFAULT_PROJECT_CONFIG_FILENAME;
    projectConfigurations = new ArrayList<>();
    lifecycleEnvironments = new ArrayList<>();
    projectLifecycles = new ArrayList<>();
    clearingDbCacheWhenSwitching = true;
  }

  public ProjectsConfig(ProjectsConfig config) {
    this();
    enabled = config.enabled;
    projectConfigurations = new ArrayList<>(config.projectConfigurations);
    lifecycleEnvironments = new ArrayList<>(config.lifecycleEnvironments);
    projectLifecycles = new ArrayList<>(config.projectLifecycles);
    projectMandatory = config.projectMandatory;
    environmentMandatory = config.environmentMandatory;
    defaultProject = config.defaultProject;
    defaultEnvironment = config.defaultEnvironment;
    standardParentProject = config.standardParentProject;
    standardProjectsFolder = config.standardProjectsFolder;
    defaultProjectConfigFile = config.defaultProjectConfigFile;
    environmentsForActiveProject = config.environmentsForActiveProject;
    clearingDbCacheWhenSwitching = config.clearingDbCacheWhenSwitching;
    sortByNameLastUsedProjects = config.sortByNameLastUsedProjects;
  }

  public ProjectConfig findProjectConfig(String projectName) {
    if (StringUtils.isEmpty(projectName)) {
      return null;
    }
    for (ProjectConfig projectConfig : projectConfigurations) {
      if (projectConfig.getProjectName().equalsIgnoreCase(projectName)) {
        return projectConfig;
      }
    }
    return null;
  }

  /**
   * Find the environments for a given project
   *
   * @param projectName The name of the environment to look up
   * @return The environments for the project
   */
  public List<LifecycleEnvironment> findEnvironmentsOfProject(String projectName) {
    List<LifecycleEnvironment> list = new ArrayList<>();
    lifecycleEnvironments.forEach(
        e -> {
          if (e.getProjectName().equals(projectName)) {
            list.add(e);
          }
        });
    return list;
  }

  public void addProjectConfig(ProjectConfig projectConfig) {
    ProjectConfig existing = findProjectConfig(projectConfig.getProjectName());
    if (existing == null) {
      projectConfigurations.add(projectConfig);
    } else {
      existing.setProjectName(projectConfig.getProjectName());
      existing.setProjectHome(projectConfig.getProjectHome());
      existing.setConfigFilename(projectConfig.getConfigFilename());
    }
  }

  public int indexOfProjectConfig(String projectName) {
    return projectConfigurations.indexOf(
        new ProjectConfig(projectName, null, null)); // Only considers the name
  }

  public ProjectConfig removeProjectConfig(String projectName) {
    int index = indexOfProjectConfig(projectName);
    if (index >= 0) {
      return projectConfigurations.remove(index);
    } else {
      return null;
    }
  }

  public List<String> listProjectConfigNames() {
    List<String> names = new ArrayList<>();
    projectConfigurations.forEach(config -> names.add(config.getProjectName()));
    Collections.sort(names);
    return names;
  }

  public LifecycleEnvironment findEnvironment(String environmentName) {
    if (StringUtils.isEmpty(environmentName)) {
      return null;
    }
    for (LifecycleEnvironment environment : lifecycleEnvironments) {
      if (environment.getName().equals(environmentName)) {
        return environment;
      }
    }
    return null;
  }

  public void addEnvironment(LifecycleEnvironment environment) {
    int index = lifecycleEnvironments.indexOf(environment);
    if (index < 0) {
      lifecycleEnvironments.add(environment);
    } else {
      lifecycleEnvironments.set(index, environment);
    }
  }

  public LifecycleEnvironment removeEnvironment(String environmentName) {
    LifecycleEnvironment environment = findEnvironment(environmentName);
    if (environment != null) {
      lifecycleEnvironments.remove(environment);
    }
    return environment;
  }

  public List<String> listEnvironmentNames() {
    List<String> names = new ArrayList<>();
    lifecycleEnvironments.stream().forEach(env -> names.add(env.getName()));
    Collections.sort(names);
    return names;
  }

  public List<String> listEnvironmentNamesForProject(String projectName) {
    List<String> names = new ArrayList<>();
    lifecycleEnvironments.forEach(
        env -> {
          if (env.getProjectName().equals(projectName)) {
            names.add(env.getName());
          }
        });

    Collections.sort(names);
    return names;
  }

  public int indexOfEnvironment(String environmentName) {
    return lifecycleEnvironments.indexOf(
        new LifecycleEnvironment(
            environmentName, null, null, Collections.emptyList())); // Only considers the name
  }

  public ProjectLifecycle findLifecycle(String lifecycleName) {
    if (StringUtils.isEmpty(lifecycleName)) {
      return null;
    }
    for (ProjectLifecycle lifecycle : projectLifecycles) {
      if (lifecycle.getName().equalsIgnoreCase(lifecycleName)) {
        return lifecycle;
      }
    }
    return null;
  }

  public void addLifecycle(ProjectLifecycle lifecycle) {
    int index = projectLifecycles.indexOf(lifecycle);
    if (index < 0) {
      projectLifecycles.add(lifecycle);
    } else {
      projectLifecycles.set(index, lifecycle);
    }
  }

  public ProjectLifecycle removeLifecycle(String lifecycleName) {
    ProjectLifecycle lifecycle = findLifecycle(lifecycleName);
    if (lifecycle != null) {
      projectLifecycles.remove(lifecycle);
    }
    return lifecycle;
  }

  public List<String> listLifecycleNames() {
    List<String> names = new ArrayList<>();
    projectLifecycles.forEach(lifecycle -> names.add(lifecycle.getName()));
    Collections.sort(names);
    return names;
  }

  public int indexOfLifecycle(String lifecycleName) {
    return projectLifecycles.indexOf(
        new ProjectLifecycle(
            lifecycleName,
            Collections.emptyList(),
            Collections.emptyList())); // Only considers the name
  }
}
