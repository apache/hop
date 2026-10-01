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

package org.apache.hop.projects.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.search.HopGuiSearchHelper;

/**
 * Searchables from the projects named in the allow-list. A project that cannot be loaded is skipped
 * so the others are still searched. Nothing here enables a project: the GUI keeps its active
 * metadata and VFS providers.
 *
 * <p>The allow-list is the one captured on the UI thread. This iterator does not ask {@code
 * ProjectsAccessControl} again, because search runs on a background thread.
 */
public class AllProjectsSearchablesIterator implements Iterator<ISearchable> {

  private final List<ISearchable> searchables;
  private final Iterator<ISearchable> iterator;

  public AllProjectsSearchablesIterator(IVariables variables, List<String> allowedProjectNames) {
    this.searchables = new ArrayList<>();
    Set<String> seen = new HashSet<>();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    List<ProjectConfig> projects =
        config == null || config.getProjectConfigurations() == null
            ? List.of()
            : new ArrayList<>(config.getProjectConfigurations());
    for (ProjectConfig projectConfig : projects) {
      if (projectConfig == null || projectConfig.getProjectName() == null) {
        continue;
      }
      if (!isAllowed(allowedProjectNames, projectConfig.getProjectName())) {
        continue;
      }
      try {
        collectProject(variables, projectConfig, seen);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error loading searchables for project '" + projectConfig.getProjectName() + "'", e);
      }
    }
    this.iterator = searchables.iterator();
  }

  /** A null list allows every project. An empty list allows none. Comparison ignores case. */
  static boolean isAllowed(List<String> allowedProjectNames, String projectName) {
    if (allowedProjectNames == null) {
      return true;
    }
    for (String allowed : allowedProjectNames) {
      if (allowed != null && allowed.equalsIgnoreCase(projectName)) {
        return true;
      }
    }
    return false;
  }

  private void collectProject(IVariables variables, ProjectConfig projectConfig, Set<String> seen)
      throws HopException {
    IVariables projectVariables = new Variables();
    projectVariables.initializeFrom(variables);
    Project project = projectConfig.loadProject(projectVariables);
    // Project home and metadata folder only. Environment files are searched as variables, not
    // applied over each other (the last environment would otherwise hide the others).
    project.modifyVariables(projectVariables, projectConfig, new ArrayList<>(), null);
    IHopMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(projectVariables);
    Iterator<ISearchable> projectSearchables =
        new ProjectSearchablesIterator(metadataProvider, projectVariables, projectConfig, true);
    while (projectSearchables.hasNext()) {
      ISearchable searchable = projectSearchables.next();
      if (seen.add(HopGuiSearchHelper.searchableKey(searchable))) {
        searchables.add(searchable);
      }
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
