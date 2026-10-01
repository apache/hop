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
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.security.ProjectsAccessControl;

/**
 * Search location over every configured project the current user may open, and the configuration
 * files of each of those projects' environments.
 *
 * <p>The allow-list is fixed when the location is built. Search runs later on a background thread,
 * where a session-bound security context may no longer resolve, so {@link
 * ProjectsAccessControl#isProjectAllowed} must not be called again from there.
 */
public class AllProjectsSearchablesLocation implements ISearchablesLocation {

  public static final String LOCATION_ID = "all-projects";

  public static final String DESCRIPTION = "All projects";

  private final List<String> allowedProjectNames;

  /** Allow-list captured now. Call this on the UI thread, where the security context is bound. */
  public AllProjectsSearchablesLocation() {
    this(allowedProjectNames());
  }

  /**
   * @param allowedProjectNames project names captured on the UI thread; not null. An empty list
   *     searches no project.
   */
  public AllProjectsSearchablesLocation(List<String> allowedProjectNames) {
    this.allowedProjectNames =
        List.copyOf(allowedProjectNames == null ? allowedProjectNames() : allowedProjectNames);
  }

  /**
   * Project names the current session may search. Call this on the UI thread and pass the result
   * into {@link #AllProjectsSearchablesLocation(List)}.
   */
  public static List<String> allowedProjectNames() {
    List<String> allowed = new ArrayList<>();
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    if (config == null || config.getProjectConfigurations() == null) {
      return allowed;
    }
    for (ProjectConfig projectConfig : config.getProjectConfigurations()) {
      if (projectConfig == null || StringUtils.isEmpty(projectConfig.getProjectName())) {
        continue;
      }
      if (ProjectsAccessControl.isProjectAllowed(projectConfig.getProjectName())) {
        allowed.add(projectConfig.getProjectName());
      }
    }
    return allowed;
  }

  public List<String> getAllowedProjectNames() {
    return allowedProjectNames;
  }

  @Override
  public String getLocationDescription() {
    return DESCRIPTION;
  }

  @Override
  public String getLocationId() {
    return LOCATION_ID;
  }

  @Override
  public boolean isIncludedInDefaultSearch() {
    return false;
  }

  @Override
  public Iterator<ISearchable> getSearchables(
      IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    // metadataProvider belongs to the active project. Each allowed project is loaded on its own.
    return new AllProjectsSearchablesIterator(variables, allowedProjectNames);
  }
}
