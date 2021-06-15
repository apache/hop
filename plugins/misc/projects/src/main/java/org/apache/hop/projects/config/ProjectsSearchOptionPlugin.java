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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.search.ProjectsSearchablesLocation;
import org.apache.hop.search.HopSearch;
import org.apache.hop.ui.core.gui.HopNamespace;

@ConfigPlugin(
    id = "ProjectsSearchOptionPlugin",
    description = "Project and Environment configuration options for hop-search",
    category = ConfigPlugin.CATEGORY_SEARCH)
public class ProjectsSearchOptionPlugin extends ProjectsOptionPlugin implements IConfigOptions {

  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables ) throws HopException {

    // If a project or environment was set, pass it to HopSearch...
    //
    if (super.handleOption( log, hasHopMetadataProvider, variables )) {

      String projectName = HopNamespace.getNamespace();
      if ( StringUtils.isNotEmpty(projectName)) {
        log.logBasic( "Searching in project : "+projectName );

        ProjectsSearchablesLocation projectsSearchablesLocation = new ProjectsSearchablesLocation( projectConfig );
        ((HopSearch)hasHopMetadataProvider).getSearchablesLocations().add(projectsSearchablesLocation);

        return true;
      }
    }
    return false;
  }
}
