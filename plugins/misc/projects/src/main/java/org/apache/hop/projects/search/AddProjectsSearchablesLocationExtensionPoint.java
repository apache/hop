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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;

import java.util.List;

@ExtensionPoint(
  id = "AddProjectsSearchablesLocationExtensionPoint",
  description = "Adds a search location to the search perspective",
  extensionPointId = "HopGuiGetSearchablesLocations"
)
public class AddProjectsSearchablesLocationExtensionPoint implements IExtensionPoint<List<ISearchablesLocation>> {
  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, List<ISearchablesLocation> searchablesLocations ) throws HopException {

    // The location to add is the currently active project and the files in the home folder
    //
    String projectName = HopNamespace.getNamespace();
    if (projectName==null) {
      return;
    }
    ProjectConfig projectConfig = ProjectsConfigSingleton.getConfig().findProjectConfig( projectName );
    if (projectConfig==null) {
      return;
    }
    try {
      ProjectsSearchablesLocation projectsSearchablesLocation = new ProjectsSearchablesLocation( projectConfig );
      searchablesLocations.add( projectsSearchablesLocation );
    } catch(Exception e) {
      log.logError( "Error getting project searchables", e );
    }
  }
}
