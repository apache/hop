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

package org.apache.hop.projects.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.run.HopRun;

import java.util.ArrayList;
import java.util.List;

@ExtensionPoint( id = "HopRunStartExtensionPoint",
  extensionPointId = "HopRunStart",
  description = "Enables a project or an environment at the start of the hop execution"
)
public class HopRunStartExtensionPoint implements IExtensionPoint<HopRun> {

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, HopRun hopRun ) throws HopException {

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = null;
    List<String> configurationFiles = new ArrayList<>();
    String projectName = null;
    String environmentName = null;

    // You can specify the project using -p (project) or -e (lifecycle environment)
    // The main difference is that the environment provides extra configuration files to consider.
    //
    if ( StringUtils.isNotEmpty( hopRun.getEnvironment() ) ) {
      // The environment contains extra configuration options we need to pass along...
      //
      LifecycleEnvironment environment = config.findEnvironment( hopRun.getEnvironment() );
      if ( environment == null ) {
        throw new HopException( "Unable to find lifecycle environment '" + hopRun.getEnvironment() + "'" );
      }
      projectName = environment.getProjectName();
      environmentName = environment.getName();

      if ( StringUtils.isEmpty( projectName ) ) {
        throw new HopException( "Lifecycle environment '" + hopRun.getEnvironment() + "' is not referencing a project." );
      }
      projectConfig = config.findProjectConfig( projectName );
      if ( projectConfig == null ) {
        throw new HopException( "Unable to find project '" + hopRun.getProject() + "'" );
      }
      configurationFiles.addAll( environment.getConfigurationFiles() );

      log.logBasic( "Referencing environment '" + hopRun.getEnvironment()+"' for project " + projectName + "' in "+environment.getPurpose() );
    } else if ( StringUtils.isNotEmpty( hopRun.getProject() ) ) {
      // Simply reference the project directly without extra configuration files...
      //
      projectConfig = config.findProjectConfig( hopRun.getProject() );
      if ( projectConfig == null ) {
        throw new HopException( "Unable to find project '" + hopRun.getProject() + "'" );
      }
      projectName = projectConfig.getProjectName();
    } else {
      // If there is no project specified on the HopRun command line, we stop here.
      //
      log.logDebug( "No project referenced using the --environment or --project options during hop-run execution" );
      return;
    }

    try {
      Project project = projectConfig.loadProject( hopRun.getVariables() );
      log.logBasic( "Enabling project '" + projectName + "'" );

      if ( project == null ) {
        throw new HopException( "Project '" + projectName + "' couldn't be found" );
      }
      // Now we just enable this project
      //
      ProjectsUtil.enableProject( log, projectName, project, hopRun.getVariables(), configurationFiles, environmentName, hopRun );

    } catch ( Exception e ) {
      throw new HopException( "Error enabling project '" + projectName + "'", e );
    }

  }
}
