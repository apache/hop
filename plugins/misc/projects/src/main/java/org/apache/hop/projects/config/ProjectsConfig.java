/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.projects.config;

import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.lifecycle.ProjectLifecycle;
import org.apache.hop.projects.project.ProjectConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProjectsConfig {

  public static final String HOP_CONFIG_PROJECTS_CONFIG_KEY = "projectsConfig";

  private boolean enabled;

  private boolean openingLastProjectAtStartup;

  private List<ProjectConfig> projectConfigurations;
  private List<LifecycleEnvironment> lifecycleEnvironments;
  private List<ProjectLifecycle> projectLifecycles;

  public ProjectsConfig() {
    enabled = true;
    openingLastProjectAtStartup = true;
    projectConfigurations = new ArrayList<>();
    lifecycleEnvironments = new ArrayList<>();
    projectLifecycles = new ArrayList<>();
  }

  public ProjectsConfig( ProjectsConfig config ) {
    this();
    enabled = config.enabled;
    openingLastProjectAtStartup = config.openingLastProjectAtStartup;
    projectConfigurations = new ArrayList<>( config.projectConfigurations );
    lifecycleEnvironments = new ArrayList<>( config.lifecycleEnvironments );
    projectLifecycles = new ArrayList<>( config.projectLifecycles );
  }

  public ProjectConfig findProjectConfig( String projectName ) {
    for ( ProjectConfig projectConfig : projectConfigurations ) {
      if ( projectConfig.getProjectName().equalsIgnoreCase( projectName ) ) {
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
  public List<LifecycleEnvironment> findEnvironmentsOfProject( String projectName ) {
    List<LifecycleEnvironment> list = new ArrayList<>();
    lifecycleEnvironments.stream().forEach( e -> { if (e.getProjectName().equals( projectName )) { list.add(e); } } );
    return list;
  }

  public void addProjectConfig( ProjectConfig projectConfig ) {
    ProjectConfig existing = findProjectConfig( projectConfig.getProjectName() );
    if ( existing == null ) {
      projectConfigurations.add( projectConfig );
    } else {
      existing.setProjectName( projectConfig.getProjectName() );
      existing.setProjectHome( projectConfig.getProjectHome() );
      existing.setConfigFilename( projectConfig.getConfigFilename() );
    }
  }

  public int indexOfProjectConfig( String projectName ) {
    return projectConfigurations.indexOf( new ProjectConfig( projectName, null, null ) ); // Only considers the name
  }

  public ProjectConfig removeProjectConfig( String projectName ) {
    int index = indexOfProjectConfig( projectName );
    if ( index >= 0 ) {
      return projectConfigurations.remove( index );
    } else {
      return null;
    }
  }

  public List<String> listProjectConfigNames() {
    List<String> names = new ArrayList<>();
    projectConfigurations.stream().forEach( config -> names.add( config.getProjectName() ) );
    Collections.sort( names );
    return names;
  }


  public LifecycleEnvironment findEnvironment( String environmentName ) {
    for ( LifecycleEnvironment environment : lifecycleEnvironments ) {
      if ( environment.getName().equals( environmentName ) ) {
        return environment;
      }
    }
    return null;
  }

  public void addEnvironment( LifecycleEnvironment environment ) {
    int index = lifecycleEnvironments.indexOf( environment );
    if ( index < 0 ) {
      lifecycleEnvironments.add( environment );
    } else {
      lifecycleEnvironments.set( index, environment );
    }
  }

  public LifecycleEnvironment removeEnvironment( String environmentName ) {
    LifecycleEnvironment environment = findEnvironment( environmentName );
    if ( environment != null ) {
      lifecycleEnvironments.remove( environment );
    }
    return environment;
  }

  public List<String> listEnvironmentNames() {
    List<String> names = new ArrayList<>();
    lifecycleEnvironments.stream().forEach( env -> names.add( env.getName() ) );
    Collections.sort( names );
    return names;
  }

  public int indexOfEnvironment( String environmentName ) {
    return lifecycleEnvironments.indexOf( new LifecycleEnvironment( environmentName, null, null, Collections.emptyList() ) ); // Only considers the name
  }


  public ProjectLifecycle findLifecycle( String lifecycleName ) {
    for ( ProjectLifecycle lifecycle : projectLifecycles ) {
      if ( lifecycle.equals( lifecycleName ) ) {
        return lifecycle;
      }
    }
    return null;
  }

  public void addLifecycle( ProjectLifecycle lifecycle ) {
    int index = projectLifecycles.indexOf( lifecycle );
    if ( index < 0 ) {
      projectLifecycles.add( lifecycle );
    } else {
      projectLifecycles.set( index, lifecycle );
    }
  }

  public ProjectLifecycle removeLifecycle( String lifecycleName ) {
    ProjectLifecycle lifecycle = findLifecycle( lifecycleName );
    if ( lifecycle != null ) {
      lifecycleEnvironments.remove( lifecycle );
    }
    return lifecycle;
  }

  public List<String> listLifecycleNames() {
    List<String> names = new ArrayList<>();
    projectLifecycles.stream().forEach( lifecycle -> names.add( lifecycle.getName() ) );
    Collections.sort( names );
    return names;
  }

  public int indexOfLifecycle( String lifecycleName ) {
    return projectLifecycles.indexOf( new ProjectLifecycle( lifecycleName, Collections.emptyList(), Collections.emptyList() ) ); // Only considers the name
  }


  /**
   * Gets enabled
   *
   * @return value of enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled The enabled to set
   */
  public void setEnabled( boolean enabled ) {
    this.enabled = enabled;
  }

  /**
   * Gets openingLastProjectAtStartup
   *
   * @return value of openingLastProjectAtStartup
   */
  public boolean isOpeningLastProjectAtStartup() {
    return openingLastProjectAtStartup;
  }

  /**
   * @param openingLastProjectAtStartup The openingLastProjectAtStartup to set
   */
  public void setOpeningLastProjectAtStartup( boolean openingLastProjectAtStartup ) {
    this.openingLastProjectAtStartup = openingLastProjectAtStartup;
  }

  /**
   * Gets projectConfigurations
   *
   * @return value of projectConfigurations
   */
  public List<ProjectConfig> getProjectConfigurations() {
    return projectConfigurations;
  }

  /**
   * @param projectConfigurations The projectConfigurations to set
   */
  public void setProjectConfigurations( List<ProjectConfig> projectConfigurations ) {
    this.projectConfigurations = projectConfigurations;
  }

  /**
   * Gets lifecycleEnvironments
   *
   * @return value of lifecycleEnvironments
   */
  public List<LifecycleEnvironment> getLifecycleEnvironments() {
    return lifecycleEnvironments;
  }

  /**
   * @param lifecycleEnvironments The lifecycleEnvironments to set
   */
  public void setLifecycleEnvironments( List<LifecycleEnvironment> lifecycleEnvironments ) {
    this.lifecycleEnvironments = lifecycleEnvironments;
  }
}
