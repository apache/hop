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

package org.apache.hop.projects.lifecycle;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.List;

/*

TODO: Re-enable when we have usage for it

@ConfigPlugin(
  id = "ManageLifecyclesOptionPlugin",
  description = "Allows command line editing of the lifecycles"
)
 */
public class ManageLifecyclesOptionPlugin implements IConfigOptions {

  @CommandLine.Option(
    names = { "-lc", "--lifecycle-create" },
    description = "Create a new project lifecycle. You need so specify a name and one or more lifecycle environments and configuration files."
  )
  private boolean createLifecycle;

  @CommandLine.Option(
    names = { "-l", "--lifecycle" },
    description = "The name of the lifecycle to manage"
  )
  private String lifecycleName;

  @CommandLine.Option(
    names = { "-le", "--lifecycle-environments" },
    description = "The names of the lifecycle environments, comma separated",
    split = ","
  )
  private String[] lifecycleEnvironments;

  @CommandLine.Option(
    names = { "-cf", "--config-files" },
    description = "The file names of the lifecycle configuration files, comma separated",
    split = ","
  )
  private String[] configFilenames;

  @CommandLine.Option( names = { "-lm", "--lifecycle-modify" }, description = "Modify a lifecycle" )
  private boolean modifyLifecycle;

  @CommandLine.Option( names = { "-ld", "--lifecycle-delete" }, description = "Delete a lifecycle" )
  private boolean deleteLifecycle;

  @CommandLine.Option( names = { "-ll", "-lifecycles-list" }, description = "List the defined lifecycles" )
  private boolean listLifecycles;


  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables ) throws HopException {
    try {
      boolean changed = false;
      if ( createLifecycle ) {
        createLifecycle( log );
        changed = true;
      } else if ( modifyLifecycle ) {
        modifyLifecycle( log );
        changed = true;
      } else if ( deleteLifecycle ) {
        deleteLifecycle( log );
        changed = true;
      } else if ( listLifecycles ) {
        listLifecycles( log );
        changed = true;
      }
      return changed;
    } catch ( Exception e ) {
      throw new HopException( "Error handling lifecycle configuration options", e );
    }

  }

  private void listLifecycles( ILogChannel log ) throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    log.logBasic( "Project lifecycles:" );
    List<String> names = config.listLifecycleNames();
    for ( String name : names ) {
      ProjectLifecycle lifecycle = config.findLifecycle( name );
      logLifecycleDetails(log, lifecycle);
    }
  }

  private void logLifecycleDetails( ILogChannel log, ProjectLifecycle lifecycle ) {
    log.logBasic( "  Name : " + lifecycle.getName() );
    log.logBasic( "    Project lifecycle environments:" );
    for (String environmentName : lifecycle.getLifecycleEnvironments()) {
      log.logBasic( "      " +environmentName );
    }
    log.logBasic( "    Configuration files:" );
    for (String configurationFile : lifecycle.getConfigurationFiles()) {
      log.logBasic( "      " +configurationFile );
    }
  }

  private void deleteLifecycle( ILogChannel log  ) throws Exception {
    validateLifecycleNameSpecified();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    validateLifecycleNameSpecified();
    ProjectLifecycle lifecycle = config.findLifecycle( lifecycleName );
    if (lifecycle==null ) {
      throw new HopException( "Project lifecycle '" + lifecycleName + "' doesn't exists, it can't be deleted" );
    }

    config.removeLifecycle( lifecycleName );

    HopConfig.getInstance().saveToFile();
    log.logBasic( "Project lifecycle '" + lifecycleName + "' was delete." );
  }

  private void modifyLifecycle( ILogChannel log ) throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    validateLifecycleNameSpecified();
    ProjectLifecycle lifecycle = config.findLifecycle( lifecycleName );
    if (lifecycle==null ) {
      throw new HopException( "Project lifecycle '" + lifecycleName + "' doesn't exists, it can't be modified" );
    }

    if (lifecycleEnvironments!=null) {
      lifecycle.getLifecycleEnvironments().clear();
      lifecycle.getLifecycleEnvironments().addAll( Arrays.asList(lifecycleEnvironments) );
    }
    if (configFilenames!=null) {
      lifecycle.getConfigurationFiles().clear();
      lifecycle.getConfigurationFiles().addAll( Arrays.asList(configFilenames) );
    }

    config.addLifecycle( lifecycle );
    HopConfig.getInstance().saveToFile();
    log.logBasic( "Project lifecycle '" + lifecycleName + "' was modified." );
    log.logBasic( "Details after changes:" );
    logLifecycleDetails( log, lifecycle );
  }


  private void createLifecycle( ILogChannel log ) throws Exception {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    validateLifecycleNameSpecified();

    log.logBasic( "Creating project lifecycle '" + lifecycleName + "'" );
    ProjectLifecycle lifecycle = config.findLifecycle( lifecycleName );
    if ( lifecycle!=null ) {
      throw new HopException( "Project lifecycle '" + lifecycleName + "' already exists." );
    }

    lifecycle = new ProjectLifecycle();
    lifecycle.setName( lifecycleName );
    if (lifecycleEnvironments!=null) {
      lifecycle.getLifecycleEnvironments().addAll( Arrays.asList( lifecycleEnvironments ) );
    }
    if (configFilenames!=null) {
      lifecycle.getConfigurationFiles().addAll( Arrays.asList(configFilenames) );
    }

    config.addLifecycle( lifecycle );
    HopConfig.getInstance().saveToFile();
    log.logBasic( "Project lifecycle '" + lifecycleName + "' was created." );
    log.logBasic( "Details after creation:" );
    logLifecycleDetails( log, lifecycle );
  }

  private void validateLifecycleNameSpecified() throws Exception {
    if ( StringUtil.isEmpty( lifecycleName ) ) {
      throw new HopException( "Please specify the name of the lifecycle" );
    }
  }
}

