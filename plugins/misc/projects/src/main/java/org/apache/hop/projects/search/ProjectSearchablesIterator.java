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

import org.apache.commons.io.FileUtils;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.search.HopGuiDescribedVariableSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiMetadataSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiPipelineSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiWorkflowSearchable;
import org.apache.hop.workflow.WorkflowMeta;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

// TODO: implement lazy loading of the searchables.
//
public class ProjectSearchablesIterator implements Iterator<ISearchable> {

  private ProjectConfig projectConfig;
  private List<ISearchable> searchables;
  private Iterator<ISearchable> iterator;

  public ProjectSearchablesIterator( ProjectConfig projectConfig ) throws HopException {
    this.projectConfig = projectConfig;
    this.searchables = new ArrayList<>();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    HopGui hopGui = HopGui.getInstance();

    try {
      List<String> configurationFiles = new ArrayList<>();
      List<LifecycleEnvironment> environments = config.findEnvironmentsOfProject( projectConfig.getProjectName() );
      if (!environments.isEmpty()) {
        configurationFiles.addAll(environments.get( 0 ).getConfigurationFiles());
      }

      // Find all the pipelines and workflows in the project homefolder...
      //
      File homeFolderFile = new File(projectConfig.getProjectHome());
      Collection<File> pipelineFiles = FileUtils.listFiles( homeFolderFile, new String[] { "hpl" }, true );
      for (File pipelineFile : pipelineFiles) {
        PipelineMeta pipelineMeta = new PipelineMeta(pipelineFile.getPath(), hopGui.getMetadataProvider(), true, hopGui.getVariables());
        searchables.add(new HopGuiPipelineSearchable( "Project pipeline file", pipelineMeta ) );
      }

      Collection<File> workflowFiles = FileUtils.listFiles( homeFolderFile, new String[] { "hwf" }, true );
      for (File workflowFile : workflowFiles) {
        WorkflowMeta workflowMeta = new WorkflowMeta(hopGui.getVariables(), workflowFile.getPath(), hopGui.getMetadataProvider() );
        searchables.add(new HopGuiWorkflowSearchable( "Project workflow file", workflowMeta ) );
      }

      // Add the available metadata objects
      //
      for ( Class<IHopMetadata> metadataClass : hopGui.getMetadataProvider().getMetadataClasses() ) {
        IHopMetadataSerializer<IHopMetadata> serializer = hopGui.getMetadataProvider().getSerializer( metadataClass );
        for ( final String metadataName : serializer.listObjectNames() ) {
          IHopMetadata hopMetadata = serializer.load( metadataName );
          HopGuiMetadataSearchable searchable = new HopGuiMetadataSearchable( hopGui.getMetadataProvider(), serializer, hopMetadata, serializer.getManagedClass() );
          searchables.add( searchable );
        }
      }

      // the described variables in HopConfig...
      //
      List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
      for ( DescribedVariable describedVariable : describedVariables ) {
        searchables.add( new HopGuiDescribedVariableSearchable( describedVariable, null ) );
      }

      // Now the described variables in the configuration files...
      //
      for (String configurationFile : configurationFiles) {
        String realConfigurationFile = hopGui.getVariables().resolve( configurationFile );

        if (new File(realConfigurationFile).exists()) {
          DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile( realConfigurationFile );
          configFile.readFromFile();
          for ( DescribedVariable describedVariable : configFile.getDescribedVariables() ) {
            searchables.add( new HopGuiDescribedVariableSearchable( describedVariable, configurationFile ) );
          }
        }
      }

      iterator = searchables.iterator();
    } catch ( Exception e ) {
      throw new HopException( "Error loading list of project '" + projectConfig.getProjectName() + "' searchables", e );
    }
  }

  @Override public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override public ISearchable next() {
    return iterator.next();
  }
}
