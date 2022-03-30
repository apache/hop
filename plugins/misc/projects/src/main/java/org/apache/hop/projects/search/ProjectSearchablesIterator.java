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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.search.HopGuiDescribedVariableSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiMetadataSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiPipelineSearchable;
import org.apache.hop.ui.hopgui.search.HopGuiWorkflowSearchable;
import org.apache.hop.workflow.WorkflowMeta;

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

  public ProjectSearchablesIterator(
      IHopMetadataProvider metadataProvider, IVariables variables, ProjectConfig projectConfig)
      throws HopException {
    this.projectConfig = projectConfig;
    this.searchables = new ArrayList<>();

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    try {
      List<String> configurationFiles = new ArrayList<>();
      List<LifecycleEnvironment> environments =
          config.findEnvironmentsOfProject(projectConfig.getProjectName());
      if (!environments.isEmpty()) {
        configurationFiles.addAll(environments.get(0).getConfigurationFiles());
      }

      // Find all the pipelines and workflows in the project homefolder...
      //
      FileObject homeFolderFile = HopVfs.getFileObject(projectConfig.getProjectHome());
      Collection<FileObject> pipelineFiles = HopVfs.findFiles(homeFolderFile, "hpl", true);
      for (FileObject pipelineFile : pipelineFiles) {
        String pipelineFilePath = pipelineFile.getName().getURI();
        try {
          PipelineMeta pipelineMeta =
              new PipelineMeta(pipelineFilePath, metadataProvider, true, variables);
          searchables.add(new HopGuiPipelineSearchable("Project pipeline file", pipelineMeta));
        } catch (Exception e) {
          // There was an error loading the XML file...
          LogChannel.GENERAL.logError("Error loading pipeline metadata: " + pipelineFilePath, e);
        }
      }

      Collection<FileObject> workflowFiles = HopVfs.findFiles(homeFolderFile, "hwf", true);
      for (FileObject workflowFile : workflowFiles) {
        String workflowFilePath = workflowFile.getName().getURI();
        try {
          WorkflowMeta workflowMeta =
              new WorkflowMeta(variables, workflowFilePath, metadataProvider);
          searchables.add(new HopGuiWorkflowSearchable("Project workflow file", workflowMeta));
        } catch (Exception e) {
          // There was an error loading the XML file...
          LogChannel.GENERAL.logError("Error loading workflow metadata: " + workflowFilePath, e);
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
          searchables.add(searchable);
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
            searchables.add(
                new HopGuiDescribedVariableSearchable(describedVariable, configurationFile));
          }
        }
      }

      iterator = searchables.iterator();
    } catch (Exception e) {
      throw new HopException(
          "Error loading list of project '" + projectConfig.getProjectName() + "' searchables", e);
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
