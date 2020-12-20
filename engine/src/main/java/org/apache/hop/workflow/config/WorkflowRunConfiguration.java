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


package org.apache.hop.workflow.config;

import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
  key = "workflow-run-configuration",
  name = "Workflow Run Configuration",
  description = "Describes how to execute a workflow",
  image = "ui/images/workflow_run_config.svg"
)
public class WorkflowRunConfiguration extends HopMetadataBase implements Cloneable, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "WorkflowRunConfiguration-PluginSpecific-Options";

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty
  private IWorkflowEngineRunConfiguration engineRunConfiguration;

  public WorkflowRunConfiguration() {
  }

  public WorkflowRunConfiguration( String name, String description, IWorkflowEngineRunConfiguration engineRunConfiguration ) {
    this();
    this.name = name;
    this.description = description;
    this.engineRunConfiguration = engineRunConfiguration;
  }

  public WorkflowRunConfiguration(WorkflowRunConfiguration c) {
    this.name = c.name;
    this.description = c.description;
    if (c.engineRunConfiguration!=null) {
      this.engineRunConfiguration = c.engineRunConfiguration.clone();
    }
  }

  @Override protected WorkflowRunConfiguration clone() {
    return new WorkflowRunConfiguration( this );
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets engineRunConfiguration
   *
   * @return value of engineRunConfiguration
   */
  public IWorkflowEngineRunConfiguration getEngineRunConfiguration() {
    return engineRunConfiguration;
  }

  /**
   * @param engineRunConfiguration The engineRunConfiguration to set
   */
  public void setEngineRunConfiguration( IWorkflowEngineRunConfiguration engineRunConfiguration ) {
    this.engineRunConfiguration = engineRunConfiguration;
  }
}
