package org.apache.hop.workflow.config;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
  key = "workflow-run-configuration",
  name = "Workflow Run Configuration",
  description = "Describes how to execute a workflow",
  iconImage = "ui/images/run.svg"
)
public class WorkflowRunConfiguration extends Variables implements Cloneable, IVariables, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "WorkflowRunConfiguration-PluginSpecific-Options";

  @HopMetadataProperty
  private String name;

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty
  private IWorkflowEngineRunConfiguration engineRunConfiguration;

  private IVariables variables = new Variables();

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
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
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
