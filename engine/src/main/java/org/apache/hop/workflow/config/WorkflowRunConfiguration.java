package org.apache.hop.workflow.config;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.util.HopDefaults;

@MetaStoreElementType(
  name = "Workflow Run Configuration",
  description = "Describes how to execute a workflow"
)
public class WorkflowRunConfiguration extends Variables implements Cloneable, IVariables, IHopMetaStoreElement<WorkflowRunConfiguration> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "WorkflowRunConfiguration-PluginSpecific-Options";

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
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

  @Override public MetaStoreFactory<WorkflowRunConfiguration> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<WorkflowRunConfiguration> createFactory( IMetaStore metaStore ) {
    MetaStoreFactory<WorkflowRunConfiguration> factory = new MetaStoreFactory<>( WorkflowRunConfiguration.class, metaStore, HopDefaults.NAMESPACE );
    factory.setObjectFactory( new WorkflowRunConfigurationMetaStoreObjectFactory() );
    return factory;
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
