package org.apache.hop.pipeline.config;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.util.HopDefaults;

import java.util.Map;
import java.util.Objects;

@MetaStoreElementType(
  name = "Pipeline Run Configuration",
  description = "Describes how and with which engine a pipeline is to be executed"
)
@GuiPlugin
@GuiMetaStoreElement(
  name = "Pipeline Run Configuration",
  description = "Describes how and with which engine a pipeline is to be executed",
  iconImage = "ui/images/run.svg"
)
public class PipelineRunConfiguration implements Cloneable, IVariables, IHopMetaStoreElement<PipelineRunConfiguration> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PipelineRunConfiguration-PluginSpecific-Options";


  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private IPipelineEngineRunConfiguration engineRunConfiguration;

  private IVariables variables = new Variables();

  public PipelineRunConfiguration() {
  }

  public PipelineRunConfiguration( String name, String description, IPipelineEngineRunConfiguration engineRunConfiguration ) {
    this.name = name;
    this.description = description;
    this.engineRunConfiguration = engineRunConfiguration;
  }

  public PipelineRunConfiguration( PipelineRunConfiguration runConfiguration ) {
    this();
    this.name = runConfiguration.name;
    this.description = runConfiguration.description;
    if ( runConfiguration.getEngineRunConfiguration() != null ) {
      this.engineRunConfiguration = runConfiguration.engineRunConfiguration.clone();
    }
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    PipelineRunConfiguration that = (PipelineRunConfiguration) o;
    return name.equals( that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name );
  }

  @Override
  public void copyVariablesFrom( IVariables variables ) {
    variables.copyVariablesFrom( variables );
  }

  @Override
  public String environmentSubstitute( String aString ) {
    return variables.environmentSubstitute( aString );
  }

  @Override
  public String[] environmentSubstitute( String[] aString ) {
    return variables.environmentSubstitute( aString );
  }

  @Override
  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData ) throws HopValueException {
    return variables.fieldSubstitute( aString, rowMeta, rowData );
  }

  @Override
  public IVariables getParentVariableSpace() {
    return variables.getParentVariableSpace();
  }

  @Override
  public void setParentVariableSpace( IVariables parent ) {
    variables.setParentVariableSpace( parent );
  }

  @Override
  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  @Override
  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  @Override
  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaBase.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  @Override
  public void initializeVariablesFrom( IVariables parent ) {
    variables.initializeVariablesFrom( parent );
  }

  @Override
  public String[] listVariables() {
    return variables.listVariables();
  }

  @Override
  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  @Override
  public void shareVariablesWith( IVariables variables ) {
    variables = variables;
  }

  @Override
  public void injectVariables( Map<String, String> prop ) {
    variables.injectVariables( prop );
  }

  @Override public MetaStoreFactory<PipelineRunConfiguration> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<PipelineRunConfiguration> createFactory( IMetaStore metaStore ) {
    MetaStoreFactory<PipelineRunConfiguration> factory = new MetaStoreFactory<>( PipelineRunConfiguration.class, metaStore, HopDefaults.NAMESPACE );
    factory.setObjectFactory( new PipelineRunConfigurationMetaStoreObjectFactory() );
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
  public IPipelineEngineRunConfiguration getEngineRunConfiguration() {
    return engineRunConfiguration;
  }

  /**
   * @param engineRunConfiguration The engineRunConfiguration to set
   */
  public void setEngineRunConfiguration( IPipelineEngineRunConfiguration engineRunConfiguration ) {
    this.engineRunConfiguration = engineRunConfiguration;
  }
}
