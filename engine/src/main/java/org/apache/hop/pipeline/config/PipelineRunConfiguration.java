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

package org.apache.hop.pipeline.config;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.PipelineMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@MetaStoreElementType(
  name = "Pipeline Run Configuration",
  description = "Describes how and with which engine a pipeline is to be executed"
)
public class PipelineRunConfiguration extends Variables implements Cloneable, IVariables, IHopMetaStoreElement<PipelineRunConfiguration> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PipelineRunConfiguration-PluginSpecific-Options";


  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<VariableValueDescription> configurationVariables;

  @MetaStoreAttribute
  private IPipelineEngineRunConfiguration engineRunConfiguration;

  public PipelineRunConfiguration() {
    configurationVariables = new ArrayList<>();
  }

  public PipelineRunConfiguration( String name, String description, List<VariableValueDescription> configurationVariables,
                                   IPipelineEngineRunConfiguration engineRunConfiguration ) {
    this.name = name;
    this.description = description;
    this.configurationVariables = configurationVariables;
    this.engineRunConfiguration = engineRunConfiguration;
  }

  public PipelineRunConfiguration( PipelineRunConfiguration runConfiguration ) {
    this();
    this.name = runConfiguration.name;
    this.description = runConfiguration.description;
    this.configurationVariables.addAll(runConfiguration.getConfigurationVariables());
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

  @Override public MetaStoreFactory<PipelineRunConfiguration> getFactory( IMetaStore metaStore ) {
    return createFactory( metaStore );
  }

  public static final MetaStoreFactory<PipelineRunConfiguration> createFactory( IMetaStore metaStore ) {
    MetaStoreFactory<PipelineRunConfiguration> factory = new MetaStoreFactory<>( PipelineRunConfiguration.class, metaStore );
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
   * Gets configurationVariables
   *
   * @return value of configurationVariables
   */
  public List<VariableValueDescription> getConfigurationVariables() {
    return configurationVariables;
  }

  /**
   * @param configurationVariables The configurationVariables to set
   */
  public void setConfigurationVariables( List<VariableValueDescription> configurationVariables ) {
    this.configurationVariables = configurationVariables;
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

  public <T extends PipelineMeta> void applyToVariables( IVariables variables ) {
    for (VariableValueDescription vvd : configurationVariables) {
      if ( StringUtils.isNotEmpty(vvd.getName())) {
        variables.setVariable( vvd.getName(), variables.environmentSubstitute(vvd.getValue()) );
      }
    }
  }
}
