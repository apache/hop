package org.apache.hop.pipeline.engines;

import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;

import java.util.Objects;

public class EmptyPipelineRunConfiguration implements IPipelineEngineRunConfiguration, Cloneable {

  private String pluginId;
  private String pluginName;

  public EmptyPipelineRunConfiguration() {
  }

  public EmptyPipelineRunConfiguration( String pluginId, String pluginName ) {
    this.pluginId = pluginId;
    this.pluginName = pluginName;
  }

  public EmptyPipelineRunConfiguration( EmptyPipelineRunConfiguration config ) {
    this.pluginId = config.pluginId;
    this.pluginName = config.pluginName;
  }

  public EmptyPipelineRunConfiguration clone() {
    return new EmptyPipelineRunConfiguration( this );
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    EmptyPipelineRunConfiguration that = (EmptyPipelineRunConfiguration) o;
    return pluginId.equals( that.pluginId );
  }

  @Override public int hashCode() {
    return Objects.hash( pluginId );
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  public String getEnginePluginId() {
    return pluginId;
  }

  /**
   * @param pluginId The pluginId to set
   */
  @Override public void setEnginePluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  public String getEnginePluginName() {
    return pluginName;
  }

  /**
   * @param pluginName The pluginName to set
   */
  @Override public void setEnginePluginName( String pluginName ) {
    this.pluginName = pluginName;
  }
}
