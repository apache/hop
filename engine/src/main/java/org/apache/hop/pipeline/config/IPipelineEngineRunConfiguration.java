package org.apache.hop.pipeline.config;

public interface IPipelineEngineRunConfiguration extends Cloneable {

  IPipelineEngineRunConfiguration clone();

  void setEnginePluginId( String pluginId );

  String getEnginePluginId();

  void setEnginePluginName( String name );

  String getEnginePluginName();
}
