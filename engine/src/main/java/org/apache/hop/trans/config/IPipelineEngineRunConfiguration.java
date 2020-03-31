package org.apache.hop.trans.config;

import org.apache.hop.trans.TransExecutionConfiguration;

public interface IPipelineEngineRunConfiguration extends Cloneable {

  IPipelineEngineRunConfiguration clone();

  void setEnginePluginId( String pluginId );

  String getEnginePluginId();

  void setEnginePluginName( String name );

  String getEnginePluginName();
}
