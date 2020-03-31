package org.apache.hop.trans.engines.localsingle;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.trans.config.IPipelineEngineRunConfiguration;
import org.apache.hop.trans.engines.EmptyPipelineRunConfiguration;

@GuiPlugin
public class LocalSinglePipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IPipelineEngineRunConfiguration {

  public LocalSinglePipelineRunConfiguration() {
    super();
  }

  public LocalSinglePipelineRunConfiguration( String pluginId, String pluginName, String rowSetSize ) {
    super( pluginId, pluginName );
  }

  public LocalSinglePipelineRunConfiguration( LocalSinglePipelineRunConfiguration config ) {
    super( config );
  }

  public LocalSinglePipelineRunConfiguration clone() {
    return new LocalSinglePipelineRunConfiguration( this );
  }
}
