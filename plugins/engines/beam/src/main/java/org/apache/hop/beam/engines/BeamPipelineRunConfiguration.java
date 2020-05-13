package org.apache.hop.beam.engines;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;


public abstract class BeamPipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IBeamPipelineEngineRunConfiguration {

  @GuiWidgetElement(
    order = "90000-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "User agent"
  )
  @MetaStoreAttribute
  protected String userAgent;

  @GuiWidgetElement(
    order = "90010-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Temp location"
  )
  @MetaStoreAttribute
  protected String tempLocation;

  @GuiWidgetElement(
    order = "90020-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Plugins to stage (, delimited)"
  )
  @MetaStoreAttribute
  protected String pluginsToStage;

  @GuiWidgetElement(
    order = "90030-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Transform plugin classes"
  )
  @MetaStoreAttribute
  protected String transformPluginClasses;

  @GuiWidgetElement(
    order = "90040-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "XP plugin classes"
  )
  @MetaStoreAttribute
  protected String xpPluginClasses;

  @GuiWidgetElement(
    order = "90050-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Streaming Hop transforms flush interval (ms)"
  )
  @MetaStoreAttribute
  protected String streamingHopTransformsFlushInterval;

  @GuiWidgetElement(
    order = "90060-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Hop streaming transforms buffer size"
  )
  @MetaStoreAttribute
  protected String streamingHopTransformsBufferSize;

  @GuiWidgetElement(
    order = "90070-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Fat jar file location"
  )
  @MetaStoreAttribute
  protected String fatJar;

  public BeamPipelineRunConfiguration() {
    userAgent = "Hop";
  }

  public BeamPipelineRunConfiguration( BeamPipelineRunConfiguration config ) {
    super( config );
    this.userAgent = config.userAgent;
    this.tempLocation = config.tempLocation;
    this.pluginsToStage = config.pluginsToStage;
    this.transformPluginClasses = config.transformPluginClasses;
    this.xpPluginClasses = config.xpPluginClasses;
    this.streamingHopTransformsFlushInterval = config.streamingHopTransformsFlushInterval;
    this.streamingHopTransformsBufferSize = config.streamingHopTransformsBufferSize;
    this.fatJar = config.fatJar;
  }

  /**
   * Gets userAgent
   *
   * @return value of userAgent
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * @param userAgent The userAgent to set
   */
  public void setUserAgent( String userAgent ) {
    this.userAgent = userAgent;
  }

  /**
   * Gets tempLocation
   *
   * @return value of tempLocation
   */
  public String getTempLocation() {
    return tempLocation;
  }

  /**
   * @param tempLocation The tempLocation to set
   */
  public void setTempLocation( String tempLocation ) {
    this.tempLocation = tempLocation;
  }

  /**
   * Gets pluginsToStage
   *
   * @return value of pluginsToStage
   */
  public String getPluginsToStage() {
    return pluginsToStage;
  }

  /**
   * @param pluginsToStage The pluginsToStage to set
   */
  public void setPluginsToStage( String pluginsToStage ) {
    this.pluginsToStage = pluginsToStage;
  }

  /**
   * Gets transformPluginClasses
   *
   * @return value of transformPluginClasses
   */
  public String getTransformPluginClasses() {
    return transformPluginClasses;
  }

  /**
   * @param transformPluginClasses The transformPluginClasses to set
   */
  public void setTransformPluginClasses( String transformPluginClasses ) {
    this.transformPluginClasses = transformPluginClasses;
  }

  /**
   * Gets xpPluginClasses
   *
   * @return value of xpPluginClasses
   */
  public String getXpPluginClasses() {
    return xpPluginClasses;
  }

  /**
   * @param xpPluginClasses The xpPluginClasses to set
   */
  public void setXpPluginClasses( String xpPluginClasses ) {
    this.xpPluginClasses = xpPluginClasses;
  }

  /**
   * Gets streamingHopTransformsFlushInterval
   *
   * @return value of streamingHopTransformsFlushInterval
   */
  public String getStreamingHopTransformsFlushInterval() {
    return streamingHopTransformsFlushInterval;
  }

  /**
   * @param streamingHopTransformsFlushInterval The streamingHopTransformsFlushInterval to set
   */
  public void setStreamingHopTransformsFlushInterval( String streamingHopTransformsFlushInterval ) {
    this.streamingHopTransformsFlushInterval = streamingHopTransformsFlushInterval;
  }

  /**
   * Gets streamingHopTransformsBufferSize
   *
   * @return value of streamingHopTransformsBufferSize
   */
  @Override public String getStreamingHopTransformsBufferSize() {
    return streamingHopTransformsBufferSize;
  }

  /**
   * @param streamingHopTransformsBufferSize The streamingHopTransformsBufferSize to set
   */
  public void setStreamingHopTransformsBufferSize( String streamingHopTransformsBufferSize ) {
    this.streamingHopTransformsBufferSize = streamingHopTransformsBufferSize;
  }

  /**
   * Gets fatJar
   *
   * @return value of fatJar
   */
  public String getFatJar() {
    return fatJar;
  }

  /**
   * @param fatJar The fatJar to set
   */
  public void setFatJar( String fatJar ) {
    this.fatJar = fatJar;
  }
}
