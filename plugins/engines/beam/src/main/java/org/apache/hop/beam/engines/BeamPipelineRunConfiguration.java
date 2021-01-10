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

package org.apache.hop.beam.engines;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;


public abstract class BeamPipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IBeamPipelineEngineRunConfiguration {

  @GuiWidgetElement(
    order = "90000-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "User agent"
  )
  @HopMetadataProperty
  protected String userAgent;

  @GuiWidgetElement(
    order = "90010-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Temp location"
  )
  @HopMetadataProperty
  protected String tempLocation;

  @GuiWidgetElement(
    order = "90020-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Plugins to stage (, delimited)"
  )
  @HopMetadataProperty
  protected String pluginsToStage;

  @GuiWidgetElement(
    order = "90030-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Transform plugin classes"
  )
  @HopMetadataProperty
  protected String transformPluginClasses;

  @GuiWidgetElement(
    order = "90040-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "XP plugin classes"
  )
  @HopMetadataProperty
  protected String xpPluginClasses;

  @GuiWidgetElement(
    order = "90050-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Streaming Hop transforms flush interval (ms)"
  )
  @HopMetadataProperty
  protected String streamingHopTransformsFlushInterval;

  @GuiWidgetElement(
    order = "90060-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Hop streaming transforms buffer size"
  )
  @HopMetadataProperty
  protected String streamingHopTransformsBufferSize;

  @GuiWidgetElement(
    order = "90070-general-options",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "Fat jar file location"
  )
  @HopMetadataProperty
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
