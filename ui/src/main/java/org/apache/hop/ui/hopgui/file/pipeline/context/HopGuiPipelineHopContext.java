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

package org.apache.hop.ui.hopgui.file.pipeline.context;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiPipelineHopContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiPipelineHopContext";

  private PipelineMeta pipelineMeta;
  private PipelineHopMeta hopMeta;
  private HopGuiPipelineGraph pipelineGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiPipelineHopContext> lambdaBuilder;

  public HopGuiPipelineHopContext( PipelineMeta pipelineMeta, PipelineHopMeta hopMeta, HopGuiPipelineGraph pipelineGraph, Point click ) {
    this.pipelineMeta = pipelineMeta;
    this.hopMeta = hopMeta;
    this.pipelineGraph = pipelineGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a pipeline.
   * These are picked up from the annotations, mostly in PipelineGraph. Plugins can add actions as well.
   *
   * @return The list of supported actions
   */
  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = getPluginActions( true );
    if ( pluginActions != null ) {
      for ( GuiAction pluginAction : pluginActions ) {
        actions.add( lambdaBuilder.createLambda( pluginAction, this, pipelineGraph ) );
      }
    }

    return actions;
  }


  /**
   * Gets pipelineMeta
   *
   * @return value of pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta The pipelineMeta to set
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Gets hopMeta
   *
   * @return value of hopMeta
   */
  public PipelineHopMeta getHopMeta() {
    return hopMeta;
  }

  /**
   * @param hopMeta The hopMeta to set
   */
  public void setHopMeta( PipelineHopMeta hopMeta ) {
    this.hopMeta = hopMeta;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph( HopGuiPipelineGraph pipelineGraph ) {
    this.pipelineGraph = pipelineGraph;
  }

  /**
   * Gets click
   *
   * @return value of click
   */
  public Point getClick() {
    return click;
  }

  /**
   * @param click The click to set
   */
  public void setClick( Point click ) {
    this.click = click;
  }


}
