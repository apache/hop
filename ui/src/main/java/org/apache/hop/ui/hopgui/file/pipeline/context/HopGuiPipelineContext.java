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

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiPipelineContext";

  private PipelineMeta pipelineMeta;
  private HopGuiPipelineGraph pipelineGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiPipelineContext> lambdaBuilder;

  public HopGuiPipelineContext( PipelineMeta pipelineMeta, HopGuiPipelineGraph pipelineGraph, Point click ) {
    this.pipelineMeta = pipelineMeta;
    this.pipelineGraph = pipelineGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }


  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a pipeline.
   * We'll add the creation of every possible transform as well as the modification of the pipeline itself.
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

    // Also add all the transform creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> transformPlugins = registry.getPlugins( TransformPluginType.class );
    for ( IPlugin transformPlugin : transformPlugins ) {
      GuiAction createTransformAction =
        new GuiAction( "pipeline-graph-create-transform-" + transformPlugin.getIds()[ 0 ], GuiActionType.Create, transformPlugin.getName(), transformPlugin.getDescription(), transformPlugin.getImageFile(),
          (shiftClicked, controlClicked, t) -> {
            pipelineGraph.pipelineTransformDelegate.newTransform( pipelineMeta, transformPlugin.getIds()[ 0 ], transformPlugin.getName(), transformPlugin.getDescription(), controlClicked, true, click );
          }
        );
      createTransformAction.getKeywords().addAll( Arrays.asList(transformPlugin.getKeywords()));
      createTransformAction.setCategory( transformPlugin.getCategory() );
      createTransformAction.setCategoryOrder( "9999_"+transformPlugin.getCategory() ); // sort alphabetically
      try {
        createTransformAction.setClassLoader( registry.getClassLoader( transformPlugin ) );
      } catch ( HopPluginException e ) {
        LogChannel.UI.logError( "Unable to get classloader for transform plugin " + transformPlugin.getIds()[ 0 ], e );
      }
      createTransformAction.getKeywords().add( transformPlugin.getCategory() );
      actions.add( createTransformAction );
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
