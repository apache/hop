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

import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;

import java.util.ArrayList;
import java.util.List;

public class HopGuiPipelineTransformContext extends BaseGuiContextHandler
    implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiPipelineTransformContext";

  private PipelineMeta pipelineMeta;
  private TransformMeta transformMeta;
  private HopGuiPipelineGraph pipelineGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiPipelineTransformContext> lambdaBuilder;

  public HopGuiPipelineTransformContext(
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      HopGuiPipelineGraph pipelineGraph,
      Point click) {
    super();
    this.pipelineMeta = pipelineMeta;
    this.transformMeta = transformMeta;
    this.pipelineGraph = pipelineGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a pipeline. We'll add the creation of every possible
   * transform as well as the modification of the pipeline itself.
   *
   * @return The list of supported actions
   */
  @Override
  public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Put references at the start since we use those things a lot
    //
    ITransformMeta iTransformMeta = transformMeta.getTransform();

    String[] objectDescriptions = iTransformMeta.getReferencedObjectDescriptions();
    for (int i = 0; objectDescriptions != null && i < objectDescriptions.length; i++) {
      final String objectDescription = objectDescriptions[i];
      if (iTransformMeta.isReferencedObjectEnabled()[i]) {
        final int index = i;
        GuiAction openReferencedAction =
            new GuiAction(
                "transform-open-referenced-" + objectDescription,
                GuiActionType.Info,
                "open: " + objectDescription,
                "This opens up the file referenced in the transform",
                "ui/images/open.svg",
                (shiftAction, controlAction, t) ->
                    openReferencedObject(iTransformMeta, objectDescription, index));
        openReferencedAction.setCategory("Basic");
        openReferencedAction.setCategoryOrder("1");
        actions.add(openReferencedAction);
      }
    }

    // Get the actions from the plugins, sorted by ID...
    //
    List<GuiAction> pluginActions = getPluginActions(true);
    if (pluginActions != null) {
      for (GuiAction pluginAction : pluginActions) {
        actions.add(lambdaBuilder.createLambda(pluginAction, this, pipelineGraph));
      }
    }

    return actions;
  }

  private void openReferencedObject(
      ITransformMeta iTransformMeta, String objectDescription, int index) {
    HopGui hopGui = HopGui.getInstance();
    try {
      IHasFilename hasFilename =
          iTransformMeta.loadReferencedObject(
              index, pipelineMeta.getMetadataProvider(), pipelineGraph.getVariables());
      if (hasFilename != null) {
        String filename =
            pipelineGraph.getVariables().resolve(hasFilename.getFilename());

        // Is this object already loaded?
        //
        HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
        TabItemHandler tabItemHandler = perspective.findTabItemHandlerWithFilename(filename);
        if (tabItemHandler != null) {
          perspective.switchToTab(tabItemHandler);
        } else {
          hopGui.fileDelegate.fileOpen(filename);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error opening referenced object '" + objectDescription + "'",
          e);
    }
  }

  /**
   * Gets pipelineMeta
   *
   * @return value of pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /** @param pipelineMeta The pipelineMeta to set */
  public void setPipelineMeta(PipelineMeta pipelineMeta) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Gets transformMeta
   *
   * @return value of transformMeta
   */
  public TransformMeta getTransformMeta() {
    return transformMeta;
  }

  /** @param transformMeta The transformMeta to set */
  public void setTransformMeta(TransformMeta transformMeta) {
    this.transformMeta = transformMeta;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /** @param pipelineGraph The pipelineGraph to set */
  public void setPipelineGraph(HopGuiPipelineGraph pipelineGraph) {
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

  /** @param click The click to set */
  public void setClick(Point click) {
    this.click = click;
  }
}
