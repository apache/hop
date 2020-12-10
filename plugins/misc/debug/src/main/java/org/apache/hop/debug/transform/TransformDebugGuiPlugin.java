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

package org.apache.hop.debug.transform;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;

import java.util.HashMap;
import java.util.Map;

@GuiPlugin
public class TransformDebugGuiPlugin {

  @GuiContextAction(
    id = "pipeline-graph-transform-11001-clear-logging",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Clear Custom Logging",
    tooltip = "Clear custom log settings ",
    image = "ui/images/debug.svg",
    category = "Logging",
    categoryOrder = "7"
  )
  public void clearCustomTransformLogging( HopGuiPipelineTransformContext context ) {
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    Map<String, Map<String, String>> attributesMap = pipelineMeta.getAttributesMap();
    Map<String, String> debugGroupAttributesMap = attributesMap.get( Defaults.DEBUG_GROUP );

    DebugLevelUtil.clearDebugLevel( debugGroupAttributesMap, transformMeta.getName());
    pipelineMeta.setChanged();
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-11000-config-logging",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit Custom Logging",
    tooltip = "Edit the custom log settings for this transform",
    image = "ui/images/debug.svg",
    category = "Logging",
    categoryOrder = "7"
  )
  public void applyCustomTransformLogging( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = HopGui.getInstance();
    try {
      PipelineMeta pipelineMeta = context.getPipelineMeta();
      TransformMeta transformMeta = context.getTransformMeta();
      IVariables variables = context.getPipelineGraph().getVariables();

      Map<String, Map<String, String>> attributesMap = pipelineMeta.getAttributesMap();
      Map<String, String> debugGroupAttributesMap = attributesMap.get( Defaults.DEBUG_GROUP );

      if ( debugGroupAttributesMap == null ) {
        debugGroupAttributesMap = new HashMap<>();
        attributesMap.put( Defaults.DEBUG_GROUP, debugGroupAttributesMap );
      }

      TransformDebugLevel debugLevel = DebugLevelUtil.getTransformDebugLevel( debugGroupAttributesMap, transformMeta.getName() );
      if ( debugLevel==null ) {
        debugLevel = new TransformDebugLevel();
      }

      IRowMeta inputRowMeta = pipelineMeta.getPrevTransformFields( variables, transformMeta );
      TransformDebugLevelDialog dialog = new TransformDebugLevelDialog( hopGui.getShell(), debugLevel, inputRowMeta );
      if (dialog.open()) {
        DebugLevelUtil.storeTransformDebugLevel(debugGroupAttributesMap, transformMeta.getName(), debugLevel);
      }

      pipelineMeta.setChanged();
    } catch(Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error changing transform log settings", e);
    }

  }


}
