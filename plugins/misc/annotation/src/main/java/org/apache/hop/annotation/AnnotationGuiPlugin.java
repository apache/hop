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

package org.apache.hop.annotation;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;

/** Context actions that add, edit, and remove an annotation. */
@GuiPlugin
public class AnnotationGuiPlugin {
  static final String ACTION_ANNOTATE_TRANSFORM = "pipeline-graph-transform-12000-annotate";
  static final String ACTION_REMOVE_TRANSFORM = "pipeline-graph-transform-12001-remove-annotation";
  static final String ACTION_ANNOTATE_ACTION = "workflow-graph-action-12000-annotate";
  static final String ACTION_REMOVE_ACTION = "workflow-graph-action-12001-remove-annotation";

  @GuiContextAction(
      id = ACTION_ANNOTATE_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::AnnotationGuiPlugin.Annotate.Name",
      tooltip = "i18n::AnnotationGuiPlugin.Annotate.Tooltip",
      image = "ui/images/note.svg",
      category = "i18n::AnnotationGuiPlugin.Category",
      categoryOrder = "9")
  public void annotateTransform(HopGuiPipelineTransformContext context) {
    HopGuiPipelineGraph graph = context.getPipelineGraph();
    AnnotationClicks.editTransform(graph, context.getTransformMeta());
  }

  @GuiContextAction(
      id = ACTION_REMOVE_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::AnnotationGuiPlugin.Remove.Name",
      tooltip = "i18n::AnnotationGuiPlugin.Remove.Tooltip",
      image = "ui/images/note.svg",
      category = "i18n::AnnotationGuiPlugin.Category",
      categoryOrder = "9")
  public void removeTransformAnnotation(HopGuiPipelineTransformContext context) {
    HopGuiPipelineGraph graph = context.getPipelineGraph();
    AnnotationClicks.remove(
        graph, context.getPipelineMeta()::setChanged, context.getTransformMeta(), graph::redraw);
  }

  @GuiContextActionFilter(parentId = HopGuiPipelineTransformContext.CONTEXT_ID)
  public boolean filterTransformActions(
      String contextActionId, HopGuiPipelineTransformContext context) {
    if (!ACTION_REMOVE_TRANSFORM.equals(contextActionId)) {
      return true;
    }
    return AnnotationStore.read(context.getTransformMeta()) != null;
  }

  @GuiContextAction(
      id = ACTION_ANNOTATE_ACTION,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::AnnotationGuiPlugin.Annotate.Name",
      tooltip = "i18n::AnnotationGuiPlugin.Annotate.Tooltip",
      image = "ui/images/note.svg",
      category = "i18n::AnnotationGuiPlugin.Category",
      categoryOrder = "9")
  public void annotateAction(HopGuiWorkflowActionContext context) {
    HopGuiWorkflowGraph graph = context.getWorkflowGraph();
    AnnotationClicks.editAction(graph, context.getActionMeta());
  }

  @GuiContextAction(
      id = ACTION_REMOVE_ACTION,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::AnnotationGuiPlugin.Remove.Name",
      tooltip = "i18n::AnnotationGuiPlugin.Remove.Tooltip",
      image = "ui/images/note.svg",
      category = "i18n::AnnotationGuiPlugin.Category",
      categoryOrder = "9")
  public void removeActionAnnotation(HopGuiWorkflowActionContext context) {
    HopGuiWorkflowGraph graph = context.getWorkflowGraph();
    AnnotationClicks.remove(
        graph, context.getWorkflowMeta()::setChanged, context.getActionMeta(), graph::redraw);
  }

  @GuiContextActionFilter(parentId = HopGuiWorkflowActionContext.CONTEXT_ID)
  public boolean filterActionActions(String contextActionId, HopGuiWorkflowActionContext context) {
    if (!ACTION_REMOVE_ACTION.equals(contextActionId)) {
      return true;
    }
    return AnnotationStore.read(context.getActionMeta()) != null;
  }
}
