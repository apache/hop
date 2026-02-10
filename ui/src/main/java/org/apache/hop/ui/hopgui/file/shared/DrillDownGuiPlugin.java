/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.file.shared;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;

/**
 * Unified GUI Plugin for drill-down functionality. Works for ALL transforms and actions that
 * support drill-down by checking the supportsDrillDown() method.
 */
@GuiPlugin
public class DrillDownGuiPlugin {

  private static final Class<?> PKG = DrillDownGuiPlugin.class;

  // Global registries to track running instances
  private static final Map<String, IPipelineEngine<PipelineMeta>> runningPipelines =
      new ConcurrentHashMap<>();
  private static final Map<String, IWorkflowEngine<WorkflowMeta>> runningWorkflows =
      new ConcurrentHashMap<>();
  public static final String DRILL_DOWN_ERROR_OPENING_EXECUTION =
      "DrillDown.Error.OpeningExecution";
  public static final String DRILL_DOWN_ERROR_TITLE = "DrillDown.Error.Title";

  public static void registerRunningPipeline(
      String logChannelId, IPipelineEngine<PipelineMeta> pipeline) {
    runningPipelines.put(logChannelId, pipeline);
  }

  public static void registerRunningWorkflow(
      String logChannelId, IWorkflowEngine<WorkflowMeta> workflow) {
    runningWorkflows.put(logChannelId, workflow);
  }

  /**
   * Clears all drill-down and sample-row state. Call when starting a new run to get a clean slate
   * and avoid leaking memory.
   */
  public static void cleanupOnRunStart() {
    runningPipelines.clear();
    runningWorkflows.clear();
    dataSnifferBuffersByLogChannelId.clear();
  }

  /**
   * Per-run data sniffer buffers (logChannelId -> transform name -> RowBuffer). Filled when
   * pipelines start so we have row data even if the pipeline tab is never opened. Only the latest
   * run's data is kept per execution (each run has its own logChannelId).
   */
  private static final Map<String, Map<String, RowBuffer>> dataSnifferBuffersByLogChannelId =
      new ConcurrentHashMap<>();

  /**
   * Attach row listeners to a pipeline at start so we capture output rows for debugging. Uses the
   * pipeline's run configuration (sample type and size in GUI) when available; otherwise no
   * sniffers are attached. Data is stored in {@link #dataSnifferBuffersByLogChannelId} so it is
   * available when the user later opens the pipeline tab. Only applies to local pipeline engine.
   */
  public static void attachDataSniffersToPipeline(IPipelineEngine<PipelineMeta> pipeline) {
    if (!(pipeline instanceof LocalPipelineEngine)) {
      return;
    }
    String logChannelId = pipeline.getLogChannelId();
    Map<String, RowBuffer> buffers =
        dataSnifferBuffersByLogChannelId.computeIfAbsent(
            logChannelId, k -> new ConcurrentHashMap<>());
    PipelineRowSamplerHelper.addRowSamplersToPipeline(pipeline, buffers);
  }

  /**
   * Return the data sniffer buffers for a pipeline run (if any). Used by the pipeline graph when
   * attaching to a running/finished pipeline so the UI can show the rows that flowed through.
   */
  public static Map<String, RowBuffer> getDataSnifferBuffersForPipeline(String logChannelId) {
    return dataSnifferBuffersByLogChannelId.get(logChannelId);
  }

  // ==================== TRANSFORM CONTEXT ====================

  @GuiContextAction(
      id = "pipeline-graph-transform-09990-open-execution",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::DrillDown.OpenExecution.Name",
      tooltip = "i18n::DrillDown.OpenExecution.Tooltip",
      image = "ui/images/running.svg",
      category = "Basic",
      categoryOrder = "1")
  public void openTransformExecution(HopGuiPipelineTransformContext context) {
    TransformMeta transformMeta = context.getTransformMeta();
    HopGuiPipelineGraph pipelineGraph = context.getPipelineGraph();

    if (!transformMeta.getTransform().supportsDrillDown()) {
      return;
    }

    try {
      IPipelineEngine<?> parent = pipelineGraph.getPipeline();
      if (parent == null) {
        return;
      }
      findRunningChildAndOpen(
          parent, transformMeta.getName(), pipelineGraph.getDisplay(), pipelineGraph.getHopGui());
    } catch (Exception e) {
      new ErrorDialog(
          pipelineGraph.getShell(),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_TITLE),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_OPENING_EXECUTION),
          e);
    }
  }

  @GuiContextActionFilter(parentId = HopGuiPipelineTransformContext.CONTEXT_ID)
  public boolean filterTransformDrillDown(
      String contextActionId, HopGuiPipelineTransformContext context) {
    if (contextActionId.equals("pipeline-graph-transform-09990-open-execution")) {
      TransformMeta transformMeta = context.getTransformMeta();
      return transformMeta.getTransform().supportsDrillDown()
          && context.getPipelineGraph().getPipeline() != null;
    }
    return true;
  }

  // ==================== ACTION CONTEXT ====================

  @GuiContextAction(
      id = "workflow-graph-action-09990-open-execution",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::DrillDown.OpenExecution.Name",
      tooltip = "i18n::DrillDown.OpenExecution.Tooltip",
      image = "ui/images/running.svg",
      category = "Basic",
      categoryOrder = "1")
  public void openActionExecution(HopGuiWorkflowActionContext context) {
    ActionMeta actionMeta = context.getActionMeta();
    HopGuiWorkflowGraph workflowGraph = context.getWorkflowGraph();

    if (!actionMeta.getAction().supportsDrillDown()) {
      return;
    }

    try {
      IWorkflowEngine<?> parent = workflowGraph.getWorkflow();
      if (parent == null) {
        return;
      }
      findRunningChildAndOpen(
          parent, actionMeta.getName(), workflowGraph.getDisplay(), workflowGraph.getHopGui());
    } catch (Exception e) {
      new ErrorDialog(
          workflowGraph.getShell(),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_TITLE),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_OPENING_EXECUTION),
          e);
    }
  }

  @GuiContextActionFilter(parentId = HopGuiWorkflowActionContext.CONTEXT_ID)
  public boolean filterActionDrillDown(
      String contextActionId, HopGuiWorkflowActionContext context) {
    if (contextActionId.equals("workflow-graph-action-09990-open-execution")) {
      ActionMeta actionMeta = context.getActionMeta();
      return actionMeta.getAction().supportsDrillDown()
          && context.getWorkflowGraph().getWorkflow() != null;
    }
    return true;
  }

  // ==================== HELPER METHODS ====================

  /**
   * Polls for a running child (pipeline or workflow) of the given parent, then opens a tab with
   * meta from that engine and attaches. Runs in a background thread; UI updates are done via
   * display.asyncExec. If no running child is found within the timeout, shows an info message.
   */
  private void findRunningChildAndOpen(
      Object parentEngine, String childName, Display display, HopGui hopGui) {
    new Thread(
            () -> {
              try {
                for (int i = 0; i < 50; i++) {
                  RunningChild child = findRunningChild(parentEngine, childName);
                  if (child != null) {
                    if (child.pipeline != null) {
                      PipelineMeta meta = child.pipeline.getPipelineMeta();
                      if (meta != null) {
                        display.asyncExec(
                            () -> openTabAndAttachPipeline(hopGui, meta, child.pipeline));
                        return;
                      }
                    } else {
                      WorkflowMeta meta = child.workflow.getWorkflowMeta();
                      if (meta != null) {
                        display.asyncExec(
                            () -> openTabAndAttachWorkflow(hopGui, meta, child.workflow));
                        return;
                      }
                    }
                  }
                  Thread.sleep(100);
                }
                display.asyncExec(() -> showNoRunningExecution(hopGui));
              } catch (Exception e) {
                // Ignore
              }
            })
        .start();
  }

  /**
   * Result of a single scan for a running child; exactly one of pipeline or workflow is non-null.
   */
  private static final class RunningChild {
    final IPipelineEngine<PipelineMeta> pipeline;
    final IWorkflowEngine<WorkflowMeta> workflow;

    RunningChild(IPipelineEngine<PipelineMeta> pipeline, IWorkflowEngine<WorkflowMeta> workflow) {
      this.pipeline = pipeline;
      this.workflow = workflow;
    }
  }

  private void openTabAndAttachPipeline(
      HopGui hopGui, PipelineMeta meta, IPipelineEngine<PipelineMeta> toAttach) {
    try {
      ExplorerPerspective perspective =
          hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
      if (perspective == null) {
        return;
      }
      HopGuiPipelineGraph pipelineGraph =
          findOpenPipelineGraph(perspective, meta.getName(), meta.getFilename());
      if (pipelineGraph == null) {
        pipelineGraph = (HopGuiPipelineGraph) perspective.addPipeline(meta);
      } else {
        perspective.setActiveFileTypeHandler(pipelineGraph);
      }
      perspective.activate();
      pipelineGraph.attachToRunningPipeline(toAttach);
      if (!pipelineGraph.isExecutionResultsPaneVisible()) {
        pipelineGraph.showExecutionResults();
      }
    } catch (HopException e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_TITLE),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_OPENING_EXECUTION),
          e);
    }
  }

  private void openTabAndAttachWorkflow(
      HopGui hopGui, WorkflowMeta meta, IWorkflowEngine<WorkflowMeta> toAttach) {
    try {
      ExplorerPerspective perspective =
          hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
      if (perspective == null) {
        return;
      }
      HopGuiWorkflowGraph workflowGraph =
          findOpenWorkflowGraph(perspective, meta.getName(), meta.getFilename());
      if (workflowGraph == null) {
        workflowGraph = (HopGuiWorkflowGraph) perspective.addWorkflow(meta);
      } else {
        perspective.setActiveFileTypeHandler(workflowGraph);
      }
      perspective.activate();
      workflowGraph.attachToRunningWorkflow(toAttach);
      if (!workflowGraph.isExecutionResultsPaneVisible()) {
        workflowGraph.showExecutionResults();
      }
    } catch (HopException e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_TITLE),
          BaseMessages.getString(PKG, DRILL_DOWN_ERROR_OPENING_EXECUTION),
          e);
    }
  }

  private void showNoRunningExecution(HopGui hopGui) {
    MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
    mb.setText(BaseMessages.getString(PKG, "DrillDown.NoRunningExecution.Title"));
    mb.setMessage(BaseMessages.getString(PKG, "DrillDown.NoRunningExecution.Message"));
    mb.open();
  }

  /**
   * Single scan over parent's children: looks up each child in both pipeline and workflow maps,
   * returns the one matching run (pipeline preferred over workflow, most recent by start time).
   */
  private RunningChild findRunningChild(Object parentEngine, String name) {
    String parentLogChannelId;
    if (parentEngine instanceof IPipelineEngine) {
      parentLogChannelId = ((IPipelineEngine<?>) parentEngine).getLogChannelId();
    } else if (parentEngine instanceof IWorkflowEngine) {
      parentLogChannelId = ((IWorkflowEngine<?>) parentEngine).getLogChannelId();
    } else {
      return null;
    }

    LoggingRegistry registry = LoggingRegistry.getInstance();
    List<String> childLogChannelIds = registry.getLogChannelChildren(parentLogChannelId);

    List<IPipelineEngine<PipelineMeta>> pipelines = new ArrayList<>();
    List<IWorkflowEngine<WorkflowMeta>> workflows = new ArrayList<>();
    for (String logChannelId : childLogChannelIds) {
      if (logChannelId.equals(parentLogChannelId)) {
        continue;
      }
      IPipelineEngine<PipelineMeta> pipeline = runningPipelines.get(logChannelId);
      if (pipeline != null && pipeline.getPipelineMeta() != null) {
        ILoggingObject parent = pipeline.getParent();
        if (name == null || (parent != null && name.equals(parent.getObjectName()))) {
          pipelines.add(pipeline);
        }
      }
      IWorkflowEngine<WorkflowMeta> workflow = runningWorkflows.get(logChannelId);
      if (workflow != null && workflow.getWorkflowMeta() != null) {
        ILoggingObject parent = workflow.getParent();
        if (name == null || (parent != null && name.equals(parent.getObjectName()))) {
          workflows.add(workflow);
        }
      }
    }

    pipelines.sort(
        Comparator.comparing(
            IPipelineEngine::getExecutionStartDate,
            Comparator.nullsLast(Comparator.reverseOrder())));
    workflows.sort(
        Comparator.comparing(
            IWorkflowEngine::getExecutionStartDate,
            Comparator.nullsLast(Comparator.reverseOrder())));

    if (!pipelines.isEmpty()) {
      return new RunningChild(pipelines.get(0), null);
    }
    if (!workflows.isEmpty()) {
      return new RunningChild(null, workflows.get(0));
    }
    return null;
  }

  private HopGuiPipelineGraph findOpenPipelineGraph(
      ExplorerPerspective perspective, String name, String filename) {
    for (TabItemHandler item : perspective.getItems()) {
      if (item.getTypeHandler() instanceof HopGuiPipelineGraph graph
          && (graph.getMeta().getName().equals(name)
              || (filename != null && filename.equals(graph.getMeta().getFilename())))) {
        return graph;
      }
    }
    return null;
  }

  private HopGuiWorkflowGraph findOpenWorkflowGraph(
      ExplorerPerspective perspective, String name, String filename) {
    for (TabItemHandler item : perspective.getItems()) {
      if (item.getTypeHandler() instanceof HopGuiWorkflowGraph graph
          && (graph.getMeta().getName().equals(name)
              || (filename != null && filename.equals(graph.getMeta().getFilename())))) {
        return graph;
      }
    }
    return null;
  }
}
