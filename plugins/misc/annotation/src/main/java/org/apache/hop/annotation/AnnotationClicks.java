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

import org.apache.hop.core.IAttributes;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;
import org.apache.hop.ui.hopgui.file.shared.ISnapshotUndoSupport;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Shell;

/**
 * Canvas clicks on an annotation callout. The callout sits outside the icon, so the click is
 * claimed.
 */
public final class AnnotationClicks {
  private static final Class<?> PKG = Annotation.class;

  /**
   * True after a press on a callout until the matching release. The UI thread is the only writer.
   */
  private static boolean calloutPressed;

  private AnnotationClicks() {}

  public static void pipelineMouseDown(HopGuiPipelineGraphExtension extension) {
    if (!claim(
        extension == null ? null : extension.getAreaOwner(),
        extension == null ? null : extension.getEvent())) {
      calloutPressed = false;
      return;
    }
    extension.setPreventingDefault(true);
    calloutPressed = true;
    if (extension.getEvent().button != 1) {
      return;
    }
    Object subject = ((AnnotationHit) extension.getAreaOwner().getParent()).subject();
    if (subject instanceof TransformMeta transform) {
      editTransform(extension.getPipelineGraph(), transform);
    }
  }

  public static void pipelineMouseUp(HopGuiPipelineGraphExtension extension) {
    if (extension == null) {
      calloutPressed = false;
      return;
    }
    swallowRelease(extension.getAreaOwner(), () -> extension.setPreventingDefault(true));
  }

  public static void pipelineDoubleClick(HopGuiPipelineGraphExtension extension) {
    if (isCallout(extension == null ? null : extension.getAreaOwner())) {
      extension.setPreventingDefault(true);
    }
  }

  public static void workflowMouseDown(HopGuiWorkflowGraphExtension extension) {
    if (!claim(
        extension == null ? null : extension.getAreaOwner(),
        extension == null ? null : extension.getEvent())) {
      calloutPressed = false;
      return;
    }
    extension.setPreventingDefault(true);
    calloutPressed = true;
    if (extension.getEvent().button != 1) {
      return;
    }
    Object subject = ((AnnotationHit) extension.getAreaOwner().getParent()).subject();
    if (subject instanceof ActionMeta action) {
      editAction(extension.getWorkflowGraph(), action);
    }
  }

  public static void workflowMouseUp(HopGuiWorkflowGraphExtension extension) {
    if (extension == null) {
      calloutPressed = false;
      return;
    }
    swallowRelease(extension.getAreaOwner(), () -> extension.setPreventingDefault(true));
  }

  public static void workflowDoubleClick(HopGuiWorkflowGraphExtension extension) {
    if (isCallout(extension == null ? null : extension.getAreaOwner())) {
      extension.setPreventingDefault(true);
    }
  }

  /**
   * Opens the annotation dialog for a transform and records one undo step when the text or
   * placement changes.
   *
   * @param graph the pipeline graph
   * @param transform the transform to annotate
   */
  public static void editTransform(HopGuiPipelineGraph graph, TransformMeta transform) {
    if (graph == null || transform == null) {
      return;
    }
    PipelineMeta pipelineMeta = graph.getPipelineMeta();
    edit(
        graph,
        pipelineMeta::setChanged,
        transform,
        graph.getShell(),
        graph.getVariables(),
        graph::redraw);
  }

  /**
   * Opens the annotation dialog for an action and records one undo step when the text or placement
   * changes.
   *
   * @param graph the workflow graph
   * @param action the action to annotate
   */
  public static void editAction(HopGuiWorkflowGraph graph, ActionMeta action) {
    if (graph == null || action == null) {
      return;
    }
    WorkflowMeta workflowMeta = graph.getWorkflowMeta();
    edit(
        graph,
        workflowMeta::setChanged,
        action,
        graph.getShell(),
        graph.getVariables(),
        graph::redraw);
  }

  /**
   * Removes the annotation and records one undo step.
   *
   * @param graph undo and redraw target
   * @param markChanged marks the file dirty
   * @param holder transform or action
   * @param redraw refreshes the canvas
   */
  public static void remove(
      ISnapshotUndoSupport graph, Runnable markChanged, IAttributes holder, Runnable redraw) {
    if (graph == null || holder == null || AnnotationStore.read(holder) == null) {
      return;
    }
    byte[] before = graph.captureUndoSnapshot();
    AnnotationStore.clear(holder);
    graph.commitDialogUndo(before);
    if (markChanged != null) {
      markChanged.run();
    }
    if (redraw != null) {
      redraw.run();
    }
  }

  private static void edit(
      ISnapshotUndoSupport undo,
      Runnable markChanged,
      IAttributes holder,
      Shell shell,
      IVariables variables,
      Runnable redraw) {
    try {
      byte[] before = undo.captureUndoSnapshot();
      Annotation existing = AnnotationStore.read(holder);
      String beforeText = existing == null ? null : existing.getText();
      AnnotationPlacement beforePlacement = existing == null ? null : existing.getPlacement();
      boolean beforeDashed = existing == null || existing.isDashedLine();
      Annotation editing = existing == null ? new Annotation() : existing;
      AnnotationDialog dialog = new AnnotationDialog(shell, variables, editing);
      if (!dialog.open()) {
        return;
      }
      AnnotationStore.write(holder, editing);
      if (same(beforeText, beforePlacement, beforeDashed, AnnotationStore.read(holder))) {
        return;
      }
      undo.commitDialogUndo(before);
      if (markChanged != null) {
        markChanged.run();
      }
      if (redraw != null) {
        redraw.run();
      }
    } catch (Exception exception) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "AnnotationDialog.Error.Title"),
          BaseMessages.getString(PKG, "AnnotationDialog.Error.Message"),
          exception);
    }
  }

  private static boolean same(
      String beforeText,
      AnnotationPlacement beforePlacement,
      boolean beforeDashed,
      Annotation after) {
    boolean beforeBlank = beforeText == null || beforeText.isBlank();
    boolean afterBlank = after == null || after.getText() == null || after.getText().isBlank();
    if (beforeBlank || afterBlank) {
      return beforeBlank && afterBlank;
    }
    AnnotationPlacement beforeSide =
        beforePlacement == null ? AnnotationPlacement.TOP : beforePlacement;
    AnnotationPlacement afterSide =
        after.getPlacement() == null ? AnnotationPlacement.TOP : after.getPlacement();
    boolean afterDashed = after.isDashedLine();
    return beforeText.equals(after.getText())
        && beforeSide == afterSide
        && beforeDashed == afterDashed;
  }

  private static void swallowRelease(AreaOwner areaOwner, Runnable preventDefault) {
    if (!calloutPressed && !isCallout(areaOwner)) {
      return;
    }
    calloutPressed = false;
    preventDefault.run();
  }

  private static boolean claim(AreaOwner areaOwner, MouseEvent event) {
    return event != null && isCallout(areaOwner);
  }

  private static boolean isCallout(AreaOwner areaOwner) {
    return areaOwner != null && areaOwner.getParent() instanceof AnnotationHit;
  }
}
