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

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.workflow.actions.missing.MissingActionDialog;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class HopGuiWorkflowActionDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  private HopGui hopGui;
  private HopGuiWorkflowGraph workflowGraph;

  public HopGuiWorkflowActionDelegate(HopGui hopGui, HopGuiWorkflowGraph workflowGraph) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public ActionMeta newAction(
      WorkflowMeta workflowMeta,
      String pluginId,
      String pluginName,
      boolean openIt,
      Point location) {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin actionPlugin;

    try {
      if (pluginId == null) {
        actionPlugin =
            PluginRegistry.getInstance().findPluginWithName(ActionPluginType.class, pluginName);
      } else {
        actionPlugin =
            PluginRegistry.getInstance().findPluginWithId(ActionPluginType.class, pluginId);
      }

      if (actionPlugin != null) {
        // Determine name & number for this entry.

        // See if the name is already used...
        //
        String actionName = pluginName;
        int nr = 2;
        ActionMeta check = workflowMeta.findAction(actionName);
        while (check != null) {
          actionName = pluginName + " " + nr++;
          check = workflowMeta.findAction(actionName);
        }

        // Generate the appropriate class...
        IAction action = (IAction) registry.loadClass(actionPlugin);
        action.setPluginId(actionPlugin.getIds()[0]);
        action.setName(actionName);

        if ( action.isStart() ) {
          // Check if start is already on the canvas...
          if (workflowMeta.findStart() != null) {
            HopGuiWorkflowGraph.showOnlyStartOnceMessage(hopGui.getShell());
            return null;
          }
        }

        if (openIt) {
          IActionDialog d = getActionDialog(action, workflowMeta);
          if (d != null && d.open() != null) {
            ActionMeta actionMeta = new ActionMeta();
            actionMeta.setAction(action);
            if (location != null) {
              actionMeta.setLocation(location.x, location.y);
            } else {
              actionMeta.setLocation(50, 50);
            }
            workflowMeta.addAction(actionMeta);

            // Verify that the name is not already used in the workflow.
            //
            workflowMeta.renameActionIfNameCollides(actionMeta);

            hopGui.undoDelegate.addUndoNew(
                workflowMeta, new ActionMeta[] {actionMeta}, new int[] {workflowMeta.indexOfAction(actionMeta)});
            workflowGraph.adjustScrolling();
            workflowGraph.updateGui();
            return actionMeta;
          } else {
            return null;
          }
        } else {
          ActionMeta actionMeta = new ActionMeta();
          actionMeta.setAction(action);
          if (location != null) {
            actionMeta.setLocation(location.x, location.y);
          } else {
            actionMeta.setLocation(50, 50);
          }          
          workflowMeta.addAction(actionMeta);
          hopGui.undoDelegate.addUndoNew(
              workflowMeta, new ActionMeta[] {actionMeta}, new int[] {workflowMeta.indexOfAction(actionMeta)});
          workflowGraph.adjustScrolling();
          workflowGraph.updateGui();
          return actionMeta;
        }
      } else {
        return null;
      }
    } catch (Throwable e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Title"),
          BaseMessages.getString(
              PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Message"),
          new Exception(e));
      return null;
    }
  }

  public IActionDialog getActionDialog(IAction action, WorkflowMeta workflowMeta) {
    Class<?>[] paramClasses = new Class<?>[] {Shell.class, IAction.class, WorkflowMeta.class};
    Object[] paramArgs = new Object[] {hopGui.getShell(), action, workflowMeta};

    if ( MissingAction.ID.equals(action.getPluginId())) {
	return new MissingActionDialog(hopGui.getShell(), action, workflowMeta);
    }
  
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(ActionPluginType.class, action);
    String dialogClassName = action.getDialogClassName();
    if (dialogClassName == null) {

      // optimistic: simply Dialog added to the action class
      //
      // org.apache.hop.workflow.actions.ActionZipFile
      //
      // gives
      //
      // org.apache.hop.workflow.actions.ActionZipFileDialog
      //

      dialogClassName = action.getClass().getCanonicalName();
      dialogClassName += "Dialog";

      try {
        // Try by injecting ui into the package. Convert:
        //
        // org.apache.hop.workflow.actions.ActionZipFileDialog
        //
        // into
        //
        // org.apache.hop.ui.workflow.actions.ActionZipFileDialog
        //
        ClassLoader pluginClassLoader = registry.getClassLoader(plugin);
        String alternateName = dialogClassName.replaceFirst("\\.hop\\.", ".hop.ui.");
        Class clazz = pluginClassLoader.loadClass(alternateName);
        dialogClassName = clazz.getName();
      } catch (Exception e) {
        // do nothing and return the optimistic plugin classname
      }
    }

    try {
      Class<IActionDialog> dialogClass = registry.getClass(plugin, dialogClassName);
      Constructor<IActionDialog> dialogConstructor = dialogClass.getConstructor(paramClasses);
      IActionDialog entryDialogInterface = dialogConstructor.newInstance(paramArgs);
      entryDialogInterface.setMetadataProvider(hopGui.getMetadataProvider());
      return entryDialogInterface;
    } catch (Throwable t) {
      t.printStackTrace();
      String errorTitle =
          BaseMessages.getString(PKG, "HopGui.Dialog.ErrorCreatingWorkflowDialog.Title");
      String errorMsg =
          BaseMessages.getString(
              PKG, "HopGui.Dialog.ErrorCreatingActionDialog.Message", dialogClassName);
      hopGui.getLog().logError(errorMsg);
      new ErrorDialog(hopGui.getShell(), errorTitle, errorMsg, t);
      return null;
    }
  }

  public void editAction(WorkflowMeta workflowMeta, ActionMeta action) {
    try {
      hopGui
          .getLog()
          .logDetailed(BaseMessages.getString(PKG, "HopGui.Log.EditAction", action.getName()));

      ActionMeta before = (ActionMeta) action.cloneDeep();

      IAction jei = action.getAction();

      IActionDialog d = getActionDialog(jei, workflowMeta);
      if (d != null) {
        if (d.open() != null) {
          // First see if the name changed.
          // If so, we need to verify that the name is not already used in the workflow.
          //
          workflowMeta.renameActionIfNameCollides(action);

          ActionMeta after = (ActionMeta) action.clone();
          hopGui.undoDelegate.addUndoChange(
              workflowMeta,
              new ActionMeta[] {before},
              new ActionMeta[] {after},
              new int[] {workflowMeta.indexOfAction(action)});
          workflowGraph.updateGui();
        }
      } else {
        MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(BaseMessages.getString(PKG, "HopGui.Dialog.ActionCanNotBeChanged.Message"));
        mb.setText(BaseMessages.getString(PKG, "HopGui.Dialog.ActionCanNotBeChanged.Title"));
        mb.open();
      }

    } catch (Exception e) {
      if (!hopGui.getShell().isDisposed()) {
        new ErrorDialog(
            hopGui.getShell(),
            BaseMessages.getString(PKG, "HopGui.ErrorDialog.ErrorEditingAction.Title"),
            BaseMessages.getString(PKG, "HopGui.ErrorDialog.ErrorEditingAction.Message"),
            e);
      }
    }
  }

  public void deleteActions(WorkflowMeta workflow, List<ActionMeta> actions) {

    // Hops belonging to the deleting actions are placed in a single transaction and removed.
    List<WorkflowHopMeta> workflowHops = new ArrayList<>();
    int[] hopIndexes = new int[workflow.nrWorkflowHops()];
    int hopIndex = 0;
    for (int i = workflow.nrWorkflowHops() - 1; i >= 0; i--) {
      WorkflowHopMeta hi = workflow.getWorkflowHop(i);
      for (int j = 0; j < actions.size() && hopIndex < hopIndexes.length; j++) {
        if (hi.getFromAction().equals(actions.get(j)) || hi.getToAction().equals(actions.get(j))) {
          int idx = workflow.indexOfWorkflowHop(hi);
          workflowHops.add((WorkflowHopMeta) hi.clone());
          hopIndexes[hopIndex] = idx;
          workflow.removeWorkflowHop(idx);
          hopIndex++;
          break;
        }
      }
    }
    if (!workflowHops.isEmpty()) {
      WorkflowHopMeta[] hops = workflowHops.toArray(new WorkflowHopMeta[workflowHops.size()]);
      hopGui.undoDelegate.addUndoDelete(workflow, hops, hopIndexes);
    }

    // Deleting actions are placed all in a single transaction and removed.
    int[] positions = new int[actions.size()];
    for (int i = 0; i < actions.size(); i++) {
      int pos = workflow.indexOfAction(actions.get(i));
      workflow.removeAction(pos);
      positions[i] = pos;
    }
    hopGui.undoDelegate.addUndoDelete(workflow, actions.toArray(new ActionMeta[0]), positions);

    workflowGraph.updateGui();
  }

  public void deleteAction(WorkflowMeta workflowMeta, ActionMeta action) {
    for (int i = workflowMeta.nrWorkflowHops() - 1; i >= 0; i--) {
      WorkflowHopMeta hi = workflowMeta.getWorkflowHop(i);
      if (hi.getFromAction().equals(action) || hi.getToAction().equals(action)) {
        int idx = workflowMeta.indexOfWorkflowHop(hi);
        hopGui.undoDelegate.addUndoDelete(
            workflowMeta, new WorkflowHopMeta[] {(WorkflowHopMeta) hi.clone()}, new int[] {idx});
        workflowMeta.removeWorkflowHop(idx);
      }
    }

    int pos = workflowMeta.indexOfAction(action);
    workflowMeta.removeAction(pos);
    hopGui.undoDelegate.addUndoDelete(workflowMeta, new ActionMeta[] {action}, new int[] {pos});

    workflowGraph.updateGui();
  }

  public void dupeAction(WorkflowMeta workflowMeta, ActionMeta action) {
    if (action == null) {
      return;
    }

    if (action.isStart()) {
      MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(BaseMessages.getString(PKG, "HopGui.Dialog.OnlyUseStartOnce.Message"));
      mb.setText(BaseMessages.getString(PKG, "HopGui.Dialog.OnlyUseStartOnce.Title"));
      mb.open();
      return;
    }

    ActionMeta copyOfAction = action.clone();  
    Point p = action.getLocation();
    copyOfAction.setLocation(p.x + 10, p.y + 10);

    workflowMeta.addAction(copyOfAction);

    workflowGraph.updateGui();
  }
}
