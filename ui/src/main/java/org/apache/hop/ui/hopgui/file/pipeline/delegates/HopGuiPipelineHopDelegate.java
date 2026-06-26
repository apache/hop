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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.IRowDistribution;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowDistributionPluginType;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.pipeline.dialog.PipelineHopDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

public class HopGuiPipelineHopDelegate {

  private static final Class<?> PKG = HopGui.class;

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;
  private PropsUi props;

  public HopGuiPipelineHopDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    this.props = PropsUi.getInstance();
  }

  public void newHop(PipelineMeta pipelineMeta, TransformMeta fr, TransformMeta to) {
    PipelineHopMeta hi = new PipelineHopMeta(fr, to);

    PipelineHopDialog hd =
        new PipelineHopDialog(hopGui.getActiveShell(), SWT.NONE, hi, pipelineMeta);
    if (hd.open() != null) {
      newHop(pipelineMeta, hi);
    }
  }

  public void newHop(PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta) {
    if (checkIfHopAlreadyExists(pipelineMeta, pipelineHopMeta)) {
      pipelineMeta.addPipelineHop(pipelineHopMeta);
      int idx = pipelineMeta.indexOfPipelineHop(pipelineHopMeta);

      if (!performNewPipelineHopChecks(pipelineMeta, pipelineHopMeta)) {
        // Some error occurred: loops, existing hop, etc.
        // Remove it again...
        //
        pipelineMeta.removePipelineHop(idx);
      } else {
        hopGui.undoDelegate.addUndoNew(
            pipelineMeta,
            new PipelineHopMeta[] {pipelineHopMeta},
            new int[] {pipelineMeta.indexOfPipelineHop(pipelineHopMeta)});
      }

      pipelineGraph.updateGui();
    }
  }

  /**
   * @param pipelineMeta pipeline's meta
   * @param newHop hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean checkIfHopAlreadyExists(PipelineMeta pipelineMeta, PipelineHopMeta newHop) {
    boolean ok = true;
    if (pipelineMeta.findPipelineHop(newHop.getFromTransform(), newHop.getToTransform()) != null) {
      MessageBox mb = new MessageBox(hopGui.getActiveShell(), SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "HopGui.Dialog.HopExists.Message")); // "This hop already exists!"
      mb.setText(BaseMessages.getString(PKG, "HopGui.Dialog.HopExists.Title")); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  /**
   * @param pipelineMeta pipeline's meta
   * @param newHop hop to be checked
   * @return true when the hop was added, false if there was an error
   */
  public boolean performNewPipelineHopChecks(PipelineMeta pipelineMeta, PipelineHopMeta newHop) {
    boolean ok = true;

    if (pipelineMeta.hasLoop(newHop.getToTransform())) {
      MessageBox mb = new MessageBox(hopGui.getActiveShell(), SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Message"));
      mb.setText(BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopCausesLoop.Title"));
      mb.open();
      ok = false;
    }

    if (ok) { // only do the following checks, e.g. checkRowMixingStatically
      // when not looping, otherwise we get a loop with
      // StackOverflow there ;-)
      try {
        if (!newHop.getToTransform().getTransform().excludeFromRowLayoutVerification()) {
          pipelineMeta.checkRowMixingStatically(
              pipelineGraph.getVariables(), newHop.getToTransform(), null);
        }
      } catch (HopRowException re) {
        // Show warning about mixing rows with conflicting layouts...
        checkLayoutOfTransformInput(newHop.getToTransform(), pipelineMeta);
      }

      // Verify copy distribution only if the new hop are not for error handling
      if (!newHop.isErrorHop()) {
        verifyCopyDistribute(pipelineMeta, newHop.getFromTransform());
      }
    }

    return ok;
  }

  public void verifyCopyDistribute(PipelineMeta pipelineMeta, TransformMeta transformMeta) {

    // Count normal hop
    //
    List<PipelineHopMeta> hops = pipelineMeta.findAllPipelineHopFrom(transformMeta);
    int hopCount = 0;
    for (PipelineHopMeta hop : hops) {
      // Ignore hop for error handling
      if (hop.isEnabled() && !hop.isErrorHop()) {
        hopCount++;
      }
    }

    // don't show it for 3 or more hops, by then you should have had the
    // message
    if (hopCount == 2) {
      boolean distributes = transformMeta.getTransform().excludeFromCopyDistributeVerification();
      boolean customDistribution = false;

      if (props.showCopyOrDistributeWarning()
          && !transformMeta.getTransform().excludeFromCopyDistributeVerification()) {
        MessageDialogWithToggle md =
            new MessageDialogWithToggle(
                hopGui.getActiveShell(),
                BaseMessages.getString(PKG, "System.Warning"),
                BaseMessages.getString(
                    PKG,
                    "HopGui.Dialog.CopyOrDistribute.Message",
                    transformMeta.getName(),
                    Integer.toString(hopCount)),
                SWT.ICON_WARNING,
                getRowDistributionLabels(),
                BaseMessages.getString(PKG, "HopGui.Message.Warning.NotShowWarning"),
                !props.showCopyOrDistributeWarning());
        int idx = md.open();
        props.setShowCopyOrDistributeWarning(!md.getToggleState());

        distributes = idx == 0; // first button is "distribute"
      }

      if (distributes) {
        transformMeta.setDistributes(true);
        transformMeta.setRowDistribution(null);
      } else if (customDistribution) {

        IRowDistribution rowDistribution = pipelineGraph.askUserForCustomDistributionMethod();

        transformMeta.setDistributes(true);
        transformMeta.setRowDistribution(rowDistribution);
      } else {
        transformMeta.setDistributes(false);
      }

      pipelineGraph.redraw();
    }
  }

  private String[] getRowDistributionLabels() {
    ArrayList<String> labels = new ArrayList<>();
    labels.add(BaseMessages.getString(PKG, "HopGui.Dialog.CopyOrDistribute.Distribute"));
    labels.add(BaseMessages.getString(PKG, "HopGui.Dialog.CopyOrDistribute.Copy"));
    if (!PluginRegistry.getInstance().getPlugins(RowDistributionPluginType.class).isEmpty()) {
      labels.add(
          BaseMessages.getString(PKG, "HopGui.Dialog.CopyOrDistribute.CustomRowDistribution"));
    }
    return labels.toArray(new String[labels.size()]);
  }

  public void delHop(PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta) {
    int index = pipelineMeta.indexOfPipelineHop(pipelineHopMeta);

    hopGui.undoDelegate.addUndoDelete(
        pipelineMeta, new Object[] {(PipelineHopMeta) pipelineHopMeta.clone()}, new int[] {index});
    pipelineMeta.removePipelineHop(index);

    TransformMeta fromTransformMeta = pipelineHopMeta.getFromTransform();

    TransformMeta beforeFrom = (TransformMeta) fromTransformMeta.clone();
    int indexFrom = pipelineMeta.indexOfTransform(fromTransformMeta);

    TransformMeta toTransformMeta = pipelineHopMeta.getToTransform();
    TransformMeta beforeTo = (TransformMeta) toTransformMeta.clone();
    int indexTo = pipelineMeta.indexOfTransform(toTransformMeta);

    boolean transformFromNeedAddUndoChange =
        fromTransformMeta.getTransform().cleanAfterHopFromRemove(pipelineHopMeta.getToTransform());
    if (fromTransformMeta.getTransform() != null) {
      fromTransformMeta.getTransform().searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    }
    boolean transformToNeedAddUndoChange =
        toTransformMeta.getTransform().cleanAfterHopToRemove(fromTransformMeta);

    if (toTransformMeta.getTransform() != null) {
      toTransformMeta.getTransform().searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    }

    // If this is an error handling hop, disable it
    //
    if (pipelineHopMeta.getFromTransform().isDoingErrorHandling()) {
      TransformErrorMeta transformErrorMeta = fromTransformMeta.getTransformErrorMeta();

      // We can only disable error handling if the target of the hop is the same as the target of
      // the error handling.
      //
      if (transformErrorMeta.getTargetTransform() != null
          && transformErrorMeta.getTargetTransform().equals(pipelineHopMeta.getToTransform())) {

        // Only if the target transform is where the error handling is going to...
        //
        transformErrorMeta.setEnabled(false);
        transformFromNeedAddUndoChange = true;
      }
    }

    // Check remaining hops from 'from' transform after deletion
    //
    List<PipelineHopMeta> fromHops = pipelineMeta.findAllPipelineHopFrom(fromTransformMeta);
    int fromHopCount = 0;
    for (PipelineHopMeta fromHop : fromHops) {
      // Ignore hop for error handling
      if (fromHop.isEnabled() && !fromHop.isErrorHop()) {
        fromHopCount++;
      }
    }

    // If remaining hops is 1, reset distribute/copy settings in the from transform
    if (fromHopCount == 1) {
      fromTransformMeta.setDistributes(true);
    }

    if (transformFromNeedAddUndoChange) {
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new Object[] {beforeFrom},
          new Object[] {fromTransformMeta},
          new int[] {indexFrom});
    }

    if (transformToNeedAddUndoChange) {
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new Object[] {beforeTo},
          new Object[] {toTransformMeta},
          new int[] {indexTo});
    }

    pipelineGraph.redraw();
  }

  public void editHop(PipelineMeta pipelineMeta, PipelineHopMeta pipelineHopMeta) {
    // Backup situation BEFORE edit:
    String name = pipelineHopMeta.toString();
    PipelineHopMeta before = (PipelineHopMeta) pipelineHopMeta.clone();

    PipelineHopDialog hd =
        new PipelineHopDialog(hopGui.getActiveShell(), SWT.NONE, pipelineHopMeta, pipelineMeta);
    if (hd.open() != null) {
      // Backup situation for redo/undo:
      PipelineHopMeta after = (PipelineHopMeta) pipelineHopMeta.clone();
      /* TODO: Create new Undo/Redo system

           addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta
             .indexOfPipelineHop( pipelineHopMeta ) } );
      */

      String newName = pipelineHopMeta.toString();
      if (!name.equalsIgnoreCase(newName)) {
        pipelineGraph.redraw(); // color, nr of copies...
      }
    }
    pipelineGraph.updateGui();
  }

  public void checkLayoutOfTransformInput(
      TransformMeta currentTransformMeta, PipelineMeta pipelineMeta) {
    Shell shell = hopGui.getShell();
    IVariables variables = hopGui.getVariables();
    List<TransformMeta> prevTransforms = pipelineMeta.findPreviousTransforms(currentTransformMeta);

    if (prevTransforms.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Title"));
      box.setMessage(BaseMessages.getString(PKG, "HopGui.LayoutCheck.NoInputs"));
      box.open();
      return;
    } else if (prevTransforms.size() == 1) {
      MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "HopGui.LayoutCheck.OneInput", prevTransforms.get(0).getName()));
      box.open();
      return;
    }

    List<IRowMeta> rowMetas = new ArrayList<>();
    boolean errorOccurred = false;
    for (TransformMeta prev : prevTransforms) {
      try {
        IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, prev);
        rowMetas.add(rowMeta);
      } catch (HopException e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "HopGui.LayoutCheck.Error"),
            BaseMessages.getString(PKG, "HopGui.LayoutCheck.ErrorGettingRowLayout", prev.getName()),
            e);
        errorOccurred = true;
        break;
      }
    }
    if (errorOccurred) {
      return;
    }

    boolean allIdentical = true;
    IRowMeta firstMeta = rowMetas.get(0);
    for (int i = 1; i < rowMetas.size(); i++) {
      if (!pipelineGraph.isLayoutIdentical(firstMeta, rowMetas.get(i))) {
        allIdentical = false;
        break;
      }
    }

    if (allIdentical) {
      MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Title"));
      box.setMessage(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Identical"));
      box.open();
    } else {
      StringBuilder diffText = new StringBuilder();
      diffText
          .append(BaseMessages.getString(PKG, "HopGui.LayoutCheck.DifferentInputs"))
          .append(Const.CR)
          .append(Const.CR);

      for (int i = 0; i < prevTransforms.size(); i++) {
        TransformMeta prev = prevTransforms.get(i);
        IRowMeta rowMeta = rowMetas.get(i);
        diffText
            .append(
                BaseMessages.getString(
                    PKG, "HopGui.LayoutCheck.InputFromTransform", prev.getName()))
            .append(Const.CR);
        if (rowMeta == null || rowMeta.isEmpty()) {
          diffText
              .append(BaseMessages.getString(PKG, "HopGui.LayoutCheck.NoFields"))
              .append(Const.CR);
        } else {
          for (int j = 0; j < rowMeta.size(); j++) {
            IValueMeta vm = rowMeta.getValueMeta(j);
            diffText
                .append(
                    BaseMessages.getString(
                        PKG,
                        "HopGui.LayoutCheck.FieldLine",
                        String.format("%3d", j + 1),
                        vm.getName(),
                        vm.getTypeDesc()))
                .append(Const.CR);
          }
        }
        diffText.append("\n");
      }

      diffText
          .append(BaseMessages.getString(PKG, "HopGui.LayoutCheck.DifferencesList"))
          .append(Const.CR);
      String firstTransformName = prevTransforms.get(0).getName();
      for (int i = 1; i < rowMetas.size(); i++) {
        IRowMeta currentMeta = rowMetas.get(i);
        String currentTransformName = prevTransforms.get(i).getName();

        if (firstMeta == null || currentMeta == null) {
          diffText
              .append(
                  BaseMessages.getString(
                      PKG,
                      "HopGui.LayoutCheck.LayoutIsNull",
                      firstTransformName,
                      currentTransformName))
              .append(Const.CR);
          continue;
        }

        if (firstMeta.size() != currentMeta.size()) {
          diffText
              .append(
                  BaseMessages.getString(
                      PKG,
                      "HopGui.LayoutCheck.FieldCountMismatch",
                      firstTransformName,
                      Integer.toString(firstMeta.size()),
                      currentTransformName,
                      Integer.toString(currentMeta.size())))
              .append(Const.CR);
        }

        int minSize = Math.min(firstMeta.size(), currentMeta.size());
        for (int j = 0; j < minSize; j++) {
          IValueMeta vm1 = firstMeta.getValueMeta(j);
          IValueMeta vm2 = currentMeta.getValueMeta(j);
          if (!vm1.getName().equalsIgnoreCase(vm2.getName())) {
            diffText
                .append(
                    BaseMessages.getString(
                        PKG,
                        "HopGui.LayoutCheck.NameMismatch",
                        Integer.toString(j + 1),
                        firstTransformName,
                        vm1.getName(),
                        currentTransformName,
                        vm2.getName()))
                .append(Const.CR);
          } else if (vm1.getType() != vm2.getType()) {
            diffText
                .append(
                    BaseMessages.getString(
                        PKG,
                        "HopGui.LayoutCheck.TypeMismatch",
                        vm1.getName(),
                        firstTransformName,
                        vm1.getTypeDesc(),
                        currentTransformName,
                        vm2.getTypeDesc()))
                .append(Const.CR);
          }
        }

        if (firstMeta.size() > currentMeta.size()) {
          for (int j = minSize; j < firstMeta.size(); j++) {
            diffText
                .append(
                    BaseMessages.getString(
                        PKG,
                        "HopGui.LayoutCheck.FieldMissing",
                        firstMeta.getValueMeta(j).getName(),
                        firstTransformName,
                        currentTransformName))
                .append(Const.CR);
          }
        } else if (currentMeta.size() > firstMeta.size()) {
          for (int j = minSize; j < currentMeta.size(); j++) {
            diffText
                .append(
                    BaseMessages.getString(
                        PKG,
                        "HopGui.LayoutCheck.FieldMissing",
                        currentMeta.getValueMeta(j).getName(),
                        currentTransformName,
                        firstTransformName))
                .append(Const.CR);
          }
        }
      }

      EnterTextDialog dialog =
          new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.MismatchTitle"),
              BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.MismatchMessage"),
              diffText.toString(),
              true);
      dialog.setReadOnly();
      dialog.open();
    }

    if ("Dummy".equals(currentTransformMeta.getPluginId())) {
      MessageBox box = new MessageBox(shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      box.setText(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.ReplaceDummyTitle"));
      box.setMessage(BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.ReplaceDummyMessage"));
      if (box.open() == SWT.YES) {
        try {
          IPlugin plugin =
              PluginRegistry.getInstance()
                  .findPluginWithId(TransformPluginType.class, "StreamSchema");
          if (plugin != null) {
            ITransformMeta streamSchemaMeta =
                (ITransformMeta) PluginRegistry.getInstance().loadClass(plugin);

            configureSchemaMergeTransform(streamSchemaMeta, prevTransforms);
            streamSchemaMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

            TransformMeta before = (TransformMeta) currentTransformMeta.clone();

            currentTransformMeta.setTransform(streamSchemaMeta);
            currentTransformMeta.setTransformPluginId("StreamSchema");
            currentTransformMeta.setChanged();

            TransformMeta after = (TransformMeta) currentTransformMeta.clone();

            hopGui.undoDelegate.addUndoChange(
                pipelineMeta,
                new TransformMeta[] {before},
                new TransformMeta[] {after},
                new int[] {pipelineMeta.indexOfTransform(currentTransformMeta)});

            pipelineMeta.setChanged();
            pipelineGraph.redraw();
          } else {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "HopGui.LayoutCheck.Error"),
                BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.StreamSchemaPluginNotFound"),
                new HopException("Plugin not found"));
          }
        } catch (Exception e) {
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "HopGui.LayoutCheck.Error"),
              BaseMessages.getString(PKG, "HopGui.LayoutCheck.Dialog.ReplaceDummyError"),
              e);
        }
      }
    }
  }

  private void configureSchemaMergeTransform(
      ITransformMeta streamSchemaMeta, List<TransformMeta> prevTransforms) {
    // Configure the Stream Schema Merge transform with the names of the previous transforms
    try {
      Class<?> metaClass = streamSchemaMeta.getClass();
      ClassLoader pluginClassLoader = metaClass.getClassLoader();
      Class<?> transformToMergeClass =
          Class.forName(
              "org.apache.hop.pipeline.transforms.streamschemamerge.StreamSchemaMeta$TransformToMerge",
              true,
              pluginClassLoader);
      java.lang.reflect.Constructor<?> constructor = transformToMergeClass.getConstructor();
      java.lang.reflect.Method setNameMethod =
          transformToMergeClass.getMethod("setName", String.class);

      List<Object> list = new ArrayList<>();
      for (TransformMeta prev : prevTransforms) {
        Object transformToMerge = constructor.newInstance();
        setNameMethod.invoke(transformToMerge, prev.getName());
        list.add(transformToMerge);
      }

      java.lang.reflect.Method setTransformsToMergeMethod =
          metaClass.getMethod("setTransformsToMerge", List.class);
      setTransformsToMergeMethod.invoke(streamSchemaMeta, list);
    } catch (Exception ex) {
      hopGui.getLog().logError("Error setting transforms to merge via reflection", ex);
    }
  }
}
