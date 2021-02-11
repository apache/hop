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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.IPartitioner;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.partition.PartitionMethodSelector;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.apache.hop.ui.hopgui.partition.processor.IMethodProcessor;
import org.apache.hop.ui.hopgui.partition.processor.MethodProcessorFactory;
import org.apache.hop.ui.pipeline.transform.TransformErrorMetaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineTransformDelegate {

  // TODO: move i18n package to HopGui
  private static final Class<?> PKG = HopGui.class; // For Translator

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;

  public HopGuiPipelineTransformDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public ITransformDialog getTransformDialog(
      ITransformMeta transformMeta, PipelineMeta pipelineMeta, String transformName)
      throws HopException {
    Class<?>[] paramClasses =
        new Class<?>[] {
          Shell.class, IVariables.class, Object.class, PipelineMeta.class, String.class
        };
    Object[] paramArgs =
        new Object[] {
          hopGui.getShell(),
          pipelineGraph.getVariables(),
          transformMeta,
          pipelineMeta,
          transformName
        };

    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(TransformPluginType.class, transformMeta);
    if (plugin == null) {
      throw new HopException("Missing transform plugin for '" + transformName + "'");
    }

    String dialogClassName = plugin.getClassMap().get(ITransformDialog.class);

    if (dialogClassName == null) {
      // Calculate it from the base meta class...
      //
      dialogClassName = transformMeta.getDialogClassName();
    }

    if (dialogClassName == null) {
      throw new HopException(
          "Unable to find dialog class for plugin '"
              + plugin.getIds()[0]
              + "' : "
              + plugin.getName());
    }

    try {
      Class<ITransformDialog> dialogClass = registry.getClass(plugin, dialogClassName);
      Constructor<ITransformDialog> dialogConstructor = dialogClass.getConstructor(paramClasses);
      return dialogConstructor.newInstance(paramArgs);
    } catch (Exception e) {
      // try the old way for compatibility
      try {
        Class<?>[] sig =
            new Class<?>[] {
              Shell.class, IVariables.class, ITransformMeta.class, PipelineMeta.class, String.class
            };
        Method method = transformMeta.getClass().getDeclaredMethod("getDialog", sig);
        if (method != null) {
          hopGui
              .getLog()
              .logDebug(
                  "Use of ITransformMeta#getDialog is deprecated, use PluginDialog annotation instead.");
          return (ITransformDialog) method.invoke(transformMeta, paramArgs);
        }
      } catch (Throwable ignored) {
      }

      String errorTitle =
          BaseMessages.getString(PKG, "HopGui.Dialog.ErrorCreatingTransformDialog.Title");
      String errorMsg =
          BaseMessages.getString(
              PKG, "HopGui.Dialog.ErrorCreatingTransformDialog.Message", dialogClassName);
      new ErrorDialog(hopGui.getShell(), errorTitle, errorMsg, e);
      throw new HopException(e);
    }
  }

  public String editTransform(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    boolean refresh = false;
    String transformName = null;
    try {
      String name = transformMeta.getName();

      // Before we do anything, let's store the situation the way it
      // was...
      //
      TransformMeta before = (TransformMeta) transformMeta.clone();
      ITransformDialog dialog =
          getTransformDialog(transformMeta.getTransform(), pipelineMeta, name);
      if (dialog != null) {
        dialog.setMetadataProvider(hopGui.getMetadataProvider());
        transformName = dialog.open();
      }

      if (!Utils.isEmpty(transformName)) {
        // Force the recreation of the transform IO metadata object. (cached by default)
        //
        transformMeta.getTransform().resetTransformIoMeta();

        //
        // See if the new name the user enter, doesn't collide with
        // another transform.
        // If so, change the transformName and warn the user!
        //
        String newname = transformName;
        TransformMeta smeta = pipelineMeta.findTransform(newname, transformMeta);
        int nr = 2;
        while (smeta != null) {
          newname = transformName + " " + nr;
          smeta = pipelineMeta.findTransform(newname);
          nr++;
        }
        if (nr > 2) {
          transformName = newname;
          MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(
              BaseMessages.getString(
                  PKG, "HopGui.Dialog.TransformnameExists.Message", transformName));
          mb.setText(BaseMessages.getString(PKG, "HopGui.Dialog.TransformnameExists.Title"));
          mb.open();
        }

        if (!transformName.equals(name)) {
          refresh = true;
        }

        TransformMeta newTransformMeta = (TransformMeta) transformMeta.clone();
        newTransformMeta.setName(transformName);
        pipelineMeta.clearCaches();
        pipelineMeta.notifyAllListeners(transformMeta, newTransformMeta);
        transformMeta.setName(transformName);

        //
        // OK, so the transform has changed...
        // Backup the situation for undo/redo
        //
        TransformMeta after = (TransformMeta) transformMeta.clone();
        /**
         * TODO: Create new Undo/Redo system hopGui.addUndoChange( pipelineMeta, new TransformMeta[]
         * { before }, new TransformMeta[] { after }, new int[] { pipelineMeta .indexOfTransform(
         * transformMeta ) } );
         */
      }
      pipelineGraph.redraw(); // name is displayed on the graph too.

      // TODO: verify "double pathway" transforms for bug #4365
      // After the transform was edited we can complain about the possible
      // deadlock here.
      //
    } catch (Throwable e) {
      if (hopGui.getShell().isDisposed()) {
        return null;
      }
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "HopGui.Dialog.UnableOpenDialog.Title"),
          BaseMessages.getString(PKG, "HopGui.Dialog.UnableOpenDialog.Message"),
          e);
    }

    return transformName;
  }

  /**
   * Allocate new transform, optionally open and rename it.
   *
   * @param id Id of the new transform
   * @param name Name of the new transform
   * @param description Description of the type of transform
   * @param openit Open the dialog for this transform?
   * @param rename Rename this transform?
   * @return The newly created TransformMeta object.
   */
  public TransformMeta newTransform(
      PipelineMeta pipelineMeta,
      String id,
      String name,
      String description,
      boolean openit,
      boolean rename,
      Point location) {
    try {
      TransformMeta transformMeta = null;

      // See if we need to rename the transform to avoid doubles!
      if (rename && pipelineMeta.findTransform(name) != null) {
        int i = 2;
        String newName = name + " " + i;
        while (pipelineMeta.findTransform(newName) != null) {
          i++;
          newName = name + " " + i;
        }
        name = newName;
      }

      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin transformPlugin =
          id != null
              ? registry.findPluginWithId(TransformPluginType.class, id)
              : registry.findPluginWithName(TransformPluginType.class, description);

      try {
        if (transformPlugin != null) {
          ITransformMeta info = (ITransformMeta) registry.loadClass(transformPlugin);

          info.setDefault();

          transformMeta = new TransformMeta(transformPlugin.getIds()[0], name, info);

          if (name != null) {
            // OK pressed in the dialog: we have a transform-name
            String newName = name;
            TransformMeta candiateTransformMeta = pipelineMeta.findTransform(newName);
            int nr = 2;
            while (candiateTransformMeta != null) {
              newName = name + " " + nr;
              candiateTransformMeta = pipelineMeta.findTransform(newName);
              nr++;
            }
            if (nr > 2) {
              transformMeta.setName(newName);
              MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
              // "This transformName already exists.  HopGui changed the transformName to
              // ["+newName+"]"
              mb.setMessage(
                  BaseMessages.getString(
                      PKG, "HopGui.Dialog.ChangeTransformname.Message", newName));
              mb.setText(BaseMessages.getString(PKG, "HopGui.Dialog.ChangeTransformname.Title"));
              mb.open();
            }
            PropsUi.setLocation(transformMeta, location.x, location.y);
            transformMeta.setLocation(location.x, location.y);
            pipelineMeta.addTransform(transformMeta);
            hopGui.undoDelegate.addUndoNew(
                pipelineMeta,
                new TransformMeta[] {transformMeta},
                new int[] {pipelineMeta.indexOfTransform(transformMeta)});

            // Also store the event in the plugin history list...
            AuditManager.registerEvent(
                HopNamespace.getNamespace(), "transform", transformPlugin.getIds()[0], "create");

            // See if we need to open the transform
            //
            if (openit) {
              pipelineGraph.editTransform(pipelineMeta, transformMeta);
            }
          } else {
            return null; // Cancel pressed in dialog.
          }
          pipelineGraph.adjustScrolling();
          pipelineGraph.updateGui();
        }
      } catch (HopException e) {
        String filename = transformPlugin.getErrorHelpFile();
        if (!Utils.isEmpty(filename)) {
          // OK, in stead of a normal error message, we give back the
          // content of the error help file... (HTML)
          FileInputStream fis = null;
          try {
            StringBuilder content = new StringBuilder();

            fis = new FileInputStream(new File(filename));
            int ch = fis.read();
            while (ch >= 0) {
              content.append((char) ch);
              ch = fis.read();
            }

            ShowBrowserDialog sbd =
                new ShowBrowserDialog(
                    hopGui.getShell(),
                    BaseMessages.getString(PKG, "HopGui.Dialog.ErrorHelpText.Title"),
                    content.toString());
            sbd.open();
          } catch (Exception ex) {
            new ErrorDialog(
                hopGui.getShell(),
                BaseMessages.getString(PKG, "HopGui.Dialog.ErrorShowingHelpText.Title"),
                BaseMessages.getString(PKG, "HopGui.Dialog.ErrorShowingHelpText.Message"),
                ex);
          } finally {
            if (fis != null) {
              try {
                fis.close();
              } catch (Exception ex) {
                hopGui.getLog().logError("Error closing plugin help file", ex);
              }
            }
          }
        } else {
          new ErrorDialog(
              hopGui.getShell(),
              // "Error creating transform"
              // "I was unable to create a new transform"
              BaseMessages.getString(PKG, "HopGui.Dialog.UnableCreateNewTransform.Title"),
              BaseMessages.getString(PKG, "HopGui.Dialog.UnableCreateNewTransform.Message"),
              e);
        }
        return null;
      } catch (Throwable e) {
        if (!hopGui.getShell().isDisposed()) {
          new ErrorDialog(
              hopGui.getShell(),
              // "Error creating transform"
              BaseMessages.getString(PKG, "HopGui.Dialog.ErrorCreatingTransform.Title"),
              BaseMessages.getString(PKG, "HopGui.Dialog.UnableCreateNewTransform.Message"),
              e);
        }
        return null;
      }

      return transformMeta;
    } finally {
      pipelineGraph.redraw();
    }
  }

  public void editTransformPartitioning(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    String[] schemaNames;
    try {
      schemaNames = hopGui.partitionManager.getNamesArray();
    } catch (HopException e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "HopGui.ErrorDialog.Title"),
          BaseMessages.getString(
              PKG, "HopGui.ErrorDialog.ErrorFetchingFromRepo.PartitioningSchemas"),
          e);
      return;
    }
    try {
      /*Check if Partition schema has already defined*/
      if (isDefinedSchemaExist(schemaNames)) {

        /*Prepare settings for Method selection*/
        PluginRegistry registry = PluginRegistry.getInstance();
        List<IPlugin> plugins = registry.getPlugins(PartitionerPluginType.class);
        int exactSize = TransformPartitioningMeta.methodDescriptions.length + plugins.size();
        PartitionSettings partitionSettings =
            new PartitionSettings(exactSize, pipelineMeta, transformMeta, hopGui.partitionManager);
        partitionSettings.fillOptionsAndCodesByPlugins(plugins);

        /*Method selection*/
        PartitionMethodSelector methodSelector = new PartitionMethodSelector();
        String partitionMethodDescription =
            methodSelector.askForPartitionMethod(hopGui.getShell(), partitionSettings);
        if (!StringUtil.isEmpty(partitionMethodDescription)) {
          String method =
              partitionSettings.getMethodByMethodDescription(partitionMethodDescription);
          int methodType = TransformPartitioningMeta.getMethodType(method);

          partitionSettings.updateMethodType(methodType);
          partitionSettings.updateMethod(method);

          /*Schema selection*/
          IMethodProcessor methodProcessor = MethodProcessorFactory.create(methodType);
          methodProcessor.schemaSelection(
              partitionSettings,
              hopGui.getShell(),
              (shell, settings) -> {
                TransformPartitioningMeta partitioningMeta =
                    settings.getTransformMeta().getTransformPartitioningMeta();
                ITransformDialog dialog =
                    getPartitionerDialog(
                        shell,
                        pipelineGraph.getVariables(),
                        settings.getTransformMeta(),
                        partitioningMeta,
                        settings.getPipelineMeta());
                return dialog.open();
              });
        }
        transformMeta.setChanged();
        hopGui.undoDelegate.addUndoChange(
            partitionSettings.getPipelineMeta(),
            new TransformMeta[] {partitionSettings.getBefore()},
            new TransformMeta[] {partitionSettings.getAfter()},
            new int[] {
              partitionSettings
                  .getPipelineMeta()
                  .indexOfTransform(partitionSettings.getTransformMeta())
            });
        pipelineGraph.redraw();
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "There was an unexpected error while editing the partitioning method specifics:",
          e);
    }
  }

  public boolean isDefinedSchemaExist(String[] schemaNames) {
    // Before we start, check if there are any partition schemas defined...
    if ((schemaNames == null) || (schemaNames.length == 0)) {
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_ERROR | SWT.OK);
      box.setText("Create a partition schema");
      box.setMessage(
          "You first need to create one or more partition schemas before you can select one!");
      box.open();
      return false;
    }
    return true;
  }

  public ITransformDialog getPartitionerDialog(
      Shell shell,
      IVariables variables,
      TransformMeta transformMeta,
      TransformPartitioningMeta partitioningMeta,
      PipelineMeta pipelineMeta)
      throws HopException {
    IPartitioner partitioner = partitioningMeta.getPartitioner();
    String dialogClassName = partitioner.getDialogClassName();

    Class<?> dialogClass;
    Class<?>[] paramClasses =
        new Class<?>[] {
          Shell.class,
          IVariables.class,
          TransformMeta.class,
          TransformPartitioningMeta.class,
          PipelineMeta.class
        };
    Object[] paramArgs =
        new Object[] {shell, variables, transformMeta, partitioningMeta, pipelineMeta};
    Constructor<?> dialogConstructor;
    try {
      dialogClass = partitioner.getClass().getClassLoader().loadClass(dialogClassName);
      dialogConstructor = dialogClass.getConstructor(paramClasses);
      return (ITransformDialog) dialogConstructor.newInstance(paramArgs);
    } catch (Exception e) {
      throw new HopException("Unable to open dialog of partitioning method", e);
    }
  }

  public void editTransformErrorHandling(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    if (transformMeta != null && transformMeta.supportsErrorHandling()) {
      TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
      if (transformErrorMeta == null) {
        transformErrorMeta = new TransformErrorMeta(transformMeta);
      }
      List<TransformMeta> targetTransforms = pipelineMeta.findNextTransforms(transformMeta);

      // now edit this transformErrorMeta object:
      TransformErrorMetaDialog dialog =
          new TransformErrorMetaDialog(
              hopGui.getShell(),
              pipelineGraph.getVariables(),
              transformErrorMeta,
              pipelineMeta,
              targetTransforms);
      if (dialog.open()) {
        transformMeta.setTransformErrorMeta(transformErrorMeta);
        transformMeta.setChanged();
        pipelineGraph.redraw();
      }
    }
  }

  public void delTransforms(PipelineMeta pipelineMeta, List<TransformMeta> transforms) {
    if (transforms == null || transforms.size() == 0) {
      return; // nothing to do
    }
    try {
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          pipelineGraph.getVariables(),
          HopExtensionPoint.PipelineBeforeDeleteTransforms.id,
          transforms);
    } catch (HopException e) {
      return;
    }

    // Hops belonging to the deleting transforms are placed in a single transaction and removed.
    List<PipelineHopMeta> pipelineHops = new ArrayList<>();
    int[] hopIndexes = new int[pipelineMeta.nrPipelineHops()];
    int hopIndex = 0;
    for (int i = pipelineMeta.nrPipelineHops() - 1; i >= 0; i--) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop(i);
      for (int j = 0; j < transforms.size() && hopIndex < hopIndexes.length; j++) {
        if (hi.getFromTransform().equals(transforms.get(j))
            || hi.getToTransform().equals(transforms.get(j))) {
          int idx = pipelineMeta.indexOfPipelineHop(hi);
          pipelineHops.add((PipelineHopMeta) hi.clone());
          hopIndexes[hopIndex] = idx;
          pipelineMeta.removePipelineHop(idx);
          hopIndex++;
          break;
        }
      }
    }
    if (!pipelineHops.isEmpty()) {
      PipelineHopMeta[] hops = pipelineHops.toArray(new PipelineHopMeta[pipelineHops.size()]);
      hopGui.undoDelegate.addUndoDelete(pipelineMeta, hops, hopIndexes);
    }

    // Deleting transforms are placed all in a single transaction and removed.
    int[] positions = new int[transforms.size()];
    for (int i = 0; i < transforms.size(); i++) {
      int pos = pipelineMeta.indexOfTransform(transforms.get(i));
      pipelineMeta.removeTransform(pos);
      positions[i] = pos;
    }
    hopGui.undoDelegate.addUndoDelete(
        pipelineMeta, transforms.toArray(new TransformMeta[0]), positions);

    pipelineGraph.redraw();
  }

  public void delTransform(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    delTransforms(pipelineMeta, Arrays.asList(transformMeta));
  }
}
