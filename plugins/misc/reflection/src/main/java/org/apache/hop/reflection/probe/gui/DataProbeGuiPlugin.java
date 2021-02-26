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
 *
 */

package org.apache.hop.reflection.probe.gui;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.reflection.probe.meta.DataProbeLocation;
import org.apache.hop.reflection.probe.meta.PipelineProbe;
import org.apache.hop.reflection.probe.meta.PipelineProbeEditor;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

import java.util.List;

@GuiPlugin
public class DataProbeGuiPlugin {

  @GuiContextAction(
      id = "pipeline-graph-transform-9000-add-probe",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "Add data probe",
      tooltip = "Streams the rows to a pipeline defined in a selected pipeline probe",
      image = "ui/images/data.svg",
      category = "Preview",
      categoryOrder = "3")
  public void addDataProbeForTransform(HopGuiPipelineTransformContext context) {
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    HopGui hopGui = HopGui.getInstance();

    try {
      // Present the user with a list of pipeline probes...
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      IHopMetadataSerializer<PipelineProbe> serializer =
          metadataProvider.getSerializer(PipelineProbe.class);
      MetadataManager<PipelineProbe> manager =
          new MetadataManager<>(hopGui.getVariables(), metadataProvider, PipelineProbe.class);

      PipelineProbe pipelineProbe = null;
      List<String> pipelineProbeNames = serializer.listObjectNames();
      if (pipelineProbeNames.isEmpty()) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("No probes available");
        box.setMessage(
            "There are no pipeline probe objects defined yet.  Do you want to create one?");
        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          // Create a new pipeline probe...
          //

          pipelineProbe = new PipelineProbe();
          addLocation(hopGui.getVariables(), pipelineProbe, pipelineMeta, transformMeta);
          manager.newMetadata(pipelineProbe);
          return;
        } else {
          return;
        }
      } else {
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                hopGui.getShell(),
                pipelineProbeNames.toArray(new String[0]),
                "Select pipeline probe",
                "Select the pipeline probe to add this pipeline transform to");
        String pipelineProbeName = dialog.open();
        if (pipelineProbeName != null) {
          pipelineProbe = serializer.load(pipelineProbeName);
        }
      }

      if (pipelineProbe != null) {

        // See if it's open in the metadata perspective...
        //
        MetadataPerspective perspective =
            (MetadataPerspective)
                hopGui.getPerspectiveManager().findPerspective(MetadataPerspective.class);
        String key = PipelineProbe.class.getAnnotation(HopMetadata.class).key();
        PipelineProbeEditor editor =
            (PipelineProbeEditor) perspective.findEditor(key, pipelineProbe.getName());
        if (editor != null) {
          // We're going to change the current metadata and flag it as changed...
          //
          pipelineProbe = new PipelineProbe();
          editor.getWidgetsContent( pipelineProbe );

          // Add the location
          //
          addLocation(hopGui.getVariables(), pipelineProbe, pipelineMeta, transformMeta);

          // Replace and refresh the dialog
          //
          editor.setMetadata(pipelineProbe);
          editor.setWidgetsContent();

          // Set changed...
          //
          editor.setChanged();

          // Switch to the editor...
          //
          perspective.activate();

          perspective.setActiveEditor( editor );


          return;
        } else {
          // Not opened in the perspective, simply add the data probe location...
          //
          addLocation(hopGui.getVariables(), pipelineProbe, pipelineMeta, transformMeta);

          // ... and save the pipeline probe
          //
          serializer.save(pipelineProbe);
        }
      }

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error adding pipeline probe to transform '" + transformMeta.getName() + "'",
          e);
    }
  }

  private void addLocation(
      IVariables variables,
      PipelineProbe pipelineProbe,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta)
      throws HopException {

    // Allow our plugins (projects etc) to turn the filename into a relative path...
    //
    String probeFilename = pipelineMeta.getFilename();
    HopGuiFileOpenedExtension ext = new HopGuiFileOpenedExtension(null, variables, probeFilename);

    ExtensionPointHandler.callExtensionPoint(
        LogChannel.UI, variables, HopGuiExtensionPoint.HopGuiFileOpenedDialog.id, ext);

    pipelineProbe
        .getDataProbeLocations()
        .add(new DataProbeLocation(ext.filename, transformMeta.getName()));
  }
}
