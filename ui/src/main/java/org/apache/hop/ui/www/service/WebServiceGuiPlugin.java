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

package org.apache.hop.ui.www.service;

import java.util.List;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.www.service.WebService;
import org.eclipse.swt.SWT;

@GuiPlugin
public class WebServiceGuiPlugin {
  private static final Class<?> PKG = WebServiceGuiPlugin.class;

  @GuiContextAction(
      id = "pipeline-graph-transform-9000-add-web-serviec",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::WebserviceGuiPlugin.GuiAction.Label",
      tooltip = "i18n::WebserviceGuiPlugin.GuiAction.ToolTip",
      image = "ui/images/webservice.svg",
      category = "Data routing",
      categoryOrder = "2")
  public void addWebServiceForTransform(HopGuiPipelineTransformContext context) {
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();
    IVariables variables = context.getPipelineGraph().getVariables();

    HopGui hopGui = HopGui.getInstance();

    try {
      // Ask which field should be used...
      //
      IRowMeta fields = pipelineMeta.getTransformFields(variables, transformMeta);
      EnterSelectionDialog fieldSelectionDialog =
          new EnterSelectionDialog(
              hopGui.getShell(),
              fields.getFieldNames(),
              BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.SelectOutputField.Label"),
              BaseMessages.getString(
                  PKG, "WebserviceGuiPlugin.GuiAction.SelectOutputField.Description"));
      String fieldName = fieldSelectionDialog.open();
      if (fieldName == null) {
        return;
      }

      // Present the user with a list of pipeline probes...
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      IHopMetadataSerializer<WebService> serializer =
          metadataProvider.getSerializer(WebService.class);
      MetadataManager<WebService> manager =
          new MetadataManager<>(
              hopGui.getVariables(), metadataProvider, WebService.class, hopGui.getShell());

      WebService webService = null;
      List<String> serviceNames = serializer.listObjectNames();
      if (serviceNames.isEmpty()) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.NoService.Label"));
        box.setMessage(
            BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.NoService.Description"));
        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          // Create a new web service...
          //
          webService =
              new WebService(
                  pipelineMeta.getName(),
                  true,
                  pipelineMeta.getFilename(),
                  transformMeta.getName(),
                  fieldName,
                  "text/plain",
                  null,
                  false,
                  null,
                  null,
                  null);
          manager.newMetadata(webService);
          return;
        } else {
          return;
        }
      } else {
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                hopGui.getShell(),
                serviceNames.toArray(new String[0]),
                BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.SelectService.Label"),
                BaseMessages.getString(
                    PKG, "WebserviceGuiPlugin.GuiAction.SelectService.Description"));
        String pipelineProbeName = dialog.open();
        if (pipelineProbeName != null) {
          webService = serializer.load(pipelineProbeName);
        }
      }

      if (webService != null) {

        // See if it's open in the metadata perspective...
        //
        MetadataPerspective perspective =
            (MetadataPerspective)
                hopGui.getPerspectiveManager().findPerspective(MetadataPerspective.class);
        String key = WebService.class.getAnnotation(HopMetadata.class).key();
        WebServiceEditor editor =
            (WebServiceEditor) perspective.findEditor(key, webService.getName());
        if (editor != null) {
          // We're going to change the current metadata and flag it as changed...
          //
          webService = new WebService();
          editor.getWidgetsContent(webService);

          // Update the web service details
          //
          webService.setFilename(pipelineMeta.getFilename());
          webService.setTransformName(transformMeta.getName());
          webService.setFieldName(fieldName);

          // Replace and refresh the dialog
          //
          editor.setMetadata(webService);
          editor.setWidgetsContent();

          // Set changed...
          //
          editor.setChanged();

          // Switch to the editor...
          //
          perspective.activate();

          perspective.setActiveEditor(editor);
        } else {
          // Not opened in the perspective, simply set the web service details
          //
          webService.setFilename(pipelineMeta.getFilename());
          webService.setTransformName(transformMeta.getName());
          webService.setFieldName(fieldName);

          // ... and save the pipeline probe
          //
          serializer.save(webService);
        }
      }

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.ErrorDialog.Label"),
          BaseMessages.getString(PKG, "WebserviceGuiPlugin.GuiAction.ErrorDialog.Description")
              + " '"
              + transformMeta.getName()
              + "'",
          e);
    }
  }
}
