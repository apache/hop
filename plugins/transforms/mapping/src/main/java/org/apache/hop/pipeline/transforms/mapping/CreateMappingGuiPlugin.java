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

package org.apache.hop.pipeline.transforms.mapping;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.pipeline.extension.PipelineRenamedExtension;
import org.apache.hop.ui.hopgui.file.shared.ReferencedConnectionSaveValidator;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

@GuiPlugin
public class CreateMappingGuiPlugin {

  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CREATE_MAPPING =
      "pipeline-graph-transform-10900-create-mapping";

  private static final Class<?> PKG = CreateMappingGuiPlugin.class;

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CREATE_MAPPING,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::CreateMapping.Action.Name",
      tooltip = "i18n::CreateMapping.Action.Tooltip",
      image = "MAP.svg",
      category =
          "i18n:org.apache.hop.ui.hopgui.file.pipeline:HopGuiPipelineGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void createMapping(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    if (!HopSecurityUi.check(Permission.FILE_SAVE) || !HopSecurityUi.check(Permission.FILE_EDIT)) {
      return;
    }

    PipelineMeta pipelineMeta = context.getPipelineMeta();
    HopGuiPipelineGraph pipelineGraph = context.getPipelineGraph();
    IVariables variables = pipelineGraph.getVariables();
    Shell shell = hopGui.getShell();

    List<TransformMeta> selected =
        CreateMappingFromSelection.resolveSelectedTransforms(
            pipelineMeta, context.getTransformMeta());
    CreateMappingFromSelection.Result result =
        CreateMappingFromSelection.analyze(pipelineMeta, selected);
    if (!result.isValid()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "CreateMapping.Error.Title"));
      box.setMessage(result.getValidationError());
      box.open();
      return;
    }

    try {
      String filename = askSaveFilename(shell, pipelineMeta, result.getEntry(), variables);
      if (StringUtils.isEmpty(filename)) {
        return;
      }
      if (!filename.toLowerCase().endsWith(PipelineMeta.PIPELINE_EXTENSION)) {
        filename = filename + PipelineMeta.PIPELINE_EXTENSION;
      }
      // Keep the filename the dialog handed us: the HopGuiFileOpenedDialog extension point has
      // already replaced every project path variable in it. Resolve only for the VFS write.
      String resolvedFilename =
          CreateMappingFromSelection.resolveFilesystemPath(filename, variables);
      if (StringUtils.isEmpty(resolvedFilename)) {
        MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        box.setText(BaseMessages.getString(PKG, "CreateMapping.Error.Title"));
        box.setMessage(BaseMessages.getString(PKG, "CreateMapping.Error.UnresolvedPath"));
        box.open();
        return;
      }
      resolvedFilename = HopVfs.normalize(resolvedFilename);
      if (!confirmOverwrite(shell, resolvedFilename, variables)) {
        return;
      }

      PipelineMeta mappingPipeline = result.getMappingPipeline();
      IHopMetadataProvider saveMetadataProvider = mappingPipeline.getMetadataProvider();
      if (saveMetadataProvider == null) {
        saveMetadataProvider = hopGui.getMetadataProvider();
      }
      if (!ReferencedConnectionSaveValidator.confirmSave(
          shell, mappingPipeline, variables, saveMetadataProvider)) {
        return;
      }

      mappingPipeline.setFilename(resolvedFilename);
      mappingPipeline.setModifiedHopVersion(Const.NVL(Const.getHopVersion(), ""));
      writePipeline(mappingPipeline, variables, hopGui.getLog());

      AuditManager.registerEvent(HopNamespace.getNamespace(), "file", resolvedFilename, "save");
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          variables,
          HopExtensionPoint.PipelineAfterSaveAs.id,
          new PipelineRenamedExtension(mappingPipeline, null));

      ExplorerPerspective explorer = HopGui.getExplorerPerspective();
      if (explorer != null) {
        explorer.refresh();
      }

      pipelineGraph.markUndoPoint();
      CreateMappingFromSelection.replaceSelection(pipelineMeta, result, filename);
      pipelineMeta.setChanged();
      pipelineGraph.updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CreateMapping.Error.Save.Title"),
          BaseMessages.getString(PKG, "CreateMapping.Error.Save.Message"),
          e);
    }
  }

  private String askSaveFilename(
      Shell shell, PipelineMeta pipelineMeta, TransformMeta entry, IVariables variables)
      throws Exception {
    HopPipelineFileType<PipelineMeta> fileType = new HopPipelineFileType<>();
    String suggested = CreateMappingFromSelection.suggestFilename(pipelineMeta, entry, variables);
    FileObject startFile = null;
    if (StringUtils.isNotEmpty(suggested)) {
      startFile = HopVfs.getFileObject(suggested, variables);
    }
    return BaseDialog.presentFileDialog(
        true,
        shell,
        null,
        startFile,
        fileType.getFilterExtensions(),
        fileType.getFilterNames(),
        true);
  }

  private boolean confirmOverwrite(Shell shell, String filename, IVariables variables)
      throws HopException {
    if (!HopVfs.fileExists(filename, variables)) {
      return true;
    }
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "CreateMapping.Overwrite.Title"));
    box.setMessage(BaseMessages.getString(PKG, "CreateMapping.Overwrite.Message", filename));
    return (box.open() & SWT.YES) != 0;
  }

  private void writePipeline(PipelineMeta mappingPipeline, IVariables variables, ILogChannel log)
      throws Exception {
    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.PipelineBeforeSave.id, mappingPipeline);

    String xml = mappingPipeline.getXml(variables);
    try (OutputStream out =
        HopVfs.getOutputStream(mappingPipeline.getFilename(), false, variables)) {
      out.write(XmlHandler.getXmlHeader(Const.UTF_8).getBytes(StandardCharsets.UTF_8));
      out.write(xml.getBytes(StandardCharsets.UTF_8));
      mappingPipeline.clearChanged();
    }

    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.PipelineAfterSave.id, mappingPipeline);
  }
}
