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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.ExportContext;
import org.apache.hop.core.diagram.IDiagramExporter;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog for exporting visual diagrams to SVG, Mermaid, etc. Uses the grouped {@link
 * GuiCompositeWidgets} pattern.
 */
public class DiagramExportDialog extends Dialog {
  private static final Class<?> PKG = DiagramExportDialog.class;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final DiagramExportDialogModel model;
  private Shell shell;
  private GuiCompositeWidgets widgets;
  private boolean cancelled = true;
  private boolean syncingFilename;

  public DiagramExportDialog(
      Shell parent,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Object subject,
      String defaultFilename) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.model = new DiagramExportDialogModel(subject, defaultFilename);
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Shell.Title"));
    if (parent.getImage() != null) {
      shell.setImage(parent.getImage());
    }

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    Label wlHeader = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHeader);
    wlHeader.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Header"));
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(0, 0);
    fdHeader.top = new FormAttachment(0, 0);
    fdHeader.right = new FormAttachment(100, 0);
    wlHeader.setLayoutData(fdHeader);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wlHeader,
            wOk,
            DiagramExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID,
            model);

    listenForFormatChanges();
    replaceFilenameBrowseWithSaveDialog();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return !cancelled;
  }

  private void listenForFormatChanges() {
    Control control = widgets.getWidgetsMap().get(DiagramExportDialogModel.WIDGET_FORMAT);
    if (control instanceof Combo combo) {
      combo.addListener(SWT.Selection, e -> syncFilenameExtension());
    }
  }

  private void replaceFilenameBrowseWithSaveDialog() {
    Control browse = widgets.getActionWidgetsMap().get(DiagramExportDialogModel.WIDGET_FILENAME);
    Control filenameControl = widgets.getWidgetsMap().get(DiagramExportDialogModel.WIDGET_FILENAME);
    if (!(browse instanceof Button button) || !(filenameControl instanceof TextVar textVar)) {
      return;
    }
    for (Listener listener : button.getListeners(SWT.Selection)) {
      button.removeListener(SWT.Selection, listener);
    }
    button.addListener(SWT.Selection, e -> browseTargetFile(textVar));
  }

  private void browseTargetFile(TextVar textVar) {
    try {
      widgets.getWidgetsContents(model, DiagramExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID);
      IDiagramExporter<?> exporter = model.getSelectedExporter();
      String extension = exporter != null ? exporter.getFileExtension() : "svg";
      String[] filterExtensions = new String[] {"*." + extension};
      String[] filterNames = exporter != null ? exporter.getFileFilterNames() : new String[0];
      if (filterNames == null || filterNames.length == 0) {
        filterNames =
            new String[] {
              exporter != null && exporter.getFormat() != null
                  ? exporter.getFormat().getName()
                  : extension
            };
      }

      FileObject proposed = null;
      if (StringUtils.isNotBlank(model.getFilename())) {
        proposed = HopVfs.getFileObject(variables.resolve(model.getFilename()));
      }
      String filename =
          BaseDialog.presentFileDialog(
              true, shell, textVar, variables, proposed, filterExtensions, filterNames, true);
      if (StringUtils.isNotEmpty(filename)) {
        model.setFilename(HopVfs.separatorsToUnix(filename));
        model.updateExtensionForSelectedFormat();
        textVar.setText(Const.NVL(model.getFilename(), ""));
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DiagramExportDialog.Error.Title"),
          BaseMessages.getString(PKG, "DiagramExportDialog.Error.Message"),
          e);
    }
  }

  private void syncFilenameExtension() {
    if (syncingFilename) {
      return;
    }
    syncingFilename = true;
    try {
      widgets.getWidgetsContents(model, DiagramExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID);
      model.updateExtensionForSelectedFormat();
      Control filenameControl =
          widgets.getWidgetsMap().get(DiagramExportDialogModel.WIDGET_FILENAME);
      if (filenameControl instanceof TextVar textVar) {
        textVar.setText(Const.NVL(model.getFilename(), ""));
      } else if (filenameControl instanceof Text text) {
        text.setText(Const.NVL(model.getFilename(), ""));
      }
    } finally {
      syncingFilename = false;
    }
  }

  private void ok() {
    try {
      widgets.getWidgetsContents(model, DiagramExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID);

      if (StringUtils.isBlank(model.getFilename())) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        mb.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Error.NoFile.Title"));
        mb.setMessage(BaseMessages.getString(PKG, "DiagramExportDialog.Error.NoFile.Message"));
        mb.open();
        return;
      }

      String resolvedFilename = variables.resolve(model.getFilename());
      FileObject fileObject = HopVfs.getFileObject(resolvedFilename);
      if (fileObject.exists()) {
        MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        mb.setText(BaseMessages.getString(PKG, "DiagramExportDialog.Warning.FileExists.Title"));
        mb.setMessage(
            BaseMessages.getString(PKG, "DiagramExportDialog.Warning.FileExists.Message"));
        if ((mb.open() & SWT.YES) == 0) {
          return;
        }
      }

      IExportContext context =
          new ExportContext(
              variables,
              metadataProvider,
              new LogChannel("DiagramExportDialog"),
              IExportContext.ExportEnvironment.GUI);

      DiagramExportResult result =
          DiagramExportService.getInstance().export(model.getSubject(), model.toOptions(), context);

      if (!result.isSuccess()) {
        throw new HopException(result.getErrorMessage(), result.getException());
      }

      cancelled = false;
      shell.dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DiagramExportDialog.Error.Title"),
          BaseMessages.getString(PKG, "DiagramExportDialog.Error.Message"),
          e);
    }
  }

  private void cancel() {
    cancelled = true;
    shell.dispose();
  }
}
