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

package org.apache.hop.ui.hopgui.dialog;

import java.lang.reflect.InvocationTargetException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.schema.HopXmlSchemaExportOptions;
import org.apache.hop.schema.HopXmlSchemaExportResult;
import org.apache.hop.schema.HopXmlSchemaService;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Dialog for exporting XML schemas for pipelines, workflows, transforms, and actions. */
public class XmlSchemaExportDialog extends Dialog {
  private static final Class<?> PKG = XmlSchemaExportDialog.class;

  private final IVariables variables;
  private final XmlSchemaExportDialogModel model;
  private GuiCompositeWidgets widgets;
  private Shell shell;
  private boolean cancelled = true;
  private HopXmlSchemaExportResult result;

  public XmlSchemaExportDialog(Shell parent, IVariables variables) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.model = new XmlSchemaExportDialogModel();
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Shell.Title"));
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
    wlHeader.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Header"));
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(0, 0);
    fdHeader.top = new FormAttachment(0, 0);
    fdHeader.right = new FormAttachment(100, 0);
    wlHeader.setLayoutData(fdHeader);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wlHeader,
            wOk,
            XmlSchemaExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID,
            model);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return !cancelled;
  }

  private void ok() {
    try {
      widgets.getWidgetsContents(model, XmlSchemaExportDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID);

      if (StringUtils.isBlank(model.getTargetFolder())) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        mb.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.NoFolder.Title"));
        mb.setMessage(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.NoFolder.Message"));
        mb.open();
        return;
      }

      String resolvedFolder = variables.resolve(model.getTargetFolder());
      FileObject folderObject = HopVfs.getFileObject(resolvedFolder, variables);

      HopXmlSchemaExportOptions options = model.toExportOptions();

      IRunnableWithProgress op =
          monitor -> {
            try {
              result =
                  HopXmlSchemaService.getInstance()
                      .exportAllSchemas(folderObject, options, monitor);
            } catch (Exception e) {
              throw new InvocationTargetException(
                  e, "Error exporting XML schemas: " + e.getMessage());
            }
          };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(true, op);

      cancelled = false;
      shell.dispose();

      if (result != null) {
        StringBuilder msg = new StringBuilder();
        msg.append(
            BaseMessages.getString(
                PKG,
                "XmlSchemaExportDialog.Result.Summary",
                Integer.toString(result.getGeneratedFiles().size()),
                folderObject.getName().getFriendlyURI()));
        msg.append(Const.CR).append(Const.CR);
        msg.append(
            BaseMessages.getString(
                PKG,
                "XmlSchemaExportDialog.Result.Details",
                result.isPipelineSchemaGenerated() ? "1" : "0",
                result.isWorkflowSchemaGenerated() ? "1" : "0",
                Integer.toString(result.getTransformSchemasCount()),
                Integer.toString(result.getActionSchemasCount())));

        if (!result.getWarnings().isEmpty()) {
          msg.append(Const.CR).append(Const.CR);
          msg.append(
              BaseMessages.getString(
                  PKG,
                  "XmlSchemaExportDialog.Result.Warnings",
                  Integer.toString(result.getWarnings().size())));
        }

        Shell parentShell = HopGui.getInstance() != null ? HopGui.getInstance().getShell() : null;
        if (parentShell != null && !parentShell.isDisposed()) {
          MessageBox mb = new MessageBox(parentShell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setText(BaseMessages.getString(PKG, "XmlSchemaExportDialog.Result.Title"));
          mb.setMessage(msg.toString());
          mb.open();
        }
      }
    } catch (InvocationTargetException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.Title"),
          BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.Message"),
          e.getTargetException());
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.Title"),
          BaseMessages.getString(PKG, "XmlSchemaExportDialog.Error.Message"),
          e);
    }
  }

  private void cancel() {
    cancelled = true;
    shell.dispose();
  }
}
