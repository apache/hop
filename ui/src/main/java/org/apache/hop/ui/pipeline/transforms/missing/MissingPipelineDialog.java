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
package org.apache.hop.ui.pipeline.transforms.missing;

import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class MissingPipelineDialog extends BaseTransformDialog {

  private static final Class<?> PKG = MissingPipelineDialog.class;

  private Shell shellParent;
  private List<Missing> missingPipeline;
  private int mode;
  private String transformResult;

  public static final int MISSING_PIPELINE_TRANSFORMS = 1;
  public static final int MISSING_PIPELINE_TRANSFORM_ID = 2;

  public MissingPipelineDialog(
      Shell parent,
      IVariables variables,
      List<Missing> missingPipeline,
      ITransformMeta baseTransformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, baseTransformMeta, pipelineMeta);
    this.shellParent = parent;
    this.missingPipeline = missingPipeline;
    this.mode = MISSING_PIPELINE_TRANSFORMS;
  }

  public MissingPipelineDialog(
      Shell parent,
      IVariables variables,
      Object in,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, transformName);
    this.shellParent = parent;
    this.mode = MISSING_PIPELINE_TRANSFORM_ID;
  }

  private static String formatMissingEntries(List<Missing> items) {
    StringBuilder entries = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      Missing item = items.get(i);
      entries
          .append("- ")
          .append(item.getTransformName())
          .append(" - ")
          .append(item.getMissingPluginId());
      if (i < items.size() - 1) {
        entries.append("\n");
      } else {
        entries.append("\n\n");
      }
    }
    return entries.toString();
  }

  private String buildMessage() {
    if (mode == MISSING_PIPELINE_TRANSFORMS) {
      return BaseMessages.getString(
          PKG,
          "MissingPipelineDialog.MissingPipelineTransforms",
          formatMissingEntries(missingPipeline));
    }
    return BaseMessages.getString(
        PKG,
        "MissingPipelineDialog.MissingPipelineTransformId",
        ((Missing) baseTransformMeta).getMissingPluginId());
  }

  @Override
  public String open() {
    String message = buildMessage();
    boolean showOpenFile = mode == MISSING_PIPELINE_TRANSFORMS;

    Display display = shellParent.getDisplay();
    Shell dialogShell =
        new Shell(shellParent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON | SWT.APPLICATION_MODAL);
    this.shell = dialogShell;

    PropsUi.setLook(dialogShell);
    dialogShell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginLeft = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    dialogShell.setText(BaseMessages.getString(PKG, "MissingPipelineDialog.MissingPlugins"));
    dialogShell.setLayout(formLayout);

    Label image = new Label(dialogShell, SWT.NONE);
    PropsUi.setLook(image);
    Image icon = display.getSystemImage(SWT.ICON_QUESTION);
    image.setImage(icon);
    FormData imageData = new FormData();
    imageData.left = new FormAttachment(0, 5);
    imageData.right = new FormAttachment(11, 0);
    imageData.top = new FormAttachment(0, 10);
    image.setLayoutData(imageData);

    Label error = new Label(dialogShell, SWT.WRAP);
    PropsUi.setLook(error);
    error.setText(message);
    FormData errorData = new FormData();
    errorData.left = new FormAttachment(image, 5);
    errorData.right = new FormAttachment(100, -5);
    errorData.top = new FormAttachment(0, 10);
    error.setLayoutData(errorData);

    Label separator = new Label(dialogShell, SWT.WRAP);
    PropsUi.setLook(separator);
    FormData separatorData = new FormData();
    separatorData.top = new FormAttachment(error, 10);
    separator.setLayoutData(separatorData);

    Runnable confirm =
        () -> {
          dialogShell.dispose();
          transformResult = transformName;
        };
    Runnable cancel =
        () -> {
          dialogShell.dispose();
          transformResult = null;
        };

    Button closeButton = new Button(dialogShell, SWT.PUSH);
    PropsUi.setLook(closeButton);
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment(98);
    fdClose.top = new FormAttachment(separator);
    closeButton.setLayoutData(fdClose);
    closeButton.setText(BaseMessages.getString(PKG, "MissingPipelineDialog.Close"));
    closeButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancel.run();
          }
        });

    if (showOpenFile) {
      Button openButton = new Button(dialogShell, SWT.PUSH);
      PropsUi.setLook(openButton);
      FormData fdOpen = new FormData();
      fdOpen.right = new FormAttachment(closeButton, -5);
      fdOpen.bottom = new FormAttachment(closeButton, 0, SWT.BOTTOM);
      openButton.setLayoutData(fdOpen);
      openButton.setText(BaseMessages.getString(PKG, "MissingPipelineDialog.OpenFile"));
      openButton.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              confirm.run();
            }
          });
    }

    BaseDialog.defaultShellHandling(dialogShell, v -> confirm.run(), v -> cancel.run());
    return transformResult;
  }
}
