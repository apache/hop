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

package org.apache.hop.pipeline.transforms.webserviceavailable;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class WebServiceAvailableDialog extends BaseTransformDialog {
  private static final Class<?> PKG = WebServiceAvailableMeta.class;

  private CCombo wURL;

  private TextVar wResult;

  private TextVar wConnectTimeOut;

  private TextVar wReadTimeOut;

  private final WebServiceAvailableMeta input;

  private boolean gotPreviousFields = false;

  public WebServiceAvailableDialog(
      Shell parent,
      IVariables variables,
      WebServiceAvailableMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "WebServiceAvailableDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // filename field
    Label wlURL = new Label(wContent, SWT.RIGHT);
    wlURL.setText(BaseMessages.getString(PKG, "WebServiceAvailableDialog.URL.Label"));
    PropsUi.setLook(wlURL);
    FormData fdlURL = new FormData();
    fdlURL.left = new FormAttachment(0, 0);
    fdlURL.right = new FormAttachment(middle, -margin);
    fdlURL.top = new FormAttachment(0, margin);
    wlURL.setLayoutData(fdlURL);

    wURL = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    wURL.setEditable(true);
    PropsUi.setLook(wURL);
    wURL.addModifyListener(lsMod);
    FormData fdURL = new FormData();
    fdURL.left = new FormAttachment(middle, 0);
    fdURL.top = new FormAttachment(0, margin);
    fdURL.right = new FormAttachment(100, -margin);
    wURL.setLayoutData(fdURL);
    wURL.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable FocusLost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });

    // connect timeout line
    Label wlConnectTimeOut = new Label(wContent, SWT.RIGHT);
    wlConnectTimeOut.setText(
        BaseMessages.getString(PKG, "WebServiceAvailableDialog.ConnectTimeOut.Label"));
    PropsUi.setLook(wlConnectTimeOut);
    FormData fdlConnectTimeOut = new FormData();
    fdlConnectTimeOut.left = new FormAttachment(0, 0);
    fdlConnectTimeOut.top = new FormAttachment(wURL, margin);
    fdlConnectTimeOut.right = new FormAttachment(middle, -margin);
    wlConnectTimeOut.setLayoutData(fdlConnectTimeOut);

    wConnectTimeOut = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wConnectTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceAvailableDialog.ConnectTimeOut.Tooltip"));
    PropsUi.setLook(wConnectTimeOut);
    wConnectTimeOut.addModifyListener(lsMod);
    FormData fdConnectTimeOut = new FormData();
    fdConnectTimeOut.left = new FormAttachment(middle, 0);
    fdConnectTimeOut.top = new FormAttachment(wURL, margin);
    fdConnectTimeOut.right = new FormAttachment(100, -margin);
    wConnectTimeOut.setLayoutData(fdConnectTimeOut);

    // Whenever something changes, set the tooltip to the expanded version:
    wConnectTimeOut.addModifyListener(
        e -> wConnectTimeOut.setToolTipText(variables.resolve(wConnectTimeOut.getText())));

    // Read timeout line
    Label wlReadTimeOut = new Label(wContent, SWT.RIGHT);
    wlReadTimeOut.setText(
        BaseMessages.getString(PKG, "WebServiceAvailableDialog.ReadTimeOut.Label"));
    PropsUi.setLook(wlReadTimeOut);
    FormData fdlReadTimeOut = new FormData();
    fdlReadTimeOut.left = new FormAttachment(0, 0);
    fdlReadTimeOut.top = new FormAttachment(wConnectTimeOut, margin);
    fdlReadTimeOut.right = new FormAttachment(middle, -margin);
    wlReadTimeOut.setLayoutData(fdlReadTimeOut);

    wReadTimeOut = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wReadTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceAvailableDialog.ReadTimeOut.Tooltip"));
    PropsUi.setLook(wReadTimeOut);
    wReadTimeOut.addModifyListener(lsMod);
    FormData fdReadTimeOut = new FormData();
    fdReadTimeOut.left = new FormAttachment(middle, 0);
    fdReadTimeOut.top = new FormAttachment(wConnectTimeOut, margin);
    fdReadTimeOut.right = new FormAttachment(100, -margin);
    wReadTimeOut.setLayoutData(fdReadTimeOut);

    // Whenever something changes, set the tooltip to the expanded version:
    wReadTimeOut.addModifyListener(
        e -> wReadTimeOut.setToolTipText(variables.resolve(wReadTimeOut.getText())));

    // Result fieldname ...
    Label wlResult = new Label(wContent, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "WebServiceAvailableDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wReadTimeOut, margin);
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(
        BaseMessages.getString(PKG, "WebServiceAvailableDialog.ResultField.Tooltip"));
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wReadTimeOut, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "WebServiceAvailableDialog.Log.GettingKeyInfo"));
    }

    if (input.getUrlField() != null) {
      wURL.setText(input.getUrlField());
    }
    if (input.getConnectTimeOut() != null) {
      wConnectTimeOut.setText(input.getConnectTimeOut());
    }
    if (input.getReadTimeOut() != null) {
      wReadTimeOut.setText(input.getReadTimeOut());
    }
    if (input.getResultFieldName() != null) {
      wResult.setText(input.getResultFieldName());
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    input.setUrlField(wURL.getText());
    input.setConnectTimeOut(wConnectTimeOut.getText());
    input.setReadTimeOut(wReadTimeOut.getText());
    input.setResultFieldName(wResult.getText());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String filefield = wURL.getText();
        wURL.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wURL.setItems(r.getFieldNames());
        }
        if (filefield != null) {
          wURL.setText(filefield);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "WebServiceAvailableDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(
                PKG, "WebServiceAvailableDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }
}
