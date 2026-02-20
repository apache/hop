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

package org.apache.hop.pipeline.transforms.html2text;

import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.i18n.BaseMessages.getString;
import static org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType.getTypeFromDescription;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.CHECK;
import static org.eclipse.swt.SWT.CURSOR_WAIT;
import static org.eclipse.swt.SWT.LEFT;
import static org.eclipse.swt.SWT.READ_ONLY;
import static org.eclipse.swt.SWT.RIGHT;
import static org.eclipse.swt.SWT.SINGLE;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.html2text.Html2TextMeta.SafelistType;
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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class Html2TextDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = Html2TextDialog.class; // For Translator
  private final Html2TextMeta input;
  private boolean gotPreviousFields = false;
  private CCombo wHtmlFieldName;
  private CCombo wSafelistType;

  private TextVar wOutputField;

  private Button wCleanOnly;
  private Button wNormalisedText;
  private Button wParallelism;

  public Html2TextDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (Html2TextMeta) in;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "Html2TextDialog.Shell.Title"));

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

    wCleanOnly = new Button(wContent, CHECK);
    wCleanOnly.setSelection(input.isCleanOnly());

    // HtmlFieldName field
    Label wlHtmlFieldName = new Label(wContent, RIGHT);
    wlHtmlFieldName.setText(getString(PKG, "Html2TextDialog.HtmlFieldName.Label"));
    PropsUi.setLook(wlHtmlFieldName);
    FormData fdlHtmlFieldName = new FormData();
    fdlHtmlFieldName.left = new FormAttachment(0, 0);
    fdlHtmlFieldName.right = new FormAttachment(middle, -margin);
    fdlHtmlFieldName.top = new FormAttachment(0, margin);
    wlHtmlFieldName.setLayoutData(fdlHtmlFieldName);
    wHtmlFieldName = new CCombo(wContent, BORDER | READ_ONLY);
    PropsUi.setLook(wHtmlFieldName);
    wHtmlFieldName.addModifyListener(lsMod);
    FormData fdHtmlFieldName = new FormData();
    fdHtmlFieldName.left = new FormAttachment(middle, 0);
    fdHtmlFieldName.top = new FormAttachment(0, margin);
    fdHtmlFieldName.right = new FormAttachment(100, -margin);
    wHtmlFieldName.setLayoutData(fdHtmlFieldName);
    wHtmlFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // OutputField
    Label wlOutputField = new Label(wContent, RIGHT);
    wlOutputField.setText(getString(PKG, "Html2TextDialog.OutputField.Label"));
    PropsUi.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(wHtmlFieldName, margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new TextVar(variables, wContent, SINGLE | LEFT | BORDER);
    wOutputField.setText("");
    PropsUi.setLook(wOutputField);
    wOutputField.addModifyListener(lsMod);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wHtmlFieldName, margin);
    fdOutputField.right = new FormAttachment(100, 0);
    wOutputField.setLayoutData(fdOutputField);

    // Normalised Text
    Label wlNormalisedText = new Label(wContent, RIGHT);
    wlNormalisedText.setVisible(!wCleanOnly.getSelection());
    wlNormalisedText.setText(getString(PKG, "Html2TextDialog.NormalisedText.Label"));
    PropsUi.setLook(wlNormalisedText);
    FormData fdlNormalisedText = new FormData();
    fdlNormalisedText.left = new FormAttachment(0, 0);
    fdlNormalisedText.top = new FormAttachment(wOutputField, margin);
    fdlNormalisedText.right = new FormAttachment(middle, -margin);
    wlNormalisedText.setLayoutData(fdlNormalisedText);

    wNormalisedText = new Button(wContent, CHECK);
    wNormalisedText.setSelection(input.isNormalisedText());
    wNormalisedText.setVisible(!wCleanOnly.getSelection());
    PropsUi.setLook(wNormalisedText);
    wNormalisedText.setToolTipText(getString(PKG, "Html2TextDialog.NormalisedText.Tooltip"));
    FormData fdNormalisedText = new FormData();
    fdNormalisedText.left = new FormAttachment(middle, -margin);
    fdNormalisedText.top = new FormAttachment(wOutputField, margin);
    fdNormalisedText.right = new FormAttachment(100, 0);
    wNormalisedText.setLayoutData(fdNormalisedText);
    wNormalisedText.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // CleanOnly
    Label wlCleanOnly = new Label(wContent, RIGHT);
    wlCleanOnly.setText(getString(PKG, "Html2TextDialog.CleanOnly.Label"));
    PropsUi.setLook(wlCleanOnly);
    FormData fdlCleanOnly = new FormData();
    fdlCleanOnly.left = new FormAttachment(0, 0);
    fdlCleanOnly.top = new FormAttachment(wNormalisedText, margin);
    fdlCleanOnly.right = new FormAttachment(middle, -margin);
    wlCleanOnly.setLayoutData(fdlCleanOnly);

    PropsUi.setLook(wCleanOnly);
    // wCleanOnly.setToolTipText(getString(PKG, "Html2TextDialog.CleanOnly.Tooltip"));
    FormData fdCleanOnly = new FormData();
    fdCleanOnly.left = new FormAttachment(middle, -margin);
    fdCleanOnly.top = new FormAttachment(wNormalisedText, margin);
    fdCleanOnly.right = new FormAttachment(100, 0);
    wCleanOnly.setLayoutData(fdCleanOnly);

    // SafelistType
    Label wlSafelistType = new Label(wContent, RIGHT);
    wlSafelistType.setVisible(wCleanOnly.getSelection());
    wCleanOnly.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            wSafelistType.setEnabled(wCleanOnly.getSelection());
            wSafelistType.setVisible(wCleanOnly.getSelection());
            wlSafelistType.setVisible(wCleanOnly.getSelection());
            wlNormalisedText.setVisible(!wCleanOnly.getSelection());
            wNormalisedText.setVisible(!wCleanOnly.getSelection());
          }
        });

    wlSafelistType.setText(getString(PKG, "Html2TextDialog.SafelistType.Label"));
    PropsUi.setLook(wlSafelistType);
    FormData fdSafelistType = new FormData();
    fdSafelistType.left = new FormAttachment(0, 0);
    fdSafelistType.right = new FormAttachment(middle, -margin);
    fdSafelistType.top = new FormAttachment(wCleanOnly, margin);
    wlSafelistType.setLayoutData(fdSafelistType);

    wSafelistType = new CCombo(wContent, SINGLE | READ_ONLY | BORDER);
    wSafelistType.setEnabled(wCleanOnly.getSelection());
    wSafelistType.setVisible(wCleanOnly.getSelection());
    wSafelistType.setItems(SafelistType.getDescriptions());
    wSafelistType.select(0);
    PropsUi.setLook(wSafelistType);
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(middle, 0);
    fdType.top = new FormAttachment(wCleanOnly, margin);
    fdType.right = new FormAttachment(100, 0);
    wSafelistType.setLayoutData(fdType);
    wSafelistType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Parallelism
    Label wlParallelism = new Label(wContent, RIGHT);
    wlParallelism.setText(getString(PKG, "Html2TextDialog.Parallelism.Label"));
    PropsUi.setLook(wlParallelism);
    FormData fdlParallelism = new FormData();
    fdlParallelism.left = new FormAttachment(0, 0);
    fdlParallelism.top = new FormAttachment(wSafelistType, margin);
    fdlParallelism.right = new FormAttachment(middle, -margin);
    wlParallelism.setLayoutData(fdlParallelism);

    wParallelism = new Button(wContent, CHECK);
    wParallelism.setSelection(input.isParallelism());
    PropsUi.setLook(wParallelism);
    wParallelism.setToolTipText(getString(PKG, "Html2TextDialog.Parallelism.Tooltip"));
    FormData fdParallelism = new FormData();
    fdParallelism.left = new FormAttachment(middle, -margin);
    fdParallelism.top = new FormAttachment(wSafelistType, margin);
    fdParallelism.right = new FormAttachment(100, 0);
    wParallelism.setLayoutData(fdParallelism);
    wParallelism.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

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
    if (input.getHtmlField() != null) {
      wHtmlFieldName.setText(input.getHtmlField());
    }

    if (input.isParallelism()) {
      wParallelism.setEnabled(input.isParallelism());
    }

    if (input.isCleanOnly()) {
      wCleanOnly.setEnabled(input.isCleanOnly());
    }

    if (input.isNormalisedText()) {
      wNormalisedText.setEnabled(input.isNormalisedText());
    }

    if (input.getSafelistType() != null) {
      String d = SafelistType.valueOf(input.getSafelistType()).getDescription();
      wSafelistType.setText(d);
    }

    wOutputField.setText(String.valueOf(input.getOutputField()));
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (isEmpty(wTransformName.getText())) {
      return;
    }

    input.setHtmlField(wHtmlFieldName.getText());

    input.setSafelistType(getTypeFromDescription(wSafelistType.getText()).getCode());

    input.setOutputField(wOutputField.getText());
    input.setCleanOnly(wCleanOnly.getSelection());
    input.setNormalisedText(wNormalisedText.getSelection());
    input.setParallelism(wParallelism.getSelection());

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String htmlField = null;
        if (wHtmlFieldName.getText() != null) {
          htmlField = wHtmlFieldName.getText();
        }
        wHtmlFieldName.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wHtmlFieldName.setItems(r.getFieldNames());
        }
        if (htmlField != null) {
          wHtmlFieldName.setText(htmlField);
        }
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            getString(PKG, "Html2TextDialog.FailedToGetFields.DialogTitle"),
            getString(PKG, "Html2TextDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
