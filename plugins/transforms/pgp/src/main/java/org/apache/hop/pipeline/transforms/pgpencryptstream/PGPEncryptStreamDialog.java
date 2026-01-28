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

package org.apache.hop.pipeline.transforms.pgpencryptstream;

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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class PGPEncryptStreamDialog extends BaseTransformDialog {
  private static final Class<?> PKG = PGPEncryptStreamMeta.class;
  private boolean gotPreviousFields = false;

  private TextVar wGPGLocation;

  private Label wlKeyName;
  private TextVar wKeyName;

  private CCombo wStreamFieldName;

  private TextVar wResult;
  private final PGPEncryptStreamMeta input;

  private Button wKeyNameFromField;

  private Label wlKeyNameFieldName;
  private CCombo wKeyNameFieldName;

  public PGPEncryptStreamDialog(
      Shell parent,
      IVariables variables,
      PGPEncryptStreamMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PGPEncryptStreamDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // ///////////////////////////////
    // START OF GPG Fields GROUP //
    // ///////////////////////////////

    Group wGPGGroup = new Group(wContent, SWT.SHADOW_NONE);
    PropsUi.setLook(wGPGGroup);
    wGPGGroup.setText(BaseMessages.getString(PKG, "PGPEncryptStreamDialog.GPGGroup.Label"));

    FormLayout gpggroupgrouplayout = new FormLayout();
    gpggroupgrouplayout.marginWidth = 10;
    gpggroupgrouplayout.marginHeight = 10;
    wGPGGroup.setLayout(gpggroupgrouplayout);

    // GPGLocation fieldname ...
    Label wlGPGLocation = new Label(wGPGGroup, SWT.RIGHT);
    wlGPGLocation.setText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.GPGLocationField.Label"));
    PropsUi.setLook(wlGPGLocation);
    FormData fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment(0, 0);
    fdlGPGLocation.right = new FormAttachment(middle, -margin);
    fdlGPGLocation.top = new FormAttachment(0, margin);
    wlGPGLocation.setLayoutData(fdlGPGLocation);

    // Browse Source files button ...
    Button wbbGpgExe = new Button(wGPGGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbGpgExe);
    wbbGpgExe.setText(BaseMessages.getString(PKG, "PGPEncryptStreamDialog.BrowseFiles.Label"));
    FormData fdbbGpgExe = new FormData();
    fdbbGpgExe.right = new FormAttachment(100, -margin);
    fdbbGpgExe.top = new FormAttachment(0, margin);
    wbbGpgExe.setLayoutData(fdbbGpgExe);

    if (wbbGpgExe != null) {
      // Listen to the browse button next to the file name
      //
      wbbGpgExe.addListener(
          SWT.Selection,
          e ->
              BaseDialog.presentFileDialog(
                  shell,
                  wGPGLocation,
                  variables,
                  new String[] {"*"},
                  new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                  true));
    }

    wGPGLocation = new TextVar(variables, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wGPGLocation.setToolTipText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.GPGLocationField.Tooltip"));
    PropsUi.setLook(wGPGLocation);
    wGPGLocation.addModifyListener(lsMod);
    FormData fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment(middle, 0);
    fdGPGLocation.top = new FormAttachment(0, margin);
    fdGPGLocation.right = new FormAttachment(wbbGpgExe, -margin);
    wGPGLocation.setLayoutData(fdGPGLocation);

    // KeyName fieldname ...
    wlKeyName = new Label(wGPGGroup, SWT.RIGHT);
    wlKeyName.setText(BaseMessages.getString(PKG, "PGPEncryptStreamDialog.KeyNameField.Label"));
    PropsUi.setLook(wlKeyName);
    FormData fdlKeyName = new FormData();
    fdlKeyName.left = new FormAttachment(0, 0);
    fdlKeyName.right = new FormAttachment(middle, -margin);
    fdlKeyName.top = new FormAttachment(wGPGLocation, margin);
    wlKeyName.setLayoutData(fdlKeyName);

    wKeyName = new TextVar(variables, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wKeyName.setToolTipText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.KeyNameField.Tooltip"));
    PropsUi.setLook(wKeyName);
    wKeyName.addModifyListener(lsMod);
    FormData fdKeyName = new FormData();
    fdKeyName.left = new FormAttachment(middle, 0);
    fdKeyName.top = new FormAttachment(wGPGLocation, margin);
    fdKeyName.right = new FormAttachment(100, 0);
    wKeyName.setLayoutData(fdKeyName);

    Label wlKeyNameFromField = new Label(wGPGGroup, SWT.RIGHT);
    wlKeyNameFromField.setText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.KeyNameFromField.Label"));
    PropsUi.setLook(wlKeyNameFromField);
    FormData fdlKeyNameFromField = new FormData();
    fdlKeyNameFromField.left = new FormAttachment(0, 0);
    fdlKeyNameFromField.top = new FormAttachment(wKeyName, margin);
    fdlKeyNameFromField.right = new FormAttachment(middle, -margin);
    wlKeyNameFromField.setLayoutData(fdlKeyNameFromField);
    wKeyNameFromField = new Button(wGPGGroup, SWT.CHECK);
    PropsUi.setLook(wKeyNameFromField);
    wKeyNameFromField.setToolTipText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.KeyNameFromField.Tooltip"));
    FormData fdKeyNameFromField = new FormData();
    fdKeyNameFromField.left = new FormAttachment(middle, 0);
    fdKeyNameFromField.top = new FormAttachment(wlKeyNameFromField, 0, SWT.CENTER);
    wKeyNameFromField.setLayoutData(fdKeyNameFromField);

    wKeyNameFromField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            keyNameFromField();
            input.setChanged();
          }
        });

    // Stream field
    wlKeyNameFieldName = new Label(wGPGGroup, SWT.RIGHT);
    wlKeyNameFieldName.setText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.KeyNameFieldName.Label"));
    PropsUi.setLook(wlKeyNameFieldName);
    FormData fdlKeyNameFieldName = new FormData();
    fdlKeyNameFieldName.left = new FormAttachment(0, 0);
    fdlKeyNameFieldName.right = new FormAttachment(middle, -margin);
    fdlKeyNameFieldName.top = new FormAttachment(wKeyNameFromField, margin);
    wlKeyNameFieldName.setLayoutData(fdlKeyNameFieldName);

    wKeyNameFieldName = new CCombo(wGPGGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wKeyNameFieldName);
    wKeyNameFieldName.addModifyListener(lsMod);
    FormData fdKeyNameFieldName = new FormData();
    fdKeyNameFieldName.left = new FormAttachment(middle, 0);
    fdKeyNameFieldName.top = new FormAttachment(wKeyNameFromField, margin);
    fdKeyNameFieldName.right = new FormAttachment(100, -margin);
    wKeyNameFieldName.setLayoutData(fdKeyNameFieldName);
    wKeyNameFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    FormData fdGPGGroup = new FormData();
    fdGPGGroup.left = new FormAttachment(0, margin);
    fdGPGGroup.top = new FormAttachment(0, margin);
    fdGPGGroup.right = new FormAttachment(100, -margin);
    wGPGGroup.setLayoutData(fdGPGGroup);

    // ///////////////////////////////
    // END OF GPG GROUP //
    // ///////////////////////////////

    // Stream field
    Label wlStreamFieldName = new Label(wContent, SWT.RIGHT);
    wlStreamFieldName.setText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.StreamFieldName.Label"));
    PropsUi.setLook(wlStreamFieldName);
    FormData fdlStreamFieldName = new FormData();
    fdlStreamFieldName.left = new FormAttachment(0, 0);
    fdlStreamFieldName.right = new FormAttachment(middle, -margin);
    fdlStreamFieldName.top = new FormAttachment(wGPGGroup, margin);
    wlStreamFieldName.setLayoutData(fdlStreamFieldName);

    wStreamFieldName = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wStreamFieldName);
    wStreamFieldName.addModifyListener(lsMod);
    FormData fdStreamFieldName = new FormData();
    fdStreamFieldName.left = new FormAttachment(middle, 0);
    fdStreamFieldName.top = new FormAttachment(wGPGGroup, margin);
    fdStreamFieldName.right = new FormAttachment(100, -margin);
    wStreamFieldName.setLayoutData(fdStreamFieldName);
    wStreamFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // Result fieldname ...
    Label wlResult = new Label(wContent, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "PGPEncryptStreamDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wStreamFieldName, margin);
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(
        BaseMessages.getString(PKG, "PGPEncryptStreamDialog.ResultField.Tooltip"));
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wStreamFieldName, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();
    keyNameFromField();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getGPGLocation() != null) {
      wGPGLocation.setText(input.getGPGLocation());
    }
    if (input.getStreamField() != null) {
      wStreamFieldName.setText(input.getStreamField());
    }
    if (input.getResultFieldName() != null) {
      wResult.setText(input.getResultFieldName());
    }
    if (input.getKeyName() != null) {
      wKeyName.setText(input.getKeyName());
    }
    wKeyNameFromField.setSelection(input.isKeynameInField());
    if (input.getKeynameFieldName() != null) {
      wKeyNameFieldName.setText(input.getKeynameFieldName());
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
    input.setStreamField(wStreamFieldName.getText());
    input.setGPGLocation(wGPGLocation.getText());
    input.setKeyName(wKeyName.getText());
    input.setResultFieldName(wResult.getText());
    input.setKeynameInField(wKeyNameFromField.getSelection());
    input.setKeynameFieldName(wKeyNameFieldName.getText());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void keyNameFromField() {
    wlKeyName.setEnabled(!wKeyNameFromField.getSelection());
    wKeyName.setEnabled(!wKeyNameFromField.getSelection());
    wlKeyNameFieldName.setEnabled(wKeyNameFromField.getSelection());
    wKeyNameFieldName.setEnabled(wKeyNameFromField.getSelection());
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String fieldvalue = wStreamFieldName.getText();
        wStreamFieldName.removeAll();
        String keyNameFieldNameText = wKeyNameFieldName.getText();
        wKeyNameFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wStreamFieldName.setItems(r.getFieldNames());
          wKeyNameFieldName.setItems(r.getFieldNames());
        }
        if (fieldvalue != null) {
          wStreamFieldName.setText(fieldvalue);
        }
        if (keyNameFieldNameText != null) {
          wKeyNameFieldName.setText(keyNameFieldNameText);
        }
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "PGPEncryptStreamDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "PGPEncryptStreamDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
