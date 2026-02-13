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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.PasswordTextVar;
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

public class PGPDecryptStreamDialog extends BaseTransformDialog {
  private static final Class<?> PKG = PGPDecryptStreamMeta.class;
  private boolean gotPreviousFields = false;

  private TextVar wGPGLocation;

  private Label wlPassphrase;
  private TextVar wPassphrase;

  private CCombo wStreamFieldName;

  private TextVar wResult;

  private final PGPDecryptStreamMeta input;

  private Button wPassphraseFromField;
  private Label wlPassphraseFromField;

  private CCombo wPassphraseFieldName;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "PGPDecryptStreamDialog.Filetype.All")};

  public PGPDecryptStreamDialog(
      Shell parent,
      IVariables variables,
      PGPDecryptStreamMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PGPDecryptStreamDialog.Shell.Title"));

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
    wGPGGroup.setText(BaseMessages.getString(PKG, "PGPDecryptStreamDialog.GPGGroup.Label"));

    FormLayout gpggroupgrouplayout = new FormLayout();
    gpggroupgrouplayout.marginWidth = 10;
    gpggroupgrouplayout.marginHeight = 10;
    wGPGGroup.setLayout(gpggroupgrouplayout);

    // GPGLocation fieldname ...
    Label wlGPGLocation = new Label(wGPGGroup, SWT.RIGHT);
    wlGPGLocation.setText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.GPGLocationField.Label"));
    PropsUi.setLook(wlGPGLocation);
    FormData fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment(0, 0);
    fdlGPGLocation.right = new FormAttachment(middle, -margin);
    fdlGPGLocation.top = new FormAttachment(0, margin);
    wlGPGLocation.setLayoutData(fdlGPGLocation);

    // Browse Source files button ...
    Button wbbGpgExe = new Button(wGPGGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbGpgExe);
    wbbGpgExe.setText(BaseMessages.getString(PKG, "PGPDecryptStreamDialog.BrowseFiles.Label"));
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
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.GPGLocationField.Tooltip"));
    PropsUi.setLook(wGPGLocation);
    wGPGLocation.addModifyListener(lsMod);
    FormData fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment(middle, 0);
    fdGPGLocation.top = new FormAttachment(0, margin);
    fdGPGLocation.right = new FormAttachment(wbbGpgExe, -margin);
    wGPGLocation.setLayoutData(fdGPGLocation);

    // Passphrase fieldname ...
    wlPassphrase = new Label(wGPGGroup, SWT.RIGHT);
    wlPassphrase.setText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.PassphraseField.Label"));
    PropsUi.setLook(wlPassphrase);
    FormData fdlPassphrase = new FormData();
    fdlPassphrase.left = new FormAttachment(0, 0);
    fdlPassphrase.right = new FormAttachment(middle, -margin);
    fdlPassphrase.top = new FormAttachment(wGPGLocation, margin);
    wlPassphrase.setLayoutData(fdlPassphrase);

    wPassphrase = new PasswordTextVar(variables, wGPGGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPassphrase.setToolTipText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.PassphraseField.Tooltip"));
    PropsUi.setLook(wPassphrase);
    wPassphrase.addModifyListener(lsMod);
    FormData fdPassphrase = new FormData();
    fdPassphrase.left = new FormAttachment(middle, 0);
    fdPassphrase.top = new FormAttachment(wGPGLocation, margin);
    fdPassphrase.right = new FormAttachment(100, 0);
    wPassphrase.setLayoutData(fdPassphrase);

    wlPassphraseFromField = new Label(wGPGGroup, SWT.RIGHT);
    wlPassphraseFromField.setText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.PassphraseFromField.Label"));
    PropsUi.setLook(wlPassphraseFromField);
    FormData fdlPassphraseFromField = new FormData();
    fdlPassphraseFromField.left = new FormAttachment(0, 0);
    fdlPassphraseFromField.top = new FormAttachment(wPassphrase, margin);
    fdlPassphraseFromField.right = new FormAttachment(middle, -margin);
    wlPassphraseFromField.setLayoutData(fdlPassphraseFromField);
    wPassphraseFromField = new Button(wGPGGroup, SWT.CHECK);
    PropsUi.setLook(wPassphraseFromField);
    wPassphraseFromField.setToolTipText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.PassphraseFromField.Tooltip"));
    FormData fdPassphraseFromField = new FormData();
    fdPassphraseFromField.left = new FormAttachment(middle, 0);
    fdPassphraseFromField.top = new FormAttachment(wlPassphraseFromField, 0, SWT.CENTER);
    wPassphraseFromField.setLayoutData(fdPassphraseFromField);

    wPassphraseFromField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            passphraseFromField();
          }
        });

    // Passphrase field
    Label wlPassphraseFieldName = new Label(wGPGGroup, SWT.RIGHT);
    wlPassphraseFieldName.setText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.PassphraseFieldName.Label"));
    PropsUi.setLook(wlPassphraseFieldName);
    FormData fdlPassphraseFieldName = new FormData();
    fdlPassphraseFieldName.left = new FormAttachment(0, 0);
    fdlPassphraseFieldName.right = new FormAttachment(middle, -margin);
    fdlPassphraseFieldName.top = new FormAttachment(wPassphraseFromField, margin);
    wlPassphraseFieldName.setLayoutData(fdlPassphraseFieldName);

    wPassphraseFieldName = new CCombo(wGPGGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wPassphraseFieldName);
    wPassphraseFieldName.addModifyListener(lsMod);
    FormData fdPassphraseFieldName = new FormData();
    fdPassphraseFieldName.left = new FormAttachment(middle, 0);
    fdPassphraseFieldName.top = new FormAttachment(wPassphraseFromField, margin);
    fdPassphraseFieldName.right = new FormAttachment(100, -margin);
    wPassphraseFieldName.setLayoutData(fdPassphraseFieldName);
    wPassphraseFieldName.addFocusListener(
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
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.StreamFieldName.Label"));
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
    wlResult.setText(BaseMessages.getString(PKG, "PGPDecryptStreamDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wStreamFieldName, margin);
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(
        BaseMessages.getString(PKG, "PGPDecryptStreamDialog.ResultField.Tooltip"));
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
    passphraseFromField();
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
    if (input.getPassphrase() != null) {
      wPassphrase.setText(input.getPassphrase());
    }
    wPassphraseFromField.setSelection(input.isPassphraseFromField());
    if (input.getPassphraseFieldName() != null) {
      wPassphraseFieldName.setText(input.getPassphraseFieldName());
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
    input.setPassphrase(wPassphrase.getText());
    input.setResultFieldName(wResult.getText());
    input.setPassphraseFromField(wPassphraseFromField.getSelection());
    input.setPassphraseFieldName(wPassphraseFieldName.getText());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void passphraseFromField() {
    wlPassphrase.setEnabled(!wPassphraseFromField.getSelection());
    wPassphrase.setEnabled(!wPassphraseFromField.getSelection());
    wlPassphraseFromField.setEnabled(wPassphraseFromField.getSelection());
    wPassphraseFromField.setEnabled(wPassphraseFromField.getSelection());
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String fieldvalue = wStreamFieldName.getText();
        String passphrasefieldvalue = wPassphraseFieldName.getText();
        wStreamFieldName.removeAll();
        wPassphraseFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          String[] fields = r.getFieldNames();
          wStreamFieldName.setItems(fields);
          wPassphraseFieldName.setItems(fields);
        }
        if (fieldvalue != null) {
          wStreamFieldName.setText(fieldvalue);
        }
        if (passphrasefieldvalue != null) {
          wPassphraseFieldName.setText(passphrasefieldvalue);
        }
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "PGPDecryptStreamDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "PGPDecryptStreamDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
