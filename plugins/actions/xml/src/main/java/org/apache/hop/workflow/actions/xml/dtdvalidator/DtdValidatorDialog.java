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

package org.apache.hop.workflow.actions.xml.dtdvalidator;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the DTD Validator job entry settings. */
public class DtdValidatorDialog extends ActionDialog {
  private static final Class<?> PKG = DtdValidator.class;

  private static final String[] FILETYPES_XML =
      new String[] {
        BaseMessages.getString(PKG, "ActionDTDValidator.Filetype.Xml"),
        BaseMessages.getString(PKG, "ActionDTDValidator.Filetype.All")
      };

  private static final String[] FILETYPES_DTD =
      new String[] {
        BaseMessages.getString(PKG, "ActionDTDValidator.Filetype.Dtd"),
        BaseMessages.getString(PKG, "ActionDTDValidator.Filetype.All")
      };

  private TextVar wxmlFilename;

  private Label wldtdFilename;
  private Button wbdtdFilename;
  private TextVar wdtdFilename;

  private Button wDTDIntern;

  private DtdValidator action;

  private boolean changed;

  public DtdValidatorDialog(
      Shell parent, DtdValidator action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionDTDValidator.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionDTDValidator.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    // XML Filename
    Label wlxmlFilename = new Label(shell, SWT.RIGHT);
    wlxmlFilename.setText(BaseMessages.getString(PKG, "ActionDTDValidator.xmlFilename.Label"));
    PropsUi.setLook(wlxmlFilename);
    FormData fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment(0, 0);
    fdlxmlFilename.top = new FormAttachment(wSpacer, margin);
    fdlxmlFilename.right = new FormAttachment(middle, -margin);
    wlxmlFilename.setLayoutData(fdlxmlFilename);
    Button wbxmlFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbxmlFilename);
    wbxmlFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment(100, 0);
    fdbxmlFilename.top = new FormAttachment(wlxmlFilename, 0, SWT.CENTER);
    wbxmlFilename.setLayoutData(fdbxmlFilename);
    wxmlFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wxmlFilename);
    wxmlFilename.addModifyListener(lsMod);
    FormData fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment(middle, 0);
    fdxmlFilename.top = new FormAttachment(wlxmlFilename, 0, SWT.CENTER);
    fdxmlFilename.right = new FormAttachment(wbxmlFilename, -margin);
    wxmlFilename.setLayoutData(fdxmlFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wxmlFilename.addModifyListener(
        e -> wxmlFilename.setToolTipText(variables.resolve(wxmlFilename.getText())));
    wbxmlFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wxmlFilename,
                variables,
                new String[] {"*.xml;*.XML", "*"},
                FILETYPES_XML,
                false));

    // DTD Intern ?
    // Intern DTD
    Label wlDTDIntern = new Label(shell, SWT.RIGHT);
    wlDTDIntern.setText(BaseMessages.getString(PKG, "ActionDTDValidator.DTDIntern.Label"));
    PropsUi.setLook(wlDTDIntern);
    FormData fdlDTDIntern = new FormData();
    fdlDTDIntern.left = new FormAttachment(0, 0);
    fdlDTDIntern.top = new FormAttachment(wxmlFilename, margin);
    fdlDTDIntern.right = new FormAttachment(middle, -margin);
    wlDTDIntern.setLayoutData(fdlDTDIntern);
    wDTDIntern = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wDTDIntern);
    wDTDIntern.setToolTipText(BaseMessages.getString(PKG, "ActionDTDValidator.DTDIntern.Tooltip"));
    FormData fdDTDIntern = new FormData();
    fdDTDIntern.left = new FormAttachment(middle, 0);
    fdDTDIntern.top = new FormAttachment(wlDTDIntern, 0, SWT.CENTER);
    fdDTDIntern.right = new FormAttachment(100, 0);
    wDTDIntern.setLayoutData(fdDTDIntern);
    wDTDIntern.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeDTDFilename();
            action.setChanged();
          }
        });

    // DTD Filename
    wldtdFilename = new Label(shell, SWT.RIGHT);
    wldtdFilename.setText(BaseMessages.getString(PKG, "ActionDTDValidator.DTDFilename.Label"));
    PropsUi.setLook(wldtdFilename);
    FormData fdldtdFilename = new FormData();
    fdldtdFilename.left = new FormAttachment(0, 0);
    fdldtdFilename.top = new FormAttachment(wlDTDIntern, margin);
    fdldtdFilename.right = new FormAttachment(middle, -margin);
    wldtdFilename.setLayoutData(fdldtdFilename);
    wbdtdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdtdFilename);
    wbdtdFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbdtdFilename = new FormData();
    fdbdtdFilename.right = new FormAttachment(100, 0);
    fdbdtdFilename.top = new FormAttachment(wldtdFilename, 0, SWT.CENTER);
    wbdtdFilename.setLayoutData(fdbdtdFilename);
    wdtdFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wdtdFilename);
    wdtdFilename.addModifyListener(lsMod);
    FormData fddtdFilename = new FormData();
    fddtdFilename.left = new FormAttachment(middle, 0);
    fddtdFilename.top = new FormAttachment(wldtdFilename, 0, SWT.CENTER);
    fddtdFilename.right = new FormAttachment(wbdtdFilename, -margin);
    wdtdFilename.setLayoutData(fddtdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wdtdFilename.addModifyListener(
        e -> wdtdFilename.setToolTipText(variables.resolve(wdtdFilename.getText())));
    wbdtdFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wdtdFilename,
                variables,
                new String[] {"*.dtd;*.DTD", "*"},
                FILETYPES_DTD,
                false));

    getData();
    activeDTDFilename();
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void activeDTDFilename() {
    wldtdFilename.setEnabled(!wDTDIntern.getSelection());
    wdtdFilename.setEnabled(!wDTDIntern.getSelection());
    wbdtdFilename.setEnabled(!wDTDIntern.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getXmlFilename() != null) {
      wxmlFilename.setText(action.getXmlFilename());
    }
    if (action.getDtdFilename() != null) {
      wdtdFilename.setText(action.getDtdFilename());
    }
    wDTDIntern.setSelection("Y".equals(action.getDtdIntern()));
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.ActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setXmlFilename(wxmlFilename.getText());
    action.setDtdFilename(wdtdFilename.getText());

    action.setDtdIntern(wDTDIntern.getSelection() ? "Y" : "N");

    dispose();
  }
}
