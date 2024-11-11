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

package org.apache.hop.workflow.actions.xml.xsdvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the XSD Validator job entry settings. */
public class XsdValidatorDialog extends ActionDialog {
  private static final Class<?> PKG = XsdValidator.class;

  private static final String[] FILETYPES_XML =
      new String[] {
        BaseMessages.getString(PKG, "ActionXSDValidator.Filetype.Xml"),
        BaseMessages.getString(PKG, "ActionXSDValidator.Filetype.All")
      };

  private static final String[] FILETYPES_XSD =
      new String[] {
        BaseMessages.getString(PKG, "ActionXSDValidator.Filetype.Xsd"),
        BaseMessages.getString(PKG, "ActionXSDValidator.Filetype.All")
      };

  private Text wName;

  private Button wAllowExternalEntities;

  private CCombo wXSDSource;

  private TextVar wxmlFilename;

  Label wlxsdFilename;

  private TextVar wxsdFilename;

  Button wbxsdFilename;

  private XsdValidator action;

  private boolean changed;

  public XsdValidatorDialog(
      Shell parent, XsdValidator action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionXSDValidator.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionXSDValidator.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionXSDValidator.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Enable/Disable external entity for XSD validation.
    Label wlAllowExternalEntities = new Label(shell, SWT.RIGHT);
    wlAllowExternalEntities.setText(
        BaseMessages.getString(PKG, "ActionXSDValidator.AllowExternalEntities.Label"));
    PropsUi.setLook(wlAllowExternalEntities);
    FormData fdlAllowExternalEntities = new FormData();
    fdlAllowExternalEntities.left = new FormAttachment(0, 0);
    fdlAllowExternalEntities.right = new FormAttachment(middle, -margin);
    fdlAllowExternalEntities.top = new FormAttachment(wName, margin);
    wlAllowExternalEntities.setLayoutData(fdlAllowExternalEntities);
    wAllowExternalEntities = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAllowExternalEntities);
    FormData fdAllowExternalEntities = new FormData();
    fdAllowExternalEntities.left = new FormAttachment(middle, 0);
    fdAllowExternalEntities.top = new FormAttachment(wlAllowExternalEntities, 0, SWT.CENTER);
    fdAllowExternalEntities.right = new FormAttachment(100, 0);
    wAllowExternalEntities.setLayoutData(fdAllowExternalEntities);
    wAllowExternalEntities.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // XSD Source?
    Label wlXSDSource = new Label(shell, SWT.RIGHT);
    wlXSDSource.setText(BaseMessages.getString(PKG, "ActionXSDValidator.XSDSource.Label"));
    PropsUi.setLook(wlXSDSource);
    FormData fdlXSDSource = new FormData();
    fdlXSDSource.left = new FormAttachment(0, 0);
    fdlXSDSource.top = new FormAttachment(wAllowExternalEntities, margin);
    fdlXSDSource.right = new FormAttachment(middle, -margin);
    wlXSDSource.setLayoutData(fdlXSDSource);
    wXSDSource = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wXSDSource.setEditable(true);
    PropsUi.setLook(wXSDSource);
    wXSDSource.addModifyListener(lsMod);
    FormData fdXSDSource = new FormData();
    fdXSDSource.left = new FormAttachment(middle, 0);
    fdXSDSource.top = new FormAttachment(wAllowExternalEntities, margin);
    fdXSDSource.right = new FormAttachment(100, -margin);
    wXSDSource.setLayoutData(fdXSDSource);
    wXSDSource.add(BaseMessages.getString(PKG, "ActionXSDValidator.XSDSource.IS_A_FILE"));
    wXSDSource.add(BaseMessages.getString(PKG, "ActionXSDValidator.XSDSource.NO_NEED"));
    wXSDSource.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setXSDSource();
          }
        });

    // Filename 1 line
    Label wlxmlFilename = new Label(shell, SWT.RIGHT);
    wlxmlFilename.setText(BaseMessages.getString(PKG, "ActionXSDValidator.xmlFilename.Label"));
    PropsUi.setLook(wlxmlFilename);
    FormData fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment(0, 0);
    fdlxmlFilename.top = new FormAttachment(wXSDSource, 2 * margin);
    fdlxmlFilename.right = new FormAttachment(middle, -margin);
    wlxmlFilename.setLayoutData(fdlxmlFilename);
    Button wbxmlFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbxmlFilename);
    wbxmlFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment(100, 0);
    fdbxmlFilename.top = new FormAttachment(wXSDSource, 2 * margin);
    wbxmlFilename.setLayoutData(fdbxmlFilename);
    wxmlFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wxmlFilename);
    wxmlFilename.addModifyListener(lsMod);
    FormData fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment(middle, 0);
    fdxmlFilename.top = new FormAttachment(wXSDSource, 2 * margin);
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
                true));

    // Filename 2 line
    wlxsdFilename = new Label(shell, SWT.RIGHT);
    wlxsdFilename.setText(BaseMessages.getString(PKG, "ActionXSDValidator.xsdFilename.Label"));
    PropsUi.setLook(wlxsdFilename);
    FormData fdlxsdFilename = new FormData();
    fdlxsdFilename.left = new FormAttachment(0, 0);
    fdlxsdFilename.top = new FormAttachment(wxmlFilename, margin);
    fdlxsdFilename.right = new FormAttachment(middle, -margin);
    wlxsdFilename.setLayoutData(fdlxsdFilename);
    wbxsdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbxsdFilename);
    wbxsdFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbxsdFilename = new FormData();
    fdbxsdFilename.right = new FormAttachment(100, 0);
    fdbxsdFilename.top = new FormAttachment(wxmlFilename, 0);
    wbxsdFilename.setLayoutData(fdbxsdFilename);
    wxsdFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wxsdFilename);
    wxsdFilename.addModifyListener(lsMod);
    FormData fdxsdFilename = new FormData();
    fdxsdFilename.left = new FormAttachment(middle, 0);
    fdxsdFilename.top = new FormAttachment(wxmlFilename, margin);
    fdxsdFilename.right = new FormAttachment(wbxsdFilename, -margin);
    wxsdFilename.setLayoutData(fdxsdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wxsdFilename.addModifyListener(
        e -> wxsdFilename.setToolTipText(variables.resolve(wxsdFilename.getText())));
    wbxsdFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wxsdFilename,
                variables,
                new String[] {"*.xsd;*.XSD", "*"},
                FILETYPES_XSD,
                true));

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, wxsdFilename);

    getData();
    setXSDSource();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wAllowExternalEntities.setSelection(action.isAllowExternalEntities());

    if (action.getXsdSource() != null) {
      if (action.getXsdSource().equals(XsdValidator.SPECIFY_FILENAME)) {
        wXSDSource.select(0);
      } else if (action.getXsdSource().equals(XsdValidator.NO_NEED)) {
        wXSDSource.select(1);
      }
    }

    wxmlFilename.setText(Const.nullToEmpty(action.getXmlFilename()));
    wxsdFilename.setText(Const.nullToEmpty(action.getXsdFilename()));

    wName.selectAll();
    wName.setFocus();
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
    action.setAllowExternalEntities(wAllowExternalEntities.getSelection());
    action.setXmlFilename(wxmlFilename.getText());
    action.setXsdFilename(wxsdFilename.getText());

    if (wXSDSource.getSelectionIndex() == 0) {
      action.setXsdSource(XsdValidator.SPECIFY_FILENAME);
    } else if (wXSDSource.getSelectionIndex() == 1) {
      action.setXsdSource(XsdValidator.NO_NEED);
    }

    dispose();
  }

  private void setXSDSource() {
    if (wXSDSource.getSelectionIndex() == 0) {
      wlxsdFilename.setEnabled(true);
      wxsdFilename.setEnabled(true);
      wbxsdFilename.setEnabled(true);
    } else if (wXSDSource.getSelectionIndex() == 1) {
      wlxsdFilename.setEnabled(false);
      wxsdFilename.setEnabled(false);
      wbxsdFilename.setEnabled(false);
    }
  }
}
