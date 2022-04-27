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

package org.apache.hop.pipeline.transforms.drools;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class RulesExecutorDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = Rules.class;

  private RulesExecutorMeta input;

  private Label wlRuleFilePath;
  private Button wbBrowse;
  private Button wbRulesInEditor;
  private TextVar wRuleFilePath;
  private StyledTextComp wRulesEditor;
  private Label wlPosition;
  private TableView wResultColumnsFields;

  public RulesExecutorDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (RulesExecutorMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "RulesExecutor.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "RulesDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    addRulesTab(wTabFolder, margin);
    addRulesResultsTab(wTabFolder, margin);

    FormData fdAgg = new FormData();
    fdAgg.left = new FormAttachment(0, 0);
    fdAgg.bottom = new FormAttachment(wOk, -margin);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    wTabFolder.setSelection(0);

    getData();

    activeRuleFilenameField();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activeRuleFilenameField() {
    wlRuleFilePath.setEnabled(!wbRulesInEditor.getSelection());
    wRuleFilePath.setEnabled(!wbRulesInEditor.getSelection());

    wRulesEditor.setEnabled(wbRulesInEditor.getSelection());
  }

  private void addRulesTab(CTabFolder wTabFolder, int margin) {

    ModifyListener lsMod =
        e -> {
          // changedInDialog = true;
          input.setChanged();
        };

    CTabItem wRulesTab = new CTabItem(wTabFolder, SWT.NONE);
    wRulesTab.setText(BaseMessages.getString(PKG, "RulesDialog.Tabs.RuleDefinition"));

    Composite wRulesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wRulesComp);

    FormLayout rulesLayout = new FormLayout();
    rulesLayout.marginWidth = 3;
    rulesLayout.marginHeight = 3;
    wRulesComp.setLayout(rulesLayout);

    wlRuleFilePath = new Label(wRulesComp, SWT.LEFT);
    props.setLook(wlRuleFilePath);
    wlRuleFilePath.setText(BaseMessages.getString(PKG, "RulesDialog.RulesFile.Label"));
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment(0, 0);
    fdlTransformation.top = new FormAttachment(0, 20);
    fdlTransformation.right = new FormAttachment(50, 0);
    wlRuleFilePath.setLayoutData(fdlTransformation);

    wbBrowse = new Button(wRulesComp, SWT.PUSH);
    props.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "RulesDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlRuleFilePath, Const.isOSX() ? 0 : 5);
    wbBrowse.setLayoutData(fdBrowse);
    wbBrowse.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wRuleFilePath,
                variables,
                new String[] {"*"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true));

    wRuleFilePath = new TextVar(variables, wRulesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    FormData fdRuleFilePath = new FormData();
    fdRuleFilePath.left = new FormAttachment(0, 0);
    fdRuleFilePath.top = new FormAttachment(wlRuleFilePath, 5);
    fdRuleFilePath.right = new FormAttachment(wbBrowse, -props.getMargin());
    wRuleFilePath.setLayoutData(fdRuleFilePath);

    wbRulesInEditor = new Button(wRulesComp, SWT.CHECK);
    props.setLook(wbRulesInEditor);
    wbRulesInEditor.setText(
        BaseMessages.getString(PKG, "RulesDialog.RuleDefinition.EnableScriptEditor.Label"));
    FormData fdPipelineNameInField = new FormData();
    fdPipelineNameInField.left = new FormAttachment(0, 0);
    fdPipelineNameInField.top = new FormAttachment(wRuleFilePath, margin);
    wbRulesInEditor.setLayoutData(fdPipelineNameInField);
    wbRulesInEditor.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                input.setChanged();
                activeRuleFilenameField();
              }
            });

    wRulesEditor =
        new StyledTextComp(
            variables, wRulesComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wRulesEditor, Props.WIDGET_STYLE_FIXED);

    FormData fdRulesEditor = new FormData();
    fdRulesEditor.left = new FormAttachment(0, 0);
    fdRulesEditor.top = new FormAttachment(wbRulesInEditor, 5);
    fdRulesEditor.right = new FormAttachment(100, -2 * margin);
    fdRulesEditor.bottom = new FormAttachment(100, -12 * margin);
    wRulesEditor.setLayoutData(fdRulesEditor);

    wRulesEditor.addModifyListener(lsMod);
    wRulesEditor.addModifyListener(arg0 -> setPosition());

    wRulesEditor.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wRulesEditor.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wRulesEditor.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    // Position label under the SQL editor
    //
    wlPosition = new Label(wRulesComp, SWT.NONE);
    props.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.top =
        new FormAttachment(wRulesEditor, margin); // 2 times since we deal with bottom instead of
    fdlPosition.right = new FormAttachment(100, 0);
    // top
    wlPosition.setLayoutData(fdlPosition);

    FormData fdRulesComp = new FormData();
    fdRulesComp.left = new FormAttachment(0, 0);
    fdRulesComp.top = new FormAttachment(0, 0);
    fdRulesComp.right = new FormAttachment(100, 0);
    fdRulesComp.bottom = new FormAttachment(100, 0);
    wRulesComp.setLayoutData(fdRulesComp);

    wRulesComp.layout();
    wRulesTab.setControl(wRulesComp);
  }

  private void setPosition() {
    int lineNumber = wRulesEditor.getLineNumber();
    int columnNumber = wRulesEditor.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "RulesDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private void addRulesResultsTab(CTabFolder wTabFolder, int margin) {

    CTabItem wRulesResultsTab = new CTabItem(wTabFolder, SWT.NONE);
    wRulesResultsTab.setText(BaseMessages.getString(PKG, "RulesDialog.Tabs.ColumnSelection"));

    Composite wRulesResultsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wRulesResultsComp);

    FormLayout rulesResultsLayout = new FormLayout();
    rulesResultsLayout.marginWidth = 3;
    rulesResultsLayout.marginHeight = 3;
    wRulesResultsComp.setLayout(rulesResultsLayout);

    int nrRows = (input.getRuleResultColumns() != null ? input.getRuleResultColumns().size() : 1);

    ColumnInfo[] ciResultFields =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RulesDialog.ColumnSelection.ColumnName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RulesDialog.ColumnSelection.ColumnType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
        };

    wResultColumnsFields =
        new TableView(
            variables,
            wRulesResultsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciResultFields,
            nrRows,
            false,
            null,
            props,
            false);

    FormData fdResultFields = new FormData();
    fdResultFields.left = new FormAttachment(0, 0);
    fdResultFields.top = new FormAttachment(0, 5);
    fdResultFields.right = new FormAttachment(100, 0);
    fdResultFields.bottom = new FormAttachment(100, -margin * 8);
    wResultColumnsFields.setLayoutData(fdResultFields);
    wResultColumnsFields.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 25, 25));

    FormData fdRulesResultsComp = new FormData();
    fdRulesResultsComp.left = new FormAttachment(0, 0);
    fdRulesResultsComp.top = new FormAttachment(0, 0);
    fdRulesResultsComp.right = new FormAttachment(100, 0);
    fdRulesResultsComp.bottom = new FormAttachment(100, 0);
    wRulesResultsComp.setLayoutData(fdRulesResultsComp);

    wRulesResultsComp.layout();
    wRulesResultsTab.setControl(wRulesResultsComp);

    getData();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setRuleFile(wRuleFilePath.getText());
    input.setRuleDefinition(wRulesEditor.getText());

    input.getRuleResultColumns().clear();

    for (int i = 0; i < wResultColumnsFields.nrNonEmpty(); i++) {
      TableItem item = wResultColumnsFields.getNonEmpty(i);

      if (!Utils.isEmpty(item.getText(1))) {
        input.getRuleResultColumns().add(new RuleResultItem(item.getText(1), item.getText(2)));
      }
    }

    dispose();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(false);
    dispose();
  }

  public void getData() {

    if (input.getRuleFile() != null) {
      wlRuleFilePath.setText(input.getRuleFile());
    }

    if (input.getRuleDefinition() != null) {
      wRulesEditor.setText(input.getRuleDefinition());
    }

    wbRulesInEditor.setSelection(input.getRuleDefinition() != null);

    for (int i = 0; i<input.getRuleResultColumns().size(); i++) {
      TableItem ti = wResultColumnsFields.table.getItem(i);
      RuleResultItem ri = input.getRuleResultColumns().get(i);
      ti.setText(1, ri.getName());
      ti.setText(2, ri.getType());
    }

    wResultColumnsFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }
}
