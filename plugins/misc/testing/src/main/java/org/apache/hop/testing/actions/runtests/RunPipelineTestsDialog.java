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

package org.apache.hop.testing.actions.runtests;

import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class RunPipelineTestsDialog extends ActionDialog implements IActionDialog {

  private static final Class<?> PKG = RunPipelineTestsDialog.class;

  private static final String COLON_SEPARATOR = " : ";

  private RunPipelineTests action;

  private TableView wTestNames;

  private ColumnInfo[] columnInfos;

  public RunPipelineTestsDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (RunPipelineTests) action;

    if (this.action.getName() == null) {
      this.action.setName("RunPipelineTests");
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "RunPipelineTests.Name"), action);
    ModifyListener lsMod = e -> action.setChanged();

    int margin = this.margin;

    buildButtonBar()
        .ok(e -> ok())
        .custom(
            BaseMessages.getString(PKG, "RunTestsDialog.Button.GetTestNames"), e -> getTestNames())
        .cancel(e -> cancel())
        .build();

    Label wlTestNames = new Label(shell, SWT.LEFT);
    wlTestNames.setText(BaseMessages.getString(PKG, "RunTestsDialog.TestsToExecute.Label"));
    PropsUi.setLook(wlTestNames);
    FormData fdlTestNames = new FormData();
    fdlTestNames.left = new FormAttachment(0, 0);
    fdlTestNames.top = new FormAttachment(wSpacer, 2 * margin);
    fdlTestNames.right = new FormAttachment(100, 0);
    wlTestNames.setLayoutData(fdlTestNames);

    int tableCols = 1;
    int upInsRows =
        (action.getTestNames() != null && !action.getTestNames().equals(Collections.emptyList())
            ? action.getTestNames().size()
            : 1);
    columnInfos = new ColumnInfo[tableCols];

    columnInfos[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "RunTestsDialog.TestsTable.Name.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    wTestNames = new TableView(variables, shell, SWT.BORDER, columnInfos, upInsRows, lsMod, props);

    PropsUi.setLook(wTestNames);
    FormData fdTestNames = new FormData();
    fdTestNames.left = new FormAttachment(0, 0);
    fdTestNames.right = new FormAttachment(100, 0);
    fdTestNames.top = new FormAttachment(wlTestNames, margin);
    fdTestNames.bottom = new FormAttachment(wCancel, -margin * 2);
    wTestNames.setLayoutData(fdTestNames);

    setTableCombo();
    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void setTableCombo() {
    try {
      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);
      List<String> testNames = testSerializer.listObjectNames();
      columnInfos[0].setComboValues(
          Const.sortStrings(testNames.toArray(new String[testNames.size()])));

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of pipeline unit test names", e);
    }
  }

  private void getTestNames() {
    try {
      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);
      List<String> testNames = testSerializer.listObjectNames();
      if (!Utils.isEmpty(testNames)) {
        String[] sortedTestNames = Const.sortStrings(testNames.toArray(new String[0]));
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                shell,
                sortedTestNames,
                BaseMessages.getString(PKG, "RunTestsDialog.AvailableTests.Title"),
                BaseMessages.getString(PKG, "RunTestsDialog.AvailableTests.Message"));
        dialog.setMulti(true);
        if (dialog.open() != null) {
          wTestNames.removeEmptyRows();
          for (int i : dialog.getSelectionIndeces()) {
            wTestNames.add(sortedTestNames[i]);
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of pipeline unit test names", e);
    }
  }

  private void cancel() {
    action = null;
    dispose();
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    int rowNr = 0;
    for (RunPipelineTestsField testName : action.getTestNames()) {
      TableItem item = wTestNames.table.getItem(rowNr++);
      item.setText(1, Const.NVL(testName.getTestName(), ""));
    }
    wTestNames.setRowNums();
    wTestNames.optWidth(true);
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText("Warning");
      mb.setMessage("The name of the action is missing!");
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.getTestNames().clear();
    for (int i = 0; i < wTestNames.nrNonEmpty(); i++) {
      TableItem item = wTestNames.getNonEmpty(i);
      RunPipelineTestsField testName = new RunPipelineTestsField();
      testName.setTestName(item.getText(1));
      action.getTestNames().add(testName);
    }

    action.setChanged();

    dispose();
  }
}
