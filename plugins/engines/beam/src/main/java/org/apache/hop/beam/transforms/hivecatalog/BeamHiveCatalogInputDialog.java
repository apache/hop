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

package org.apache.hop.beam.transforms.hivecatalog;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class BeamHiveCatalogInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamHiveCatalogInput.class; // For Translator
  private final BeamHiveCatalogInputMeta input;

  int middle;
  int margin;

  private TextVar wMetastoreUri;

  private TextVar wMetastoreDatabase;

  private TextVar wMetastoreTable;

  public BeamHiveCatalogInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (BeamHiveCatalogInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamHiveCatalogInputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // metatore uri
    Label wlMetastoreUri = new Label(shell, SWT.RIGHT);
    wlMetastoreUri.setText(BaseMessages.getString(PKG, "BeamHiveCatalogInputDialog.MetaStoreUri"));
    PropsUi.setLook(wlMetastoreUri);
    FormData fdlmetastoreUri = new FormData();
    fdlmetastoreUri.left = new FormAttachment(0, 0);
    fdlmetastoreUri.top = new FormAttachment(lastControl, margin);
    fdlmetastoreUri.right = new FormAttachment(middle, -margin);
    wlMetastoreUri.setLayoutData(fdlmetastoreUri);
    wMetastoreUri = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMetastoreUri);
    FormData fdMetastoreUri = new FormData();
    fdMetastoreUri.left = new FormAttachment(middle, 0);
    fdMetastoreUri.top = new FormAttachment(wlMetastoreUri, 0, SWT.CENTER);
    fdMetastoreUri.right = new FormAttachment(100, 0);
    wMetastoreUri.setLayoutData(fdMetastoreUri);
    lastControl = wMetastoreUri;

    // Database
    Label wlMetastoreDatabase = new Label(shell, SWT.RIGHT);
    wlMetastoreDatabase.setText(
        BaseMessages.getString(PKG, "BeamHiveCatalogInputDialog.MetaStoreDatabase"));
    PropsUi.setLook(wlMetastoreDatabase);
    FormData fdlmetastoredatabase = new FormData();
    fdlmetastoredatabase.left = new FormAttachment(0, 0);
    fdlmetastoredatabase.top = new FormAttachment(lastControl, margin);
    fdlmetastoredatabase.right = new FormAttachment(middle, -margin);
    wlMetastoreDatabase.setLayoutData(fdlmetastoredatabase);
    wMetastoreDatabase = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMetastoreDatabase);
    FormData fdMetastoreDatabase = new FormData();
    fdMetastoreDatabase.left = new FormAttachment(middle, 0);
    fdMetastoreDatabase.top = new FormAttachment(wlMetastoreDatabase, 0, SWT.CENTER);
    fdMetastoreDatabase.right = new FormAttachment(100, 0);
    wMetastoreDatabase.setLayoutData(fdMetastoreDatabase);
    lastControl = wMetastoreDatabase;

    // Table
    Label wlMetastoreTable = new Label(shell, SWT.RIGHT);
    wlMetastoreTable.setText(
        BaseMessages.getString(PKG, "BeamHiveCatalogInputDialog.MetaStoreTable"));
    PropsUi.setLook(wlMetastoreTable);
    FormData fdlmetastoreTable = new FormData();
    fdlmetastoreTable.left = new FormAttachment(0, 0);
    fdlmetastoreTable.top = new FormAttachment(lastControl, margin);
    fdlmetastoreTable.right = new FormAttachment(middle, -margin);
    wlMetastoreTable.setLayoutData(fdlmetastoreTable);
    wMetastoreTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMetastoreTable);
    FormData fdMetastoreTable = new FormData();
    fdMetastoreTable.left = new FormAttachment(middle, 0);
    fdMetastoreTable.top = new FormAttachment(wlMetastoreTable, 0, SWT.CENTER);
    fdMetastoreTable.right = new FormAttachment(100, 0);
    wMetastoreTable.setLayoutData(fdMetastoreTable);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  public void getData() {
    wTransformName.setText(transformName);
    wMetastoreUri.setText(Const.NVL(input.getHiveMetastoreUris(), ""));
    wMetastoreDatabase.setText(Const.NVL(input.getHiveMetastoreDatabase(), ""));
    wMetastoreTable.setText(Const.NVL(input.getHiveMetastoreTable(), ""));
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    getInfo(input);

    dispose();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(BeamHiveCatalogInputMeta in) {
    transformName = wTransformName.getText(); // return value
    in.setHiveMetastoreUris(wMetastoreUri.getText());
    in.setHiveMetastoreDatabase(wMetastoreDatabase.getText());
    in.setHiveMetastoreTable(wMetastoreTable.getText());

    input.setChanged();
  }
}
