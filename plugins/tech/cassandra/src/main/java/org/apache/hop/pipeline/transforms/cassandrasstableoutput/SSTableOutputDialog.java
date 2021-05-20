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
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.io.File;

/** Dialog class for the SSTableOutput transform. */
public class SSTableOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = SSTableOutputMeta.class;

  private final SSTableOutputMeta input;

  private TextVar wYaml;

  private TextVar wDirectory;

  private TextVar wKeyspace;

  private TextVar wTable;

  private Label wlKeyField;
  private CCombo wKeyField;

  private TextVar wBufferSize;

  private Button wbGetFields;

  public SSTableOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String name) {

    super(parent, variables, (BaseTransformMeta) in, tr, name);

    input = (SSTableOutputMeta) in;
  }

  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.transformName.Label"));
    props.setLook(wlTransformName);

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fd);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fd);

    // yaml file line
    /** various UI bits and pieces for the dialog */
    Label wlYaml = new Label(shell, SWT.RIGHT);
    props.setLook(wlYaml);
    wlYaml.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.YAML.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlYaml.setLayoutData(fd);

    Button wbYaml = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbYaml);
    wbYaml.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.YAML.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    wbYaml.setLayoutData(fd);

    wbYaml.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            FileDialog dialog = new FileDialog(shell, SWT.OPEN);
            String[] extensions = null;
            String[] filterNames = null;

            extensions = new String[2];
            filterNames = new String[2];

            extensions[0] = "*.yaml";
            filterNames[0] = BaseMessages.getString(PKG, "SSTableOutputDialog.FileType.YAML");

            extensions[1] = "*";
            filterNames[1] = BaseMessages.getString(PKG, "System.FileType.AllFiles");

            dialog.setFilterExtensions(extensions);
            dialog.setFilterNames(filterNames);

            if (dialog.open() != null) {
              String path =
                  dialog.getFilterPath()
                      + System.getProperty("file.separator")
                      + dialog.getFileName();
              path = new File(path).toURI().toString();
              wYaml.setText(path);
            }
          }
        });

    wYaml = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wYaml);
    wYaml.addModifyListener(e -> wYaml.setToolTipText(variables.resolve(wYaml.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(wbYaml, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.left = new FormAttachment(middle, 0);
    wYaml.setLayoutData(fd);

    // directory line
    Label wlDirectory = new Label(shell, SWT.RIGHT);
    props.setLook(wlDirectory);
    wlDirectory.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.Directory.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wYaml, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlDirectory.setLayoutData(fd);

    Button wbDirectory = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbDirectory);
    wbDirectory.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.Directory.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wYaml, margin);
    wbDirectory.setLayoutData(fd);

    wbDirectory.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            DirectoryDialog dialog = new DirectoryDialog(shell, SWT.OPEN);

            if (dialog.open() != null) {
              String path = dialog.getFilterPath();
              path = new File(path).toURI().toString();
              wDirectory.setText(path);
            }
          }
        });

    wDirectory = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDirectory);
    wDirectory.addModifyListener(
        e -> wDirectory.setToolTipText(variables.resolve(wDirectory.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(wbDirectory, 0);
    fd.top = new FormAttachment(wYaml, margin);
    fd.left = new FormAttachment(middle, 0);
    wDirectory.setLayoutData(fd);

    // keyspace line
    Label wlKeyspace = new Label(shell, SWT.RIGHT);
    props.setLook(wlKeyspace);
    wlKeyspace.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.Keyspace.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wDirectory, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlKeyspace.setLayoutData(fd);

    wKeyspace = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wKeyspace);
    wKeyspace.addModifyListener(
        e -> wKeyspace.setToolTipText(variables.resolve(wKeyspace.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wDirectory, margin);
    fd.left = new FormAttachment(middle, 0);
    wKeyspace.setLayoutData(fd);

    // table line
    Label wlTable = new Label(shell, SWT.RIGHT);
    props.setLook(wlTable);
    wlTable.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.Table.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wKeyspace, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlTable.setLayoutData(fd);

    wTable = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTable);
    wTable.addModifyListener(e -> wTable.setToolTipText(variables.resolve(wTable.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wKeyspace, margin);
    fd.left = new FormAttachment(middle, 0);
    wTable.setLayoutData(fd);

    // key field line
    wlKeyField = new Label(shell, SWT.RIGHT);
    props.setLook(wlKeyField);
    wlKeyField.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.KeyField.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTable, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fd);

    wbGetFields = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.GetFields.Button"));

    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wTable, 0);
    wbGetFields.setLayoutData(fd);

    wbGetFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            showEnterSelectionDialog();
          }
        });

    wKeyField = new CCombo(shell, SWT.BORDER);
    wKeyField.addModifyListener(
        e -> wKeyField.setToolTipText(variables.resolve(wKeyField.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(wbGetFields, -margin);
    fd.top = new FormAttachment(wTable, margin);
    fd.left = new FormAttachment(middle, 0);
    wKeyField.setLayoutData(fd);

    // buffer size
    Label wlBufferSize = new Label(shell, SWT.RIGHT);
    props.setLook(wlBufferSize);
    wlBufferSize.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.BufferSize.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wKeyField, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlBufferSize.setLayoutData(fd);

    wBufferSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBufferSize);
    wBufferSize.addModifyListener(
        e -> wBufferSize.setToolTipText(variables.resolve(wBufferSize.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wKeyField, margin);
    fd.left = new FormAttachment(middle, 0);
    wBufferSize.setLayoutData(fd);

    // Buttons inherited from BaseTransformDialog
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wBufferSize);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void onCql3CheckSelection() {
    wbGetFields.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.SelectFields.Button"));
    wlKeyField.setText(BaseMessages.getString(PKG, "SSTableOutputDialog.KeyFields.Label"));
  }

  protected void setupFieldsCombo() {
    // try and set up from incoming fields from previous transform

    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

    if (transformMeta != null) {
      try {
        IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

        if (row.size() == 0) {
          MessageDialog.openError(
              shell,
              BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields.Title"),
              BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields"));

          return;
        }

        wKeyField.removeAll();
        for (int i = 0; i < row.size(); i++) {
          IValueMeta vm = row.getValueMeta(i);
          wKeyField.add(vm.getName());
        }
      } catch (HopException ex) {
        MessageDialog.openError(
            shell,
            BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields.Title"),
            BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields"));
      }
    }
  }

  protected void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();
    input.setYamlPath(wYaml.getText());
    input.setDirectory(wDirectory.getText());
    input.setCassandraKeyspace(wKeyspace.getText());
    input.setTableName(wTable.getText());
    input.setKeyField(wKeyField.getText());
    input.setBufferSize(wBufferSize.getText());

    input.setChanged();

    dispose();
  }

  protected void cancel() {
    transformName = null;
    dispose();
  }

  protected void getData() {

    if (!Utils.isEmpty(input.getYamlPath())) {
      wYaml.setText(input.getYamlPath());
    }

    if (!Utils.isEmpty(input.getDirectory())) {
      wDirectory.setText(input.getDirectory());
    }

    if (!Utils.isEmpty(input.getCassandraKeyspace())) {
      wKeyspace.setText(input.getCassandraKeyspace());
    }

    if (!Utils.isEmpty(input.getTableName())) {
      wTable.setText(input.getTableName());
    }

    if (!Utils.isEmpty(input.getKeyField())) {
      wKeyField.setText(input.getKeyField());
    }

    if (!Utils.isEmpty(input.getBufferSize())) {
      wBufferSize.setText(input.getBufferSize());
    }

    onCql3CheckSelection();
  }

  protected void showEnterSelectionDialog() {
    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

    String[] choices = null;
    if (transformMeta != null) {
      try {
        IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

        if (row.size() == 0) {
          MessageDialog.openError(
              shell,
              BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields.Title"),
              BaseMessages.getString(PKG, "SSTableOutputData.Message.NoIncomingFields"));

          return;
        }

        choices = new String[row.size()];
        for (int i = 0; i < row.size(); i++) {
          IValueMeta vm = row.getValueMeta(i);
          choices[i] = vm.getName();
        }

        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                shell,
                choices,
                BaseMessages.getString(PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Title"),
                BaseMessages.getString(PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Message"),
                370,
                280);
        dialog.setMulti(true);
        if (!Utils.isEmpty(wKeyField.getText())) {
          String current = wKeyField.getText();
          String[] parts = current.split(",");
          int[] currentSelection = new int[parts.length];
          int count = 0;
          for (String s : parts) {
            int index = row.indexOfValue(s.trim());
            if (index >= 0) {
              currentSelection[count++] = index;
            }
          }

          dialog.setSelectedNrs(currentSelection);
        }

        dialog.open();

        int[] selected = dialog.getSelectionIndeces(); // SIC
        if (selected != null && selected.length > 0) {
          StringBuilder newSelection = new StringBuilder();
          boolean first = true;
          for (int i : selected) {
            if (first) {
              newSelection.append(choices[i]);
              first = false;
            } else {
              newSelection.append(",").append(choices[i]);
            }
          }

          wKeyField.setText(newSelection.toString());
        }
      } catch (HopException ex) {
        MessageDialog.openError(
            shell,
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields.Title"),
            BaseMessages.getString(PKG, "CassandraOutputData.Message.NoIncomingFields"));
      }
    }
  }
}
