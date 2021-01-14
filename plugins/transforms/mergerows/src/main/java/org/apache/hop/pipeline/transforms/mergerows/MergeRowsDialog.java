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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class MergeRowsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MergeRowsMeta.class; // For Translator
  public static final String STRING_SORT_WARNING_PARAMETER = "MergeRowsSortWarning";

  private CCombo wReference;

  private CCombo wCompare;

  private Text wFlagField;

  private TableView wKeys;

  private TableView wValues;

  private final MergeRowsMeta input;

  public MergeRowsDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (MergeRowsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Shell.Label"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    Button wbKeys = new Button(shell, SWT.PUSH);
    wbKeys.setText(BaseMessages.getString(PKG, "MergeRowsDialog.KeyFields.Button"));
    FormData fdbKeys = new FormData();
    fdbKeys.bottom = new FormAttachment(wOk, -2 * margin);
    fdbKeys.left = new FormAttachment(0, 0);
    fdbKeys.right = new FormAttachment(50, -margin);
    wbKeys.setLayoutData(fdbKeys);
    wbKeys.addSelectionListener(
        new SelectionAdapter() {

          public void widgetSelected(SelectionEvent e) {
            getKeys();
          }
        });

    Button wbValues = new Button(shell, SWT.PUSH);
    wbValues.setText(BaseMessages.getString(PKG, "MergeRowsDialog.ValueFields.Button"));
    FormData fdbValues = new FormData();
    fdbValues.bottom = new FormAttachment(wOk, -2 * margin);
    fdbValues.left = new FormAttachment(50, 0);
    fdbValues.right = new FormAttachment(100, 0);
    wbValues.setLayoutData(fdbValues);
    wbValues.addSelectionListener(
        new SelectionAdapter() {

          public void widgetSelected(SelectionEvent e) {
            getValues();
          }
        });

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MergeRowsDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Get the previous transforms...
    String[] previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    // Send 'True' data to...
    Label wlReference = new Label(shell, SWT.RIGHT);
    wlReference.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Reference.Label"));
    props.setLook(wlReference);
    FormData fdlReference = new FormData();
    fdlReference.left = new FormAttachment(0, 0);
    fdlReference.right = new FormAttachment(middle, -margin);
    fdlReference.top = new FormAttachment(wTransformName, margin);
    wlReference.setLayoutData(fdlReference);
    wReference = new CCombo(shell, SWT.BORDER);
    props.setLook(wReference);

    if (previousTransforms != null) {
      wReference.setItems(previousTransforms);
    }

    wReference.addModifyListener(lsMod);
    FormData fdReference = new FormData();
    fdReference.left = new FormAttachment(middle, 0);
    fdReference.top = new FormAttachment(wTransformName, margin);
    fdReference.right = new FormAttachment(100, 0);
    wReference.setLayoutData(fdReference);

    // Send 'False' data to...
    Label wlCompare = new Label(shell, SWT.RIGHT);
    wlCompare.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Compare.Label"));
    props.setLook(wlCompare);
    FormData fdlCompare = new FormData();
    fdlCompare.left = new FormAttachment(0, 0);
    fdlCompare.right = new FormAttachment(middle, -margin);
    fdlCompare.top = new FormAttachment(wReference, margin);
    wlCompare.setLayoutData(fdlCompare);
    wCompare = new CCombo(shell, SWT.BORDER);
    props.setLook(wCompare);

    if (previousTransforms != null) {
      wCompare.setItems(previousTransforms);
    }

    wCompare.addModifyListener(lsMod);
    FormData fdCompare = new FormData();
    fdCompare.top = new FormAttachment(wReference, margin);
    fdCompare.left = new FormAttachment(middle, 0);
    fdCompare.right = new FormAttachment(100, 0);
    wCompare.setLayoutData(fdCompare);

    // TransformName line
    Label wlFlagfield = new Label(shell, SWT.RIGHT);
    wlFlagfield.setText(BaseMessages.getString(PKG, "MergeRowsDialog.FlagField.Label"));
    props.setLook(wlFlagfield);
    FormData fdlFlagfield = new FormData();
    fdlFlagfield.left = new FormAttachment(0, 0);
    fdlFlagfield.right = new FormAttachment(middle, -margin);
    fdlFlagfield.top = new FormAttachment(wCompare, margin);
    wlFlagfield.setLayoutData(fdlFlagfield);
    wFlagField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFlagField);
    wFlagField.addModifyListener(lsMod);
    FormData fdFlagfield = new FormData();
    fdFlagfield.top = new FormAttachment(wCompare, margin);
    fdFlagfield.left = new FormAttachment(middle, 0);
    fdFlagfield.right = new FormAttachment(100, 0);
    wFlagField.setLayoutData(fdFlagfield);

    // THE KEYS TO MATCH...
    Label wlKeys = new Label(shell, SWT.NONE);
    wlKeys.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Keys.Label"));
    props.setLook(wlKeys);
    FormData fdlKeys = new FormData();
    fdlKeys.left = new FormAttachment(0, 0);
    fdlKeys.top = new FormAttachment(wFlagField, margin);
    wlKeys.setLayoutData(fdlKeys);

    int nrKeyRows = (input.getKeyFields() != null ? input.getKeyFields().length : 1);

    ColumnInfo[] ciKeys =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MergeRowsDialog.ColumnInfo.KeyField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wKeys =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKeys,
            nrKeyRows,
            lsMod,
            props);

    FormData fdKeys = new FormData();
    fdKeys.top = new FormAttachment(wlKeys, margin);
    fdKeys.left = new FormAttachment(0, 0);
    fdKeys.bottom = new FormAttachment(wbKeys, -margin);
    fdKeys.right = new FormAttachment(50, -margin);
    wKeys.setLayoutData(fdKeys);

    // VALUES TO COMPARE
    Label wlValues = new Label(shell, SWT.NONE);
    wlValues.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Values.Label"));
    props.setLook(wlValues);
    FormData fdlValues = new FormData();
    fdlValues.left = new FormAttachment(50, 0);
    fdlValues.top = new FormAttachment(wFlagField, margin);
    wlValues.setLayoutData(fdlValues);

    int nrValueRows = (input.getValueFields() != null ? input.getValueFields().length : 1);

    ColumnInfo[] ciValues =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MergeRowsDialog.ColumnInfo.ValueField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wValues =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciValues,
            nrValueRows,
            lsMod,
            props);

    FormData fdValues = new FormData();
    fdValues.top = new FormAttachment(wlValues, margin);
    fdValues.left = new FormAttachment(50, 0);
    fdValues.bottom = new FormAttachment(wbValues, -margin);
    fdValues.right = new FormAttachment(100, 0);
    wValues.setLayoutData(fdValues);

    // Add listeners

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(backupChanged);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();

    wReference.setText(Const.NVL(infoStreams.get(0).getTransformName(), ""));
    wCompare.setText(Const.NVL(infoStreams.get(1).getTransformName(), ""));
    if (input.getFlagField() != null) {
      wFlagField.setText(input.getFlagField());
    }

    for (int i = 0; i < input.getKeyFields().length; i++) {
      TableItem item = wKeys.table.getItem(i);
      if (input.getKeyFields()[i] != null) {
        item.setText(1, input.getKeyFields()[i]);
      }
    }
    for (int i = 0; i < input.getValueFields().length; i++) {
      TableItem item = wValues.table.getItem(i);
      if (input.getValueFields()[i] != null) {
        item.setText(1, input.getValueFields()[i]);
      }
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    infoStreams.get(0).setTransformMeta(pipelineMeta.findTransform(wReference.getText()));
    infoStreams.get(1).setTransformMeta(pipelineMeta.findTransform(wCompare.getText()));
    input.setFlagField(wFlagField.getText());

    int nrKeys = wKeys.nrNonEmpty();
    int nrValues = wValues.nrNonEmpty();

    input.allocate(nrKeys, nrValues);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrKeys; i++) {
      TableItem item = wKeys.getNonEmpty(i);
      input.getKeyFields()[i] = item.getText(1);
    }

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrValues; i++) {
      TableItem item = wValues.getNonEmpty(i);
      input.getValueFields()[i] = item.getText(1);
    }

    transformName = wTransformName.getText(); // return value

    // PDI-13509 Fix
    if (nrKeys > 0
        && "Y".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "MergeRowsDialog.MergeRowsWarningDialog.DialogTitle"),
              BaseMessages.getString(
                      PKG, "MergeRowsDialog.MergeRowsWarningDialog.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "MergeRowsDialog.MergeRowsWarningDialog.Option1")
              },
              BaseMessages.getString(PKG, "MergeRowsDialog.MergeRowsWarningDialog.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }

    dispose();
  }

  private void getKeys() {
    try {
      TransformMeta transformMeta = pipelineMeta.findTransform(wReference.getText());
      if (transformMeta != null) {
        IRowMeta prev = pipelineMeta.getTransformFields(variables, transformMeta);
        if (prev != null) {
          BaseTransformDialog.getFieldsFromPrevious(
              prev, wKeys, 1, new int[] {1}, new int[] {}, -1, -1, null);
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MergeRowsDialog.ErrorGettingFields.DialogTitle"),
          BaseMessages.getString(PKG, "MergeRowsDialog.ErrorGettingFields.DialogMessage"),
          e);
    }
  }

  private void getValues() {
    try {
      TransformMeta transformMeta = pipelineMeta.findTransform(wReference.getText());
      if (transformMeta != null) {
        IRowMeta prev = pipelineMeta.getTransformFields(variables, transformMeta);
        if (prev != null) {
          BaseTransformDialog.getFieldsFromPrevious(
              prev, wValues, 1, new int[] {1}, new int[] {}, -1, -1, null);
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MergeRowsDialog.ErrorGettingFields.DialogTitle"),
          BaseMessages.getString(PKG, "MergeRowsDialog.ErrorGettingFields.DialogMessage"),
          e);
    }
  }
}
