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

package org.apache.hop.pipeline.transforms.mergerows;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class MergeRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = MergeRowsMeta.class;
  public static final String STRING_SORT_WARNING_PARAMETER = "MergeRowsSortWarning";

  private static final String YES = BaseMessages.getString("System.Combo.Yes");
  private static final String NO = BaseMessages.getString("System.Combo.No");

  private CTabFolder wTabFolder;

  private CCombo wReference;
  private CCombo wCompare;
  private Text wFlagField;
  private Text wDiffField;

  private TableView wKeys;
  private TableView wValues;
  private TableView wExtra;

  private final MergeRowsMeta input;

  public MergeRowsDialog(
      Shell parent, IVariables variables, MergeRowsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MergeRowsDialog.Shell.Label"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    // We want 4 tabs: Sources, Keys, Values, Extra
    //
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addSourcesTab();
    addKeysTab();
    addValuesTab();
    addExtraTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.setSelection(0);

    getData();
    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addSourcesTab() {
    CTabItem wSourcesTab = new CTabItem(wTabFolder, SWT.NONE);
    wSourcesTab.setFont(GuiResource.getInstance().getFontDefault());
    wSourcesTab.setText(BaseMessages.getString(PKG, "MergeRowsDialog.SourcesTab.CTabItem"));

    Composite wSourcesComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSourcesComp);
    wSourcesComp.setLayout(props.createFormLayout());

    // Get the previous transforms...
    String[] previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    // Send 'True' data to...
    Label wlReference = new Label(wSourcesComp, SWT.RIGHT);
    wlReference.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Reference.Label"));
    PropsUi.setLook(wlReference);
    FormData fdlReference = new FormData();
    fdlReference.left = new FormAttachment(0, 0);
    fdlReference.right = new FormAttachment(middle, -margin);
    fdlReference.top = new FormAttachment(wSpacer, margin);
    wlReference.setLayoutData(fdlReference);
    wReference = new CCombo(wSourcesComp, SWT.BORDER);
    PropsUi.setLook(wReference);
    if (previousTransforms != null) {
      wReference.setItems(previousTransforms);
    }
    wReference.addModifyListener(lsMod);
    FormData fdReference = new FormData();
    fdReference.left = new FormAttachment(middle, 0);
    fdReference.top = new FormAttachment(wSpacer, margin);
    fdReference.right = new FormAttachment(100, 0);
    wReference.setLayoutData(fdReference);

    // Send 'False' data to...
    Label wlCompare = new Label(wSourcesComp, SWT.RIGHT);
    wlCompare.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Compare.Label"));
    PropsUi.setLook(wlCompare);
    FormData fdlCompare = new FormData();
    fdlCompare.left = new FormAttachment(0, 0);
    fdlCompare.right = new FormAttachment(middle, -margin);
    fdlCompare.top = new FormAttachment(wReference, margin);
    wlCompare.setLayoutData(fdlCompare);
    wCompare = new CCombo(wSourcesComp, SWT.BORDER);
    PropsUi.setLook(wCompare);
    if (previousTransforms != null) {
      wCompare.setItems(previousTransforms);
    }
    wCompare.addModifyListener(lsMod);
    FormData fdCompare = new FormData();
    fdCompare.top = new FormAttachment(wReference, margin);
    fdCompare.left = new FormAttachment(middle, 0);
    fdCompare.right = new FormAttachment(100, 0);
    wCompare.setLayoutData(fdCompare);

    // The flag field line
    Label wlFlagField = new Label(wSourcesComp, SWT.RIGHT);
    wlFlagField.setText(BaseMessages.getString(PKG, "MergeRowsDialog.FlagField.Label"));
    PropsUi.setLook(wlFlagField);
    FormData fdlFlagField = new FormData();
    fdlFlagField.left = new FormAttachment(0, 0);
    fdlFlagField.right = new FormAttachment(middle, -margin);
    fdlFlagField.top = new FormAttachment(wCompare, margin);
    wlFlagField.setLayoutData(fdlFlagField);
    wFlagField = new Text(wSourcesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFlagField);
    wFlagField.addModifyListener(lsMod);
    FormData fdFlagField = new FormData();
    fdFlagField.top = new FormAttachment(wCompare, margin);
    fdFlagField.left = new FormAttachment(middle, 0);
    fdFlagField.right = new FormAttachment(100, 0);
    wFlagField.setLayoutData(fdFlagField);

    // The flag field line
    Label wlDiffField = new Label(wSourcesComp, SWT.RIGHT);
    wlDiffField.setText(BaseMessages.getString(PKG, "MergeRowsDialog.DiffField.Label"));
    PropsUi.setLook(wlDiffField);
    FormData fdlDiffField = new FormData();
    fdlDiffField.left = new FormAttachment(0, 0);
    fdlDiffField.right = new FormAttachment(middle, -margin);
    fdlDiffField.top = new FormAttachment(wFlagField, margin);
    wlDiffField.setLayoutData(fdlDiffField);
    wDiffField = new Text(wSourcesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDiffField);
    wDiffField.addModifyListener(lsMod);
    FormData fdDiffField = new FormData();
    fdDiffField.top = new FormAttachment(wFlagField, margin);
    fdDiffField.left = new FormAttachment(middle, 0);
    fdDiffField.right = new FormAttachment(100, 0);
    wDiffField.setLayoutData(fdDiffField);

    FormData fdSourcesComp = new FormData();
    fdSourcesComp.left = new FormAttachment(0, 0);
    fdSourcesComp.top = new FormAttachment(0, 0);
    fdSourcesComp.right = new FormAttachment(100, 0);
    fdSourcesComp.bottom = new FormAttachment(100, 0);
    wSourcesComp.setLayoutData(fdSourcesComp);

    wSourcesComp.layout();
    wSourcesTab.setControl(wSourcesComp);
  }

  private void addKeysTab() {
    CTabItem wKeysTab = new CTabItem(wTabFolder, SWT.NONE);
    wKeysTab.setFont(GuiResource.getInstance().getFontDefault());
    wKeysTab.setText(BaseMessages.getString(PKG, "MergeRowsDialog.KeysTab.CTabItem"));

    Composite wKeysComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wKeysComp);
    wKeysComp.setLayout(props.createFormLayout());

    // The [Get key fields] button at the bottom:
    Button wbKeys = new Button(wKeysComp, SWT.PUSH);
    wbKeys.setText(BaseMessages.getString(PKG, "MergeRowsDialog.KeyFields.Button"));
    FormData fdbKeys = new FormData();
    fdbKeys.bottom = new FormAttachment(100, -margin);
    fdbKeys.left = new FormAttachment(0, 0);
    wbKeys.setLayoutData(fdbKeys);
    wbKeys.addListener(SWT.Selection, e -> getKeys());

    // THE KEYS TO MATCH...

    Label wlKeys = new Label(wKeysComp, SWT.NONE);
    wlKeys.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Keys.Label"));
    PropsUi.setLook(wlKeys);
    FormData fdlKeys = new FormData();
    fdlKeys.left = new FormAttachment(0, 0);
    fdlKeys.top = new FormAttachment(0, 0);
    wlKeys.setLayoutData(fdlKeys);

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
            wKeysComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKeys,
            1,
            lsMod,
            props);

    FormData fdKeys = new FormData();
    fdKeys.top = new FormAttachment(wlKeys, margin);
    fdKeys.left = new FormAttachment(0, 0);
    fdKeys.bottom = new FormAttachment(wbKeys, -margin);
    fdKeys.right = new FormAttachment(100, 0);
    wKeys.setLayoutData(fdKeys);

    FormData fdKeysComp = new FormData();
    fdKeysComp.left = new FormAttachment(0, 0);
    fdKeysComp.top = new FormAttachment(0, 0);
    fdKeysComp.right = new FormAttachment(100, 0);
    fdKeysComp.bottom = new FormAttachment(100, 0);
    wKeysComp.setLayoutData(fdKeysComp);

    wKeysComp.layout();
    wKeysTab.setControl(wKeysComp);
  }

  private void addValuesTab() {
    CTabItem wValuesTab = new CTabItem(wTabFolder, SWT.NONE);
    wValuesTab.setFont(GuiResource.getInstance().getFontDefault());
    wValuesTab.setText(BaseMessages.getString(PKG, "MergeRowsDialog.ValuesTab.CTabItem"));

    Composite wValuesComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wValuesComp);
    wValuesComp.setLayout(props.createFormLayout());

    Button wbValues = new Button(wValuesComp, SWT.PUSH);
    wbValues.setText(BaseMessages.getString(PKG, "MergeRowsDialog.ValueFields.Button"));
    FormData fdbValues = new FormData();
    fdbValues.bottom = new FormAttachment(100, -margin);
    fdbValues.left = new FormAttachment(0, 0);
    wbValues.setLayoutData(fdbValues);
    wbValues.addListener(SWT.Selection, e -> getValues());

    // VALUES TO COMPARE
    Label wlValues = new Label(wValuesComp, SWT.NONE);
    wlValues.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Values.Label"));
    PropsUi.setLook(wlValues);
    FormData fdlValues = new FormData();
    fdlValues.left = new FormAttachment(50, 0);
    fdlValues.top = new FormAttachment(wDiffField, margin);
    wlValues.setLayoutData(fdlValues);

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
            wValuesComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciValues,
            1,
            lsMod,
            props);

    FormData fdValues = new FormData();
    fdValues.top = new FormAttachment(wlValues, margin);
    fdValues.left = new FormAttachment(0, 0);
    fdValues.bottom = new FormAttachment(wbValues, -margin);
    fdValues.right = new FormAttachment(100, 0);
    wValues.setLayoutData(fdValues);

    FormData fdValuesComp = new FormData();
    fdValuesComp.left = new FormAttachment(0, 0);
    fdValuesComp.top = new FormAttachment(0, 0);
    fdValuesComp.right = new FormAttachment(100, 0);
    fdValuesComp.bottom = new FormAttachment(100, 0);
    wValuesComp.setLayoutData(fdValuesComp);

    wValuesComp.layout();
    wValuesTab.setControl(wValuesComp);
  }

  private void addExtraTab() {
    CTabItem wExtraTab = new CTabItem(wTabFolder, SWT.NONE);
    wExtraTab.setFont(GuiResource.getInstance().getFontDefault());
    wExtraTab.setText(BaseMessages.getString(PKG, "MergeRowsDialog.ExtraTab.CTabItem"));

    Composite wExtraComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wExtraComp);
    wExtraComp.setLayout(props.createFormLayout());

    // The [Get fields] button at the bottom
    Button wbExtra = new Button(wExtraComp, SWT.PUSH);
    wbExtra.setText(BaseMessages.getString(PKG, "MergeRowsDialog.ExtraFields.Button"));
    FormData fdbExtra = new FormData();
    fdbExtra.bottom = new FormAttachment(100, -margin);
    fdbExtra.left = new FormAttachment(0, 0);
    wbExtra.setLayoutData(fdbExtra);
    wbExtra.addListener(SWT.Selection, e -> getPassThroughFields());

    // Extra fields to pass through to the output
    Label wlExtra = new Label(wExtraComp, SWT.NONE);
    wlExtra.setText(BaseMessages.getString(PKG, "MergeRowsDialog.Extra.Label"));
    PropsUi.setLook(wlExtra);
    FormData fdlExtra = new FormData();
    fdlExtra.left = new FormAttachment(50, 0);
    fdlExtra.top = new FormAttachment(wDiffField, margin);
    wlExtra.setLayoutData(fdlExtra);

    ColumnInfo[] ciExtra =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MergeRowsDialog.ExtraColumn.Reference.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES,
              NO),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MergeRowsDialog.ExtraColumn.Field.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MergeRowsDialog.ExtraColumn.RenameTo.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wExtra =
        new TableView(
            variables,
            wExtraComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciExtra,
            1,
            lsMod,
            props);

    FormData fdExtra = new FormData();
    fdExtra.top = new FormAttachment(wlExtra, margin);
    fdExtra.left = new FormAttachment(0, 0);
    fdExtra.bottom = new FormAttachment(wbExtra, -margin);
    fdExtra.right = new FormAttachment(100, 0);
    wExtra.setLayoutData(fdExtra);

    FormData fdExtraComp = new FormData();
    fdExtraComp.left = new FormAttachment(0, 0);
    fdExtraComp.top = new FormAttachment(0, 0);
    fdExtraComp.right = new FormAttachment(100, 0);
    fdExtraComp.bottom = new FormAttachment(100, 0);
    wExtraComp.setLayoutData(fdExtraComp);

    wExtraComp.layout();
    wExtraTab.setControl(wExtraComp);
  }

  private void getPassThroughFields() {
    MergeRowsMeta meta = new MergeRowsMeta();
    getInfo(meta);

    // Get all fields from both reference and compare transforms
    // and add them to the wExtra table view.
    try {
      IRowMeta refRowMeta =
          pipelineMeta.getTransformFields(variables, meta.getReferenceTransform());
      for (IValueMeta refValueMeta : refRowMeta.getValueMetaList()) {
        TableItem item = new TableItem(wExtra.table, SWT.NONE);
        item.setText(1, YES);
        item.setText(2, refValueMeta.getName());
        item.setText(3, "ref-" + refValueMeta.getName());
      }
      IRowMeta cmpRowMeta = pipelineMeta.getTransformFields(variables, meta.getCompareTransform());
      for (IValueMeta cmpValueMeta : cmpRowMeta.getValueMetaList()) {
        TableItem item = new TableItem(wExtra.table, SWT.NONE);
        item.setText(1, NO);
        item.setText(2, cmpValueMeta.getName());
        item.setText(3, "cmp-" + cmpValueMeta.getName());
      }
      wExtra.optimizeTableView();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error adding passthrough fields", e);
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();

    wReference.setText(Const.NVL(infoStreams.get(0).getTransformName(), ""));
    wCompare.setText(Const.NVL(infoStreams.get(1).getTransformName(), ""));
    wFlagField.setText(Const.NVL(input.getFlagField(), ""));
    wDiffField.setText(Const.NVL(input.getDiffJsonField(), ""));

    for (int i = 0; i < input.getKeyFields().size(); i++) {
      TableItem item = new TableItem(wKeys.table, SWT.NONE);
      item.setText(1, Const.NVL(input.getKeyFields().get(i), ""));
    }
    wKeys.optimizeTableView();
    for (int i = 0; i < input.getValueFields().size(); i++) {
      TableItem item = new TableItem(wValues.table, SWT.NONE);
      item.setText(1, Const.NVL(input.getValueFields().get(i), ""));
    }
    wValues.optimizeTableView();
    for (int i = 0; i < input.getPassThroughFields().size(); i++) {
      PassThroughField field = input.getPassThroughFields().get(i);
      TableItem item = new TableItem(wExtra.table, SWT.NONE);
      item.setText(1, field.isReferenceField() ? YES : NO);
      item.setText(2, Const.NVL(field.getSourceField(), ""));
      item.setText(3, Const.NVL(field.getRenameTo(), ""));
    }
    wExtra.optimizeTableView();
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
    getInfo(input);

    transformName = wTransformName.getText(); // return value

    if (!input.getKeyFields().isEmpty()
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

  private void getInfo(MergeRowsMeta meta) {
    meta.setReferenceTransform(wReference.getText());
    meta.setCompareTransform(wCompare.getText());
    List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
    infoStreams.get(0).setTransformMeta(pipelineMeta.findTransform(wReference.getText()));
    infoStreams.get(1).setTransformMeta(pipelineMeta.findTransform(wCompare.getText()));
    meta.setFlagField(wFlagField.getText());
    meta.setDiffJsonField(wDiffField.getText());

    meta.getKeyFields().clear();
    for (TableItem item : wKeys.getNonEmptyItems()) {
      meta.getKeyFields().add(item.getText(1));
    }

    meta.getValueFields().clear();
    for (TableItem item : wValues.getNonEmptyItems()) {
      meta.getValueFields().add(item.getText(1));
    }

    meta.getPassThroughFields().clear();
    for (TableItem item : wExtra.getNonEmptyItems()) {
      boolean reference = YES.equalsIgnoreCase(item.getText(1));
      String fieldName = item.getText(2);
      String renameTo = item.getText(3);
      meta.getPassThroughFields().add(new PassThroughField(fieldName, renameTo, reference));
    }
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
