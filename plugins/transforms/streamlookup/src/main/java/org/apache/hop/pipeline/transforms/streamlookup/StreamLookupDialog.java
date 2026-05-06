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

package org.apache.hop.pipeline.transforms.streamlookup;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class StreamLookupDialog extends BaseTransformDialog {
  private static final Class<?> PKG = StreamLookupMeta.class;

  private Combo wTransform;

  private TableView wKey;

  private TableView wReturn;

  private Button wPreserveMemory;

  private Button wSortedList;

  private Button wIntegerPair;

  private final StreamLookupMeta input;

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  public StreamLookupDialog(
      Shell parent,
      IVariables variables,
      StreamLookupMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  private void addGeneralTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "StreamLookupDialog.GeneralTab.Title"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    // Lookup transform line...
    Label wlTransform = new Label(composite, SWT.RIGHT);
    wlTransform.setText(BaseMessages.getString(PKG, "StreamLookupDialog.LookupTransform.Label"));
    wlTransform.setLayoutData(
        FormDataBuilder.builder().left().right(middle, -margin).top(wSpacer, margin).result());
    PropsUi.setLook(wlTransform);
    wTransform = new Combo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransform.setLayoutData(
        FormDataBuilder.builder().left(middle, 0).top(wSpacer, margin).right().result());
    PropsUi.setLook(wTransform);

    List<TransformMeta> previousTransforms =
        pipelineMeta.findPreviousTransforms(transformMeta, true);
    for (TransformMeta previousTransform : previousTransforms) {
      wTransform.add(previousTransform.getName());
    }

    wTransform.addModifyListener(lsMod);
    wTransform.addListener(
        SWT.Selection,
        e -> {
          input.setChanged();
          updateComboFields();
        });

    Label wlPreserveMemory = new Label(composite, SWT.RIGHT);
    wlPreserveMemory.setText(
        BaseMessages.getString(PKG, "StreamLookupDialog.PreserveMemory.Label"));
    wlPreserveMemory.setLayoutData(
        FormDataBuilder.builder().left().top(wTransform, margin).right(middle, -margin).result());
    PropsUi.setLook(wlPreserveMemory);
    wPreserveMemory = new Button(composite, SWT.CHECK);
    wPreserveMemory.setLayoutData(
        FormDataBuilder.builder()
            .left(middle, 0)
            .top(wlPreserveMemory, 0, SWT.CENTER)
            .right()
            .result());
    wPreserveMemory.addListener(SWT.Selection, e -> input.setChanged());

    // preserve memory should be enabled to have this options on.
    wPreserveMemory.addListener(
        SWT.Selection,
        event -> {
          boolean selection = wPreserveMemory.getSelection();
          wSortedList.setEnabled(selection);
          wIntegerPair.setEnabled(selection);
        });
    PropsUi.setLook(wPreserveMemory);

    Label wlIntegerPair = new Label(composite, SWT.RIGHT);
    wlIntegerPair.setText(BaseMessages.getString(PKG, "StreamLookupDialog.IntegerPair.Label"));
    PropsUi.setLook(wlIntegerPair);
    wlIntegerPair.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .top(wPreserveMemory, margin)
            .right(middle, -margin)
            .result());

    wIntegerPair = new Button(composite, SWT.RADIO);
    wIntegerPair.setEnabled(false);
    PropsUi.setLook(wIntegerPair);
    wIntegerPair.setLayoutData(
        FormDataBuilder.builder()
            .left(middle, 0)
            .top(wlIntegerPair, 0, SWT.CENTER)
            .right()
            .result());
    wIntegerPair.addListener(SWT.Selection, e -> input.setChanged());

    Label wlSortedList = new Label(composite, SWT.RIGHT);
    wlSortedList.setText(BaseMessages.getString(PKG, "StreamLookupDialog.SortedList.Label"));
    wlSortedList.setLayoutData(
        FormDataBuilder.builder().left().top(wIntegerPair, margin).right(middle, -margin).result());
    PropsUi.setLook(wlSortedList);

    wSortedList = new Button(composite, SWT.RADIO);
    wSortedList.setEnabled(false);
    wSortedList.setLayoutData(
        FormDataBuilder.builder()
            .left(middle, 0)
            .top(wlSortedList, 0, SWT.CENTER)
            .right()
            .result());
    wSortedList.addListener(SWT.Selection, e -> input.setChanged());
    PropsUi.setLook(wSortedList);

    wGeneralTab.setControl(composite);
  }

  private void addKeysTab(CTabFolder wTabFolder, int margin, ModifyListener lsMod) {
    CTabItem wKeysTab = new CTabItem(wTabFolder, SWT.NONE);
    wKeysTab.setFont(GuiResource.getInstance().getFontDefault());
    wKeysTab.setText(BaseMessages.getString(PKG, "StreamLookupDialog.KeysTab.Title"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    Label wlKey = new Label(composite, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "StreamLookupDialog.Key.Label"));
    wlKey.setLayoutData(FormDataBuilder.builder().left().top().result());
    PropsUi.setLook(wlKey);

    Button wGetKeyFields = new Button(composite, SWT.PUSH);
    wGetKeyFields.setText(BaseMessages.getString(PKG, "StreamLookupDialog.GetKeyFields.Button"));
    wGetKeyFields.addListener(SWT.Selection, e -> getKeyFields());
    setButtonPositions(new Button[] {wGetKeyFields}, margin, null);
    PropsUi.setLook(wGetKeyFields);

    int nrKeyCols = 2;

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.Field"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.LookupField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    wKey =
        new TableView(
            variables,
            composite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            1,
            lsMod,
            props);

    wKey.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .top(wlKey, margin)
            .right()
            .bottom(wGetKeyFields, -margin)
            .result());

    wKeysTab.setControl(composite);
  }

  private void addFieldsTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "StreamLookupDialog.ReturnFieldsTab.Title"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    composite.setLayout(props.createFormLayout());
    PropsUi.setLook(composite);

    Label wlReturn = new Label(composite, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "StreamLookupDialog.ReturnFields.Label"));
    wlReturn.setLayoutData(FormDataBuilder.builder().left().top().result());
    PropsUi.setLook(wlReturn);

    Button wGetLookupFields = new Button(composite, SWT.PUSH);
    wGetLookupFields.setText(
        BaseMessages.getString(PKG, "StreamLookupDialog.GetLookupFields.Button"));
    wGetLookupFields.addListener(SWT.Selection, e -> getLookupFields());
    PropsUi.setLook(wGetLookupFields);
    setButtonPositions(new Button[] {wGetLookupFields}, margin, null);

    int upInsCols = 4;

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.FieldReturn"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.NewName"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.Default"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[2].setUsingVariables(true);
    ciReturn[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamLookupDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());

    wReturn =
        new TableView(
            variables,
            composite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            1,
            lsMod,
            props);

    wReturn.setLayoutData(
        FormDataBuilder.builder()
            .top(wlReturn, margin)
            .bottom(wGetLookupFields, -margin)
            .fullWidth()
            .result());

    wFieldsTab.setControl(composite);
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "StreamLookupDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, middle, margin, lsMod);
    addKeysTab(wTabFolder, margin, lsMod);
    addFieldsTab(wTabFolder, middle, margin, lsMod);

    wTabFolder.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .top(wSpacer, margin)
            .right()
            .bottom(wOk, -margin)
            .result());
    wTabFolder.setSelection(0);

    getData();

    updateComboFields();
    input.setChanged(changed);
    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** /* Search the input and lookup fields in the background */
  protected void updateComboFields() {

    final Runnable runnable =
        () -> {
          String lookupTransformName = wTransform.getText();

          // Input fields
          //
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta rowMeta = new RowMeta();
              for (String prevTransformName : pipelineMeta.getPrevTransformNames(transformName)) {
                if (!prevTransformName.equalsIgnoreCase(lookupTransformName)) {
                  IRowMeta row =
                      pipelineMeta.getPrevTransformFields(
                          variables, transformMeta, prevTransformName, null);

                  // See if the add fields are not already in the row
                  for (int i = 0; i < row.size(); i++) {
                    IValueMeta valueMeta = row.getValueMeta(i);
                    if (rowMeta.searchValueMeta(valueMeta.getName()) == null) {
                      rowMeta.addValueMeta(valueMeta);
                    }
                  }
                }
              }

              String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());

              // return fields
              ciKey[0].setComboValues(fieldNames);
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }

          // Lookup fields
          //
          TransformMeta lookupTransformMeta = pipelineMeta.findTransform(lookupTransformName);
          if (lookupTransformMeta != null) {
            try {
              IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, lookupTransformMeta);

              String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());

              // return fields
              ciReturn[0].setComboValues(fieldNames);
              ciKey[1].setComboValues(fieldNames);
            } catch (HopException e) {
              logError(
                  "It was not possible to retrieve the list of fields for transform ["
                      + wTransform.getText()
                      + "]!");
            }
          }
        };
    shell.getDisplay().asyncExec(runnable);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "StreamLookupDialog.Log.GettingKeyInfo"));
    }

    for (StreamLookupMeta.MatchKey matchKey : input.getLookup().getMatchKeys()) {
      TableItem item = new TableItem(wKey.table, SWT.NONE);
      item.setText(1, Const.NVL(matchKey.getKeyStream(), ""));
      item.setText(2, Const.NVL(matchKey.getKeyLookup(), ""));
    }
    wKey.optimizeTableView();

    for (StreamLookupMeta.ReturnValue returnValue : input.getLookup().getReturnValues()) {
      TableItem item = new TableItem(wReturn.table, SWT.NONE);
      item.setText(1, Const.NVL(returnValue.getValue(), ""));
      if (StringUtils.isNotEmpty(returnValue.getValueName())
          && !returnValue.getValueName().equalsIgnoreCase(returnValue.getValue())) {
        item.setText(2, returnValue.getValueName());
      }
      item.setText(3, Const.NVL(returnValue.getValueDefault(), ""));
      item.setText(4, ValueMetaFactory.getValueMetaName(returnValue.getValueDefaultType()));
    }
    wReturn.optimizeTableView();

    wTransform.setText(Const.NVL(input.getSourceTransformName(), ""));

    boolean isPreserveMemory = input.isMemoryPreservationActive();
    wPreserveMemory.setSelection(isPreserveMemory);
    if (isPreserveMemory) {
      wSortedList.setEnabled(true);
      wIntegerPair.setEnabled(true);
    }
    // usually this is sorted list or integer pair
    // for backward compatibility they can be set both
    // but user will be forced to choose only one option later.
    wSortedList.setSelection(input.isUsingSortedList());
    wIntegerPair.setSelection(input.isUsingIntegerPair());
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

    input.setMemoryPreservationActive(wPreserveMemory.getSelection());
    input.setUsingSortedList(wSortedList.getSelection());
    input.setUsingIntegerPair(wIntegerPair.getSelection());
    StreamLookupMeta.Lookup lookup = input.getLookup();
    lookup.getMatchKeys().clear();
    for (TableItem item : wKey.getNonEmptyItems()) {
      StreamLookupMeta.MatchKey matchKey = new StreamLookupMeta.MatchKey();
      lookup.getMatchKeys().add(matchKey);

      matchKey.setKeyStream(item.getText(1));
      matchKey.setKeyLookup(item.getText(2));
    }

    lookup.getReturnValues().clear();
    for (TableItem item : wReturn.getNonEmptyItems()) {
      StreamLookupMeta.ReturnValue returnValue = new StreamLookupMeta.ReturnValue();
      lookup.getReturnValues().add(returnValue);

      returnValue.setValue(item.getText(1));
      returnValue.setValueName(item.getText(2));
      if (Utils.isEmpty(returnValue.getValueName())) {
        returnValue.setValueName(returnValue.getValue());
      }
      returnValue.setValueDefault(item.getText(3));
      returnValue.setValueDefaultType(ValueMetaFactory.getIdForValueMeta(item.getText(4)));
    }

    input.setSourceTransformName(wTransform.getText());
    input.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
    IStream infoStream = input.getTransformIOMeta().getInfoStreams().getFirst();
    if (infoStream.getTransformMeta() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      if (Utils.isEmpty(wTransform.getText())) {
        mb.setMessage(
            BaseMessages.getString(
                PKG,
                "StreamLookupDialog.NotTransformSpecified.DialogMessage",
                wTransform.getText()));
      } else {
        mb.setMessage(
            BaseMessages.getString(
                PKG,
                "StreamLookupDialog.TransformCanNotFound.DialogMessage",
                wTransform.getText()));
      }

      mb.setText(
          BaseMessages.getString(PKG, "StreamLookupDialog.TransformCanNotFound.DialogTitle"));
      mb.open();
    }

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void getKeyFields() {
    if (pipelineMeta.findTransform(wTransform.getText()) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "StreamLookupDialog.PleaseSelectATransformToReadFrom.DialogMessage"));
      mb.setText(
          BaseMessages.getString(
              PKG, "StreamLookupDialog.PleaseSelectATransformToReadFrom.DialogTitle"));
      mb.open();
      return;
    }

    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wKey, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      } else {
        String transformFrom = wTransform.getText();
        if (!Utils.isEmpty(transformFrom)) {
          r = pipelineMeta.getTransformFields(variables, transformFrom);
          if (r != null) {
            BaseTransformDialog.getFieldsFromPrevious(
                r, wKey, 2, new int[] {1, 2}, new int[] {}, -1, -1, null);
          } else {
            MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
            mb.setMessage(
                BaseMessages.getString(PKG, "StreamLookupDialog.CouldNotFindFields.DialogMessage"));
            mb.setText(
                BaseMessages.getString(PKG, "StreamLookupDialog.CouldNotFindFields.DialogTitle"));
            mb.open();
          }
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(
              BaseMessages.getString(
                  PKG, "StreamLookupDialog.TransformNameRequired.DialogMessage"));
          mb.setText(
              BaseMessages.getString(PKG, "StreamLookupDialog.TransformNameRequired.DialogTitle"));
          mb.open();
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "StreamLookupDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "StreamLookupDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getLookupFields() {
    try {
      String transformFrom = wTransform.getText();
      if (!Utils.isEmpty(transformFrom)) {
        IRowMeta r = pipelineMeta.getTransformFields(variables, transformFrom);
        if (r != null && !r.isEmpty()) {
          BaseTransformDialog.getFieldsFromPrevious(
              r, wReturn, 1, new int[] {1}, new int[] {4}, -1, -1, null);
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(
              BaseMessages.getString(PKG, "StreamLookupDialog.CouldNotFindFields.DialogMessage"));
          mb.setText(
              BaseMessages.getString(PKG, "StreamLookupDialog.CouldNotFindFields.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "StreamLookupDialog.TransformNameRequired.DialogMessage"));
        mb.setText(
            BaseMessages.getString(PKG, "StreamLookupDialog.TransformNameRequired.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "StreamLookupDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "StreamLookupDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
