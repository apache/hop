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

package org.apache.hop.ui.pipeline.transforms.groupby;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.Aggregation;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class GroupByDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = GroupByMeta.class; // For Translator

  public static final String STRING_SORT_WARNING_PARAMETER = "GroupSortWarning";
  private static final int AGGREGATION_TABLE_TYPE_INDEX = 3;

  private Label wlGroup;

  private TableView wGroup;

  private FormData fdlGroup, fdGroup;

  private Label wlAgg;

  private TableView wAgg;

  private FormData fdlAgg, fdAgg;

  private Label wlAllRows;

  private Button wAllRows;

  private FormData fdlAllRows, fdAllRows;

  private Label wlSortDir;

  private Button wbSortDir;

  private TextVar wSortDir;

  private FormData fdlSortDir, fdbSortDir, fdSortDir;

  private Label wlPrefix;

  private Text wPrefix;

  private FormData fdlPrefix, fdPrefix;

  private Label wlAddLineNr;

  private Button wAddLineNr;

  private FormData fdlAddLineNr, fdAddLineNr;

  private Label wlLineNrField;

  private Text wLineNrField;

  private FormData fdlLineNrField, fdLineNrField;

  private Label wlAlwaysAddResult;

  private Button wAlwaysAddResult;

  private FormData fdlAlwaysAddResult, fdAlwaysAddResult;
  private Button wGet, wGetAgg;
  private FormData fdGet, fdGetAgg;
  private Listener lsGet, lsGetAgg;

  private GroupByMeta input;

  private boolean backupAllRows;

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  private Map<String, Integer> inputFields;

  public GroupByDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (GroupByMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();
    backupAllRows = input.passAllRows();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "GroupByDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "GroupByDialog.TransformName.Label"));
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

    // Include all rows?
    wlAllRows = new Label(shell, SWT.RIGHT);
    wlAllRows.setText(BaseMessages.getString(PKG, "GroupByDialog.AllRows.Label"));
    props.setLook(wlAllRows);
    fdlAllRows = new FormData();
    fdlAllRows.left = new FormAttachment(0, 0);
    fdlAllRows.top = new FormAttachment(wTransformName, margin);
    fdlAllRows.right = new FormAttachment(middle, -margin);
    wlAllRows.setLayoutData(fdlAllRows);
    wAllRows = new Button(shell, SWT.CHECK);
    props.setLook(wAllRows);
    fdAllRows = new FormData();
    fdAllRows.left = new FormAttachment(middle, 0);
    fdAllRows.top = new FormAttachment(wlAllRows, 0, SWT.CENTER);
    fdAllRows.right = new FormAttachment(100, 0);
    wAllRows.setLayoutData(fdAllRows);
    wAllRows.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setPassAllRows(!input.passAllRows());
            input.setChanged();
            setFlags();
          }
        });

    wlSortDir = new Label(shell, SWT.RIGHT);
    wlSortDir.setText(BaseMessages.getString(PKG, "GroupByDialog.TempDir.Label"));
    props.setLook(wlSortDir);
    fdlSortDir = new FormData();
    fdlSortDir.left = new FormAttachment(0, 0);
    fdlSortDir.right = new FormAttachment(middle, -margin);
    fdlSortDir.top = new FormAttachment(wAllRows, margin);
    wlSortDir.setLayoutData(fdlSortDir);

    wbSortDir = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSortDir);
    wbSortDir.setText(BaseMessages.getString(PKG, "GroupByDialog.Browse.Button"));
    fdbSortDir = new FormData();
    fdbSortDir.right = new FormAttachment(100, 0);
    fdbSortDir.top = new FormAttachment(wAllRows, margin);
    wbSortDir.setLayoutData(fdbSortDir);

    wSortDir = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSortDir);
    wSortDir.addModifyListener(lsMod);
    fdSortDir = new FormData();
    fdSortDir.left = new FormAttachment(middle, 0);
    fdSortDir.top = new FormAttachment(wAllRows, margin);
    fdSortDir.right = new FormAttachment(wbSortDir, -margin);
    wSortDir.setLayoutData(fdSortDir);

    wbSortDir.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            DirectoryDialog dd = new DirectoryDialog(shell, SWT.NONE);
            dd.setFilterPath(wSortDir.getText());
            String dir = dd.open();
            if (dir != null) {
              wSortDir.setText(dir);
            }
          }
        });

    // Whenever something changes, set the tooltip to the expanded version:
    wSortDir.addModifyListener(e -> wSortDir.setToolTipText(variables.resolve(wSortDir.getText())));

    // Prefix line...
    wlPrefix = new Label(shell, SWT.RIGHT);
    wlPrefix.setText(BaseMessages.getString(PKG, "GroupByDialog.FilePrefix.Label"));
    props.setLook(wlPrefix);
    fdlPrefix = new FormData();
    fdlPrefix.left = new FormAttachment(0, 0);
    fdlPrefix.right = new FormAttachment(middle, -margin);
    fdlPrefix.top = new FormAttachment(wbSortDir, margin * 2);
    wlPrefix.setLayoutData(fdlPrefix);
    wPrefix = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrefix);
    wPrefix.addModifyListener(lsMod);
    fdPrefix = new FormData();
    fdPrefix.left = new FormAttachment(middle, 0);
    fdPrefix.top = new FormAttachment(wbSortDir, margin * 2);
    fdPrefix.right = new FormAttachment(100, 0);
    wPrefix.setLayoutData(fdPrefix);

    // Include all rows?
    wlAddLineNr = new Label(shell, SWT.RIGHT);
    wlAddLineNr.setText(BaseMessages.getString(PKG, "GroupByDialog.AddLineNr.Label"));
    props.setLook(wlAddLineNr);
    fdlAddLineNr = new FormData();
    fdlAddLineNr.left = new FormAttachment(0, 0);
    fdlAddLineNr.top = new FormAttachment(wPrefix, margin);
    fdlAddLineNr.right = new FormAttachment(middle, -margin);
    wlAddLineNr.setLayoutData(fdlAddLineNr);
    wAddLineNr = new Button(shell, SWT.CHECK);
    props.setLook(wAddLineNr);
    fdAddLineNr = new FormData();
    fdAddLineNr.left = new FormAttachment(middle, 0);
    fdAddLineNr.top = new FormAttachment(wlAddLineNr, 0, SWT.CENTER);
    fdAddLineNr.right = new FormAttachment(100, 0);
    wAddLineNr.setLayoutData(fdAddLineNr);
    wAddLineNr.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setAddingLineNrInGroup(!input.isAddingLineNrInGroup());
            input.setChanged();
            setFlags();
          }
        });

    // LineNrField line...
    wlLineNrField = new Label(shell, SWT.RIGHT);
    wlLineNrField.setText(BaseMessages.getString(PKG, "GroupByDialog.LineNrField.Label"));
    props.setLook(wlLineNrField);
    fdlLineNrField = new FormData();
    fdlLineNrField.left = new FormAttachment(0, 0);
    fdlLineNrField.right = new FormAttachment(middle, -margin);
    fdlLineNrField.top = new FormAttachment(wAddLineNr, margin);
    wlLineNrField.setLayoutData(fdlLineNrField);
    wLineNrField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLineNrField);
    wLineNrField.addModifyListener(lsMod);
    fdLineNrField = new FormData();
    fdLineNrField.left = new FormAttachment(middle, 0);
    fdLineNrField.top = new FormAttachment(wAddLineNr, margin);
    fdLineNrField.right = new FormAttachment(100, 0);
    wLineNrField.setLayoutData(fdLineNrField);

    // Always pass a result rows as output
    //
    wlAlwaysAddResult = new Label(shell, SWT.RIGHT);
    wlAlwaysAddResult.setText(BaseMessages.getString(PKG, "GroupByDialog.AlwaysAddResult.Label"));
    wlAlwaysAddResult.setToolTipText(
        BaseMessages.getString(PKG, "GroupByDialog.AlwaysAddResult.ToolTip"));
    props.setLook(wlAlwaysAddResult);
    fdlAlwaysAddResult = new FormData();
    fdlAlwaysAddResult.left = new FormAttachment(0, 0);
    fdlAlwaysAddResult.top = new FormAttachment(wLineNrField, margin);
    fdlAlwaysAddResult.right = new FormAttachment(middle, -margin);
    wlAlwaysAddResult.setLayoutData(fdlAlwaysAddResult);
    wAlwaysAddResult = new Button(shell, SWT.CHECK);
    wAlwaysAddResult.setToolTipText(
        BaseMessages.getString(PKG, "GroupByDialog.AlwaysAddResult.ToolTip"));
    props.setLook(wAlwaysAddResult);
    fdAlwaysAddResult = new FormData();
    fdAlwaysAddResult.left = new FormAttachment(middle, 0);
    fdAlwaysAddResult.top = new FormAttachment(wlAlwaysAddResult, 0, SWT.CENTER);
    fdAlwaysAddResult.right = new FormAttachment(100, 0);
    wAlwaysAddResult.setLayoutData(fdAlwaysAddResult);

    wlGroup = new Label(shell, SWT.NONE);
    wlGroup.setText(BaseMessages.getString(PKG, "GroupByDialog.Group.Label"));
    props.setLook(wlGroup);
    fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment(0, 0);
    fdlGroup.top = new FormAttachment(wAlwaysAddResult, margin);
    wlGroup.setLayoutData(fdlGroup);

    int nrKeyCols = 1;
    int nrKeyRows = (input.getGroupField() != null ? input.getGroupField().length : 1);

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.GroupField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    wGroup =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "GroupByDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.top = new FormAttachment(wlGroup, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    fdGroup = new FormData();
    fdGroup.left = new FormAttachment(0, 0);
    fdGroup.top = new FormAttachment(wlGroup, margin);
    fdGroup.right = new FormAttachment(wGet, -margin);
    fdGroup.bottom = new FormAttachment(55, 0);
    wGroup.setLayoutData(fdGroup);

    // THE Aggregate fields
    wlAgg = new Label(shell, SWT.NONE);
    wlAgg.setText(BaseMessages.getString(PKG, "GroupByDialog.Aggregates.Label"));
    props.setLook(wlAgg);
    fdlAgg = new FormData();
    fdlAgg.left = new FormAttachment(0, 0);
    fdlAgg.top = new FormAttachment(wGroup, margin);
    wlAgg.setLayoutData(fdlAgg);

    int nrCols = 4;
    int nrRows = input.getAggregations().size();

    ciReturn = new ColumnInfo[nrCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.Name"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.Subject"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            GroupByMeta.typeGroupLongDesc);
    ciReturn[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.Value"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[3].setToolTip(BaseMessages.getString(PKG, "GroupByDialog.ColumnInfo.Value.Tooltip"));
    ciReturn[3].setUsingVariables(true);

    wAgg =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            nrRows,
            lsMod,
            props);

    wAgg.addModifyListener(
        modifyEvent -> {
          updateAllRowsCheckbox(wAgg, wAllRows, false);
          setFlags();
          input.setChanged();
        });

    wGetAgg = new Button(shell, SWT.PUSH);
    wGetAgg.setText(BaseMessages.getString(PKG, "GroupByDialog.GetLookupFields.Button"));
    fdGetAgg = new FormData();
    fdGetAgg.top = new FormAttachment(wlAgg, margin);
    fdGetAgg.right = new FormAttachment(100, 0);
    wGetAgg.setLayoutData(fdGetAgg);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    fdAgg = new FormData();
    fdAgg.left = new FormAttachment(0, 0);
    fdAgg.top = new FormAttachment(wlAgg, margin);
    fdAgg.right = new FormAttachment(wGetAgg, -margin);
    fdAgg.bottom = new FormAttachment(wOk, -margin);
    wAgg.setLayoutData(fdAgg);

    // Add listeners
    lsOk = e -> ok();
    lsGet = e -> get();
    lsGetAgg = e -> getAgg();
    lsCancel = e -> cancel();

    wOk.addListener(SWT.Selection, lsOk);
    wGet.addListener(SWT.Selection, lsGet);
    wGetAgg.addListener(SWT.Selection, lsGetAgg);
    wCancel.addListener(SWT.Selection, lsCancel);

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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    ciKey[0].setComboValues(fieldNames);
    ciReturn[1].setComboValues(fieldNames);
  }

  public void setFlags() {
    wlSortDir.setEnabled(wAllRows.getSelection());
    wbSortDir.setEnabled(wAllRows.getSelection());
    wSortDir.setEnabled(wAllRows.getSelection());
    wlPrefix.setEnabled(wAllRows.getSelection());
    wPrefix.setEnabled(wAllRows.getSelection());
    wlAddLineNr.setEnabled(wAllRows.getSelection());
    wAddLineNr.setEnabled(wAllRows.getSelection());

    wlLineNrField.setEnabled(wAllRows.getSelection() && wAddLineNr.getSelection());
    wLineNrField.setEnabled(wAllRows.getSelection() && wAddLineNr.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "GroupByDialog.Log.GettingKeyInfo"));

    wAllRows.setSelection(input.passAllRows());

    if (input.getPrefix() != null) {
      wPrefix.setText(input.getPrefix());
    }
    if (input.getDirectory() != null) {
      wSortDir.setText(input.getDirectory());
    }
    wAddLineNr.setSelection(input.isAddingLineNrInGroup());
    if (input.getLineNrInGroupField() != null) {
      wLineNrField.setText(input.getLineNrInGroupField());
    }
    wAlwaysAddResult.setSelection(input.isAlwaysGivingBackOneRow());

    if (input.getGroupField() != null) {
      for (int i = 0; i < input.getGroupField().length; i++) {
        TableItem item = wGroup.table.getItem(i);
        if (input.getGroupField()[i] != null) {
          item.setText(1, input.getGroupField()[i]);
        }
      }
    }

    int i = 0;
    for (Aggregation aggregation : input.getAggregations()) {
      TableItem item = wAgg.table.getItem(i++);
      item.setText(1, Const.NVL(aggregation.getField(), ""));
      item.setText(2, Const.NVL(aggregation.getSubject(), ""));
      item.setText(3, GroupByMeta.getTypeDescLong(aggregation.getType()));
      item.setText(3, Const.NVL(aggregation.getValue(), ""));
    }

    wGroup.setRowNums();
    wGroup.optWidth(true);
    wAgg.setRowNums();
    wAgg.optWidth(true);

    setFlags();
    updateAllRowsCheckbox(wAgg, wAllRows, !input.passAllRows());

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(false);
    input.setPassAllRows(backupAllRows);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    int sizegroup = wGroup.nrNonEmpty();
    int nrFields = wAgg.nrNonEmpty();
    input.setPrefix(wPrefix.getText());
    input.setDirectory(wSortDir.getText());

    input.setLineNrInGroupField(wLineNrField.getText());
    input.setAlwaysGivingBackOneRow(wAlwaysAddResult.getSelection());
    input.setPassAllRows(wAllRows.getSelection());

    input.allocate(sizegroup);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < sizegroup; i++) {
      TableItem item = wGroup.getNonEmpty(i);
      input.getGroupField()[i] = item.getText(1);
    }

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wAgg.getNonEmpty(i);
      String aggField = item.getText(1);
      String aggSubject = item.getText(2);
      int aggType = GroupByMeta.getType(item.getText(3));
      String aggValue = item.getText(4);
      input.getAggregations().add(new Aggregation(aggField, aggSubject, aggType, aggValue));
    }

    transformName = wTransformName.getText();

    if (sizegroup > 0
        && "Y".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "GroupByDialog.GroupByWarningDialog.DialogTitle"),
              BaseMessages.getString(
                      PKG, "GroupByDialog.GroupByWarningDialog.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "GroupByDialog.GroupByWarningDialog.Option1")
              },
              BaseMessages.getString(PKG, "GroupByDialog.GroupByWarningDialog.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wGroup, 1, new int[] {1}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GroupByDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "GroupByDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getAgg() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wAgg, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GroupByDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "GroupByDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /**
   * Method to update on-the-fly "Include All Rows" checkbox whether "Cumulative sum" or "Cumulative
   * average" values are configured in the Aggregation table.
   *
   * @param aggregationTable the aggregation table to check the entries
   * @param allRowsButton the checkbox to update
   * @param forceUpdate if an update must be done
   */
  void updateAllRowsCheckbox(
      TableView aggregationTable, Button allRowsButton, boolean forceUpdate) {

    boolean isCumulativeSelected =
        IntStream.range(0, aggregationTable.nrNonEmpty())
            .map(
                row ->
                    GroupByMeta.getType(
                        aggregationTable.getNonEmpty(row).getText(AGGREGATION_TABLE_TYPE_INDEX)))
            .anyMatch(
                pred ->
                    pred == GroupByMeta.TYPE_GROUP_CUMULATIVE_SUM
                        || pred == GroupByMeta.TYPE_GROUP_CUMULATIVE_AVERAGE);

    allRowsButton.setEnabled(!isCumulativeSelected);

    if (isCumulativeSelected) {
      allRowsButton.setSelection(true);
      if (forceUpdate) {
        backupChanged = true;
      }
    }
  }

  @Override
  protected Button createHelpButton(Shell shell, TransformMeta transformMeta, IPlugin plugin) {
    plugin.setDocumentationUrl(
        "https://hop.apache.org/manual/latest/plugins/transforms/groupby.html");
    return super.createHelpButton(shell, transformMeta, plugin);
  }
}
