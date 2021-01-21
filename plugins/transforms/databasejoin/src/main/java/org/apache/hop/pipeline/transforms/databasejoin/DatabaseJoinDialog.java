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

package org.apache.hop.pipeline.transforms.databasejoin;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
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
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatabaseJoinDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DatabaseJoinMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private StyledTextComp wSql;

  private Text wLimit;

  private Button wOuter;

  private TableView wParam;

  private Button wuseVars;

  private final DatabaseJoinMeta input;

  private Label wlPosition;

  private ColumnInfo[] ciKey;

  private final Map<String, Integer> inputFields;

  public DatabaseJoinDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (DatabaseJoinMeta) in;
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

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.TransformName.Label"));
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

    // Connection line
    wConnection = addConnectionLine(shell, wTransformName, input.getDatabaseMeta(), lsMod);

    // SQL editor...
    Label wlSql = new Label(shell, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.SQL.Label"));
    props.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wConnection, margin * 2);
    wlSql.setLayoutData(fdlSql);

    wSql =
        new StyledTextComp(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wlSql, margin);
    fdSql.right = new FormAttachment(100, -2 * margin);
    fdSql.bottom = new FormAttachment(60, 0);
    wSql.setLayoutData(fdSql);

    wSql.addModifyListener(arg0 -> setPosition());

    wSql.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wSql.addFocusListener(
        new FocusAdapter() {
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wSql.addMouseListener(
        new MouseAdapter() {
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    wlPosition = new Label(shell, SWT.NONE);
    props.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.top = new FormAttachment(wSql, margin);
    fdlPosition.right = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    // Limit the number of lines returns
    Label wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(wlPosition, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.right = new FormAttachment(100, 0);
    fdLimit.top = new FormAttachment(wlPosition, margin);
    wLimit.setLayoutData(fdLimit);

    // Outer join?
    Label wlOuter = new Label(shell, SWT.RIGHT);
    wlOuter.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Outerjoin.Label"));
    wlOuter.setToolTipText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Outerjoin.Tooltip"));
    props.setLook(wlOuter);
    FormData fdlOuter = new FormData();
    fdlOuter.left = new FormAttachment(0, 0);
    fdlOuter.right = new FormAttachment(middle, -margin);
    fdlOuter.top = new FormAttachment(wLimit, margin);
    wlOuter.setLayoutData(fdlOuter);
    wOuter = new Button(shell, SWT.CHECK);
    props.setLook(wOuter);
    wOuter.setToolTipText(wlOuter.getToolTipText());
    FormData fdOuter = new FormData();
    fdOuter.left = new FormAttachment(middle, 0);
    fdOuter.top = new FormAttachment(wlOuter, 0, SWT.CENTER);
    wOuter.setLayoutData(fdOuter);
    wOuter.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // useVars ?
    Label wluseVars = new Label(shell, SWT.RIGHT);
    wluseVars.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.useVarsjoin.Label"));
    wluseVars.setToolTipText(BaseMessages.getString(PKG, "DatabaseJoinDialog.useVarsjoin.Tooltip"));
    props.setLook(wluseVars);
    FormData fdluseVars = new FormData();
    fdluseVars.left = new FormAttachment(0, 0);
    fdluseVars.right = new FormAttachment(middle, -margin);
    fdluseVars.top = new FormAttachment(wOuter, margin);
    wluseVars.setLayoutData(fdluseVars);
    wuseVars = new Button(shell, SWT.CHECK);
    props.setLook(wuseVars);
    wuseVars.setToolTipText(wluseVars.getToolTipText());
    FormData fduseVars = new FormData();
    fduseVars.left = new FormAttachment(middle, 0);
    fduseVars.top = new FormAttachment(wluseVars, 0, SWT.CENTER);
    wuseVars.setLayoutData(fduseVars);
    wuseVars.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFields.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    // The parameters
    Label wlParam = new Label(shell, SWT.NONE);
    wlParam.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Param.Label"));
    props.setLook(wlParam);
    FormData fdlParam = new FormData();
    fdlParam.left = new FormAttachment(0, 0);
    fdlParam.top = new FormAttachment(wuseVars, margin);
    wlParam.setLayoutData(fdlParam);

    int nrKeyCols = 2;
    int nrKeyRows = (input.getParameterField() != null ? input.getParameterField().length : 1);

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterFieldname"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseJoinDialog.ColumnInfo.ParameterType"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());

    wParam =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);

    FormData fdParam = new FormData();
    fdParam.left = new FormAttachment(0, 0);
    fdParam.top = new FormAttachment(wlParam, margin);
    fdParam.right = new FormAttachment(100, 0);
    fdParam.bottom = new FormAttachment(wOk, -2 * margin);
    wParam.setLayoutData(fdParam);

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
                inputFields.put(row.getValueMeta(i).getName(), i);
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    // Add listeners
    lsOk = e -> ok();
    Listener lsGet = e -> get();
    lsCancel = e -> cancel();

    wOk.addListener(SWT.Selection, lsOk);
    wGet.addListener(SWT.Selection, lsGet);
    wCancel.addListener(SWT.Selection, lsCancel);

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wLimit.addSelectionListener(lsDef);

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
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "DatabaseJoinDialog.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "DatabaseJoinDialog.Log.GettingKeyInfo"));

    wSql.setText(Const.NVL(input.getSql(), ""));
    wLimit.setText("" + input.getRowLimit());
    wOuter.setSelection(input.isOuterJoin());
    wuseVars.setSelection(input.isVariableReplace());
    if (input.getParameterField() != null) {
      for (int i = 0; i < input.getParameterField().length; i++) {
        TableItem item = wParam.table.getItem(i);
        if (input.getParameterField()[i] != null) {
          item.setText(1, input.getParameterField()[i]);
        }
        if (input.getParameterType()[i] != 0) {
          item.setText(2, ValueMetaFactory.getValueMetaName(input.getParameterType()[i]));
        }
      }
    }

    if (input.getDatabaseMeta() != null) {
      wConnection.setText(input.getDatabaseMeta().getName());
    }

    wParam.setRowNums();
    wParam.optWidth(true);

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

    int nrparam = wParam.nrNonEmpty();

    input.allocate(nrparam);

    input.setRowLimit(Const.toInt(wLimit.getText(), 0));
    input.setSql(wSql.getText());

    input.setOuterJoin(wOuter.getSelection());
    input.setVariableReplace(wuseVars.getSelection());
    logDebug(
        BaseMessages.getString(PKG, "DatabaseJoinDialog.Log.ParametersFound")
            + nrparam
            + " parameters");
    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrparam; i++) {
      TableItem item = wParam.getNonEmpty(i);
      input.getParameterField()[i] = item.getText(1);
      input.getParameterType()[i] = ValueMetaFactory.getIdForValueMeta(item.getText(2));
    }

    input.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText()));

    transformName = wTransformName.getText(); // return value

    if (pipelineMeta.findDatabase(wConnection.getText()) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wParam, 1, new int[] {1}, new int[] {2}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogTitle"),
          BaseMessages.getString(PKG, "DatabaseJoinDialog.GetFieldsFailed.DialogMessage"),
          ke);
    }
  }
}
