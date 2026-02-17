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

package org.apache.hop.pipeline.transforms.databasejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DatabaseJoinDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DatabaseJoinMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextComposite wSql;

  private Text wLimit;

  private Button wOuter;

  private TableView wParam;

  private Button wUseVars;

  private final DatabaseJoinMeta input;

  private Label wlPosition;

  private ColumnInfo[] ciKey;

  private final List<String> inputFields = new ArrayList<>();

  private Button wCache;

  private Label wlCacheSize;
  private Text wCacheSize;

  public DatabaseJoinDialog(
      Shell parent,
      IVariables variables,
      DatabaseJoinMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DatabaseJoinDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    // Connection line
    wConnection = addConnectionLine(shell, wSpacer, input.getConnection(), lsMod);
    wConnection.addListener(SWT.Selection, e -> getSqlReservedWords());

    // ICache?
    Label wlCache = new Label(shell, SWT.RIGHT);
    wlCache.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Cache.Label"));
    PropsUi.setLook(wlCache);
    FormData fdlCache = new FormData();
    fdlCache.left = new FormAttachment(0, 0);
    fdlCache.right = new FormAttachment(middle, -margin);
    fdlCache.top = new FormAttachment(wConnection, margin);
    wlCache.setLayoutData(fdlCache);
    wCache = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCache);
    FormData fdCache = new FormData();
    fdCache.left = new FormAttachment(middle, 0);
    fdCache.top = new FormAttachment(wlCache, 0, SWT.CENTER);
    wCache.setLayoutData(fdCache);
    wCache.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // ICache size line
    wlCacheSize = new Label(shell, SWT.RIGHT);
    wlCacheSize.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.CacheSize.Label"));
    PropsUi.setLook(wlCacheSize);
    wlCacheSize.setEnabled(input.isCached());
    FormData fdlCacheSize = new FormData();
    fdlCacheSize.left = new FormAttachment(0, 0);
    fdlCacheSize.right = new FormAttachment(middle, -margin);
    fdlCacheSize.top = new FormAttachment(wCache, margin);
    wlCacheSize.setLayoutData(fdlCacheSize);
    wCacheSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCacheSize);
    wCacheSize.setEnabled(input.isCached());
    wCacheSize.addModifyListener(lsMod);
    FormData fdCacheSize = new FormData();
    fdCacheSize.left = new FormAttachment(middle, 0);
    fdCacheSize.right = new FormAttachment(100, 0);
    fdCacheSize.top = new FormAttachment(wCache, margin);
    wCacheSize.setLayoutData(fdCacheSize);

    // SQL editor...
    Label wlSql = new Label(shell, SWT.NONE);
    wlSql.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.SQL.Label"));
    PropsUi.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment(0, 0);
    fdlSql.top = new FormAttachment(wCacheSize, margin);
    wlSql.setLayoutData(fdlSql);

    wSql =
        EnvironmentUtils.getInstance().isWeb()
            ? new StyledTextComp(
                variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL)
            : new SQLStyledTextComp(
                variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wSql.addLineStyleListener(getSqlReservedWords());
    PropsUi.setLook(wSql, Props.WIDGET_STYLE_FIXED);
    wSql.addModifyListener(lsMod);
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment(0, 0);
    fdSql.top = new FormAttachment(wlSql, margin);
    fdSql.right = new FormAttachment(100, -margin);
    fdSql.bottom = new FormAttachment(60, 0);
    wSql.setLayoutData(fdSql);

    wSql.addModifyListener(arg0 -> setPosition());

    wSql.addKeyListener(
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
    wSql.addFocusListener(
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
    wSql.addMouseListener(
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

    wlPosition = new Label(shell, SWT.NONE);
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.top = new FormAttachment(wSql, margin);
    fdlPosition.right = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    // Limit the number of lines returns
    Label wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.top = new FormAttachment(wlPosition, margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
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
    PropsUi.setLook(wlOuter);
    FormData fdlOuter = new FormData();
    fdlOuter.left = new FormAttachment(0, 0);
    fdlOuter.right = new FormAttachment(middle, -margin);
    fdlOuter.top = new FormAttachment(wLimit, margin);
    wlOuter.setLayoutData(fdlOuter);
    wOuter = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wOuter);
    wOuter.setToolTipText(wlOuter.getToolTipText());
    FormData fdOuter = new FormData();
    fdOuter.left = new FormAttachment(middle, 0);
    fdOuter.top = new FormAttachment(wlOuter, 0, SWT.CENTER);
    wOuter.setLayoutData(fdOuter);
    wOuter.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // useVars ?
    Label wluseVars = new Label(shell, SWT.RIGHT);
    wluseVars.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.useVarsjoin.Label"));
    wluseVars.setToolTipText(BaseMessages.getString(PKG, "DatabaseJoinDialog.useVarsjoin.Tooltip"));
    PropsUi.setLook(wluseVars);
    FormData fdluseVars = new FormData();
    fdluseVars.left = new FormAttachment(0, 0);
    fdluseVars.right = new FormAttachment(middle, -margin);
    fdluseVars.top = new FormAttachment(wOuter, margin);
    wluseVars.setLayoutData(fdluseVars);
    wUseVars = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wUseVars);
    wUseVars.setToolTipText(wluseVars.getToolTipText());
    FormData fduseVars = new FormData();
    fduseVars.left = new FormAttachment(middle, 0);
    fduseVars.top = new FormAttachment(wluseVars, 0, SWT.CENTER);
    wUseVars.setLayoutData(fduseVars);
    wUseVars.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // THE BUTTONS
    // The parameters
    Label wlParam = new Label(shell, SWT.NONE);
    wlParam.setText(BaseMessages.getString(PKG, "DatabaseJoinDialog.Param.Label"));
    PropsUi.setLook(wlParam);
    FormData fdlParam = new FormData();
    fdlParam.left = new FormAttachment(0, 0);
    fdlParam.top = new FormAttachment(wUseVars, margin);
    wlParam.setLayoutData(fdlParam);

    int nrKeyCols = 2;
    int nrKeyRows = (input.getParameters() != null ? input.getParameters().size() : 1);

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
    fdParam.bottom = new FormAttachment(wOk, -margin);
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
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private List<String> getSqlReservedWords() {
    // Do not search keywords when connection is empty
    if (Utils.isEmpty(input.getConnection())) {
      return List.of();
    }

    // If connection is a variable that can't be resolved
    if (variables.resolve(input.getConnection()).startsWith("${")) {
      return List.of();
    }

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(input.getConnection(), variables);
    return Arrays.stream(databaseMeta.getReservedWords()).toList();
  }

  private void enableFields() {
    wCacheSize.setEnabled(wCache.getSelection());
    wlCacheSize.setEnabled(wCache.getSelection());
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
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

    wConnection.setText(Const.NVL(input.getConnection(), ""));

    wCache.setSelection(input.isCached());
    wCacheSize.setText("" + input.getCacheSize());

    wSql.setText(Const.NVL(input.getSql(), ""));
    wLimit.setText("" + input.getRowLimit());
    wOuter.setSelection(input.isOuterJoin());
    wUseVars.setSelection(input.isReplaceVariables());
    if (input.getParameters() != null) {
      int i = 0;
      for (ParameterField field : input.getParameters()) {
        TableItem item = wParam.table.getItem(i++);
        if (field != null) {
          item.setText(1, field.getName());
          item.setText(2, field.getType());
        }
      }
    }

    wParam.setRowNums();
    wParam.optWidth(true);

    enableFields();
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

    input.setConnection(wConnection.getText());
    input.setCached(wCache.getSelection());
    input.setCacheSize(Const.toInt(wCacheSize.getText(), 0));
    input.setRowLimit(Const.toInt(wLimit.getText(), 0));
    input.setSql(wSql.getText());
    input.setOuterJoin(wOuter.getSelection());
    input.setReplaceVariables(wUseVars.getSelection());
    logDebug(
        BaseMessages.getString(PKG, "DatabaseJoinDialog.Log.ParametersFound")
            + nrparam
            + " parameters");

    List<ParameterField> parameters = new ArrayList<>();
    for (int i = 0; i < nrparam; i++) {
      TableItem item = wParam.getNonEmpty(i);
      ParameterField field = new ParameterField();
      field.setName(item.getText(1));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(2)));
      parameters.add(field);
    }
    input.setParameters(parameters);

    transformName = wTransformName.getText(); // return value

    if (pipelineMeta.findDatabase(wConnection.getText(), variables) == null) {
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
