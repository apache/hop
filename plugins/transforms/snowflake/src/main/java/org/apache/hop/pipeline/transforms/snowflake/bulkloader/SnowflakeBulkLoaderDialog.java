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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SnowflakeBulkLoaderDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  /** The descriptions for the location type drop down */
  private static final String[] LOCATION_TYPE_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.LocationType.User"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.LocationType.Table"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.LocationType.InternalStage")
      };

  /** The descriptions for the on error drop down */
  private static final String[] ON_ERROR_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.OnError.Continue"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.OnError.SkipFile"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.OnError.SkipFilePercent"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.OnError.Abort")
      };

  /** The descriptions for the data type drop down */
  private static final String[] DATA_TYPE_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.DataType.CSV"),
        BaseMessages.getString(PKG, "SnowflakeBulkLoad.Dialog.DataType.JSON")
      };

  public static final String CONST_SYSTEM_BUTTON_BROWSE = "System.Button.Browse";

  /* ********************************************************
   * Loader tab
   * This tab is used to configure information about the
   * database and bulk load method.
   * ********************************************************/

  // Database connection line
  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  // Location Type line

  private CCombo wLocationType;

  // Stage Name line

  private ComboVar wStageName;

  // Work Directory Line

  private TextVar wWorkDirectory;

  // On Error Line
  private CCombo wOnError;
  // Error Limit Line
  private Label wlErrorLimit;
  private TextVar wErrorLimit;

  // Split Size Line

  private TextVar wSplitSize;

  // Remove files line

  private Button wRemoveFiles;

  /* *************************************************************
   * End Loader Tab
   * *************************************************************/

  /* *************************************************************
   * Begin Data Type tab
   * This tab is used to configure the specific loading parameters
   * for the data type selected.
   * *************************************************************/

  // Data Type Line
  private CCombo wDataType;

  /* -------------------------------------------------------------
   * CSV Group
   * ------------------------------------------------------------*/

  private Group gCsvGroup;

  // Trim Whitespace line
  private Button wTrimWhitespace;

  // Null If line
  private TextVar wNullIf;
  // Error on column mismatch line
  private Button wColumnMismatch;

  /* --------------------------------------------------
   * End CSV Group
   * -------------------------------------------------*/

  /* --------------------------------------------------
   * Start JSON Group
   * -------------------------------------------------*/

  private Group gJsonGroup;

  // Strip null line
  private Button wStripNull;

  // Ignore UTF-8 Error line
  private Button wIgnoreUtf8;

  // Allow duplicate elements lines
  private Button wAllowDuplicate;

  // Enable Octal line
  private Button wEnableOctal;

  /* -------------------------------------------------
   * End JSON Group
   * ------------------------------------------------*/

  /* ************************************************
   * End Data tab
   * ************************************************/

  /* ************************************************
   * Start fields tab
   * This tab is used to define the field mappings
   * from the stream field to the database
   * ************************************************/

  // Specify Fields line
  private Button wSpecifyFields;

  // JSON Field Line
  private Label wlJsonField;
  private CCombo wJsonField;

  // Field mapping table
  private TableView wFields;
  private ColumnInfo[] colinf;

  // Enter field mapping
  private Button wDoMapping;
  /* ************************************************
   * End Fields tab
   * ************************************************/

  private SnowflakeBulkLoaderMeta input;

  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

  private final List<String> inputFields = new ArrayList<>();

  private Display display;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public SnowflakeBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      SnowflakeBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * Open the Bulk Loader dialog
   *
   * @return The transform name
   */
  public String open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    int margin = PropsUi.getMargin();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    /* ************************************************
     * Modify Listeners
     * ************************************************/

    // Basic modify listener, sets if anything has changed.  Hop's way to know the pipeline
    // needs saved
    ModifyListener lsMod = e -> input.setChanged();

    SelectionAdapter bMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };

    // Some settings have to modify what is or is not visible within the shell.  This listener does
    // this.
    SelectionAdapter lsFlags =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFlags();
          }
        };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.Title"));

    int middle = props.getMiddlePct();

    // Transform name line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    /* *********************************************
     * Start of Loader tab
     * *********************************************/

    CTabItem wLoaderTab = new CTabItem(wTabFolder, SWT.NONE);
    wLoaderTab.setFont(GuiResource.getInstance().getFontDefault());
    wLoaderTab.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.LoaderTab.TabTitle"));

    Composite wLoaderComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wLoaderComp);

    FormLayout loaderLayout = new FormLayout();
    loaderLayout.marginWidth = 3;
    loaderLayout.marginHeight = 3;
    wLoaderComp.setLayout(loaderLayout);

    // Connection line
    wConnection = addConnectionLine(wLoaderComp, wTransformName, input.getConnection(), lsMod);
    if (input.getConnection() == null && pipelineMeta.nrDatabases() == 1) {
      wConnection.select(0);
    }
    wConnection.addModifyListener(lsMod);

    // Schema line
    Label wlSchema = new Label(wLoaderComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.Schema.Label"));
    wlSchema.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.Schema.Tooltip"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdlSchema.right = new FormAttachment(middle, -margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(wLoaderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin * 2);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);
    wSchema.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent focusEvent) {
            setTableFieldCombo();
          }
        });

    // Table line...
    Label wlTable = new Label(wLoaderComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.Table.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(wLoaderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);
    wTable.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent focusEvent) {
            setTableFieldCombo();
          }
        });

    // Location Type line
    //
    Label wlLocationType = new Label(wLoaderComp, SWT.RIGHT);
    wlLocationType.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.LocationType.Label"));
    wlLocationType.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.LocationType.Tooltip"));
    PropsUi.setLook(wlLocationType);
    FormData fdlLocationType = new FormData();
    fdlLocationType.left = new FormAttachment(0, 0);
    fdlLocationType.top = new FormAttachment(wTable, margin * 2);
    fdlLocationType.right = new FormAttachment(middle, -margin);
    wlLocationType.setLayoutData(fdlLocationType);

    wLocationType = new CCombo(wLoaderComp, SWT.BORDER | SWT.READ_ONLY);
    wLocationType.setEditable(false);
    PropsUi.setLook(wLocationType);
    wLocationType.addModifyListener(lsMod);
    wLocationType.addSelectionListener(lsFlags);
    FormData fdLocationType = new FormData();
    fdLocationType.left = new FormAttachment(middle, 0);
    fdLocationType.top = new FormAttachment(wTable, margin * 2);
    fdLocationType.right = new FormAttachment(100, 0);
    wLocationType.setLayoutData(fdLocationType);
    for (String locationType : LOCATION_TYPE_COMBO) {
      wLocationType.add(locationType);
    }

    // Stage name line
    //
    Label wlStageName = new Label(wLoaderComp, SWT.RIGHT);
    wlStageName.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.StageName.Label"));
    PropsUi.setLook(wlStageName);
    FormData fdlStageName = new FormData();
    fdlStageName.left = new FormAttachment(0, 0);
    fdlStageName.top = new FormAttachment(wLocationType, margin * 2);
    fdlStageName.right = new FormAttachment(middle, -margin);
    wlStageName.setLayoutData(fdlStageName);

    wStageName = new ComboVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStageName);
    wStageName.addModifyListener(lsMod);
    wStageName.addSelectionListener(lsFlags);
    FormData fdStageName = new FormData();
    fdStageName.left = new FormAttachment(middle, 0);
    fdStageName.top = new FormAttachment(wLocationType, margin * 2);
    fdStageName.right = new FormAttachment(100, 0);
    wStageName.setLayoutData(fdStageName);
    wStageName.setEnabled(false);
    wStageName.addFocusListener(
        new FocusAdapter() {
          /**
           * Get the list of stages for the schema, and populate the stage name drop down.
           *
           * @param focusEvent The event
           */
          @Override
          public void focusGained(FocusEvent focusEvent) {
            String stageNameText = wStageName.getText();
            wStageName.removeAll();

            DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
            if (databaseMeta != null) {
              try (Database db = new Database(loggingObject, variables, databaseMeta)) {
                db.connect();
                String sql = "show stages";
                if (!StringUtils.isEmpty(variables.resolve(wSchema.getText()))) {
                  sql += " in " + variables.resolve(wSchema.getText());
                }

                try (ResultSet resultSet =
                    db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false)) {
                  IRowMeta rowMeta = db.getReturnRowMeta();
                  Object[] row = db.getRow(resultSet);
                  int nameField = rowMeta.indexOfValue("NAME");
                  if (nameField >= 0) {
                    while (row != null) {
                      String stageName = rowMeta.getString(row, nameField);
                      wStageName.add(stageName);
                      row = db.getRow(resultSet);
                    }
                  } else {
                    throw new HopException("Unable to find stage name field in result");
                  }
                  db.closeQuery(resultSet);
                }
                if (stageNameText != null) {
                  wStageName.setText(stageNameText);
                }

              } catch (Exception ex) {
                logDebug("Error getting stages", ex);
              }
            }
          }
        });

    // Work directory line
    Label wlWorkDirectory = new Label(wLoaderComp, SWT.RIGHT);
    wlWorkDirectory.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.WorkDirectory.Label"));
    wlWorkDirectory.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.WorkDirectory.Tooltip"));
    PropsUi.setLook(wlWorkDirectory);
    FormData fdlWorkDirectory = new FormData();
    fdlWorkDirectory.left = new FormAttachment(0, 0);
    fdlWorkDirectory.top = new FormAttachment(wStageName, margin);
    fdlWorkDirectory.right = new FormAttachment(middle, -margin);
    wlWorkDirectory.setLayoutData(fdlWorkDirectory);

    Button wbWorkDirectory = new Button(wLoaderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbWorkDirectory);
    wbWorkDirectory.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbWorkDirectory = new FormData();
    fdbWorkDirectory.right = new FormAttachment(100, 0);
    fdbWorkDirectory.top = new FormAttachment(wStageName, margin);
    wbWorkDirectory.setLayoutData(fdbWorkDirectory);

    wWorkDirectory = new TextVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWorkDirectory.setText("temp");
    PropsUi.setLook(wWorkDirectory);
    wWorkDirectory.addModifyListener(lsMod);
    FormData fdWorkDirectory = new FormData();
    fdWorkDirectory.left = new FormAttachment(middle, 0);
    fdWorkDirectory.top = new FormAttachment(wStageName, margin);
    fdWorkDirectory.right = new FormAttachment(wbWorkDirectory, -margin);
    wWorkDirectory.setLayoutData(fdWorkDirectory);
    wbWorkDirectory.addListener(
        SWT.Selection,
        e -> {
          BaseDialog.presentDirectoryDialog(shell, wWorkDirectory, variables);
        });

    // Whenever something changes, set the tooltip to the expanded version:
    wWorkDirectory.addModifyListener(
        e -> wWorkDirectory.setToolTipText(variables.resolve(wWorkDirectory.getText())));

    // On Error line
    //
    Label wlOnError = new Label(wLoaderComp, SWT.RIGHT);
    wlOnError.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.OnError.Label"));
    wlOnError.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.OnError.Tooltip"));
    PropsUi.setLook(wlOnError);
    FormData fdlOnError = new FormData();
    fdlOnError.left = new FormAttachment(0, 0);
    fdlOnError.top = new FormAttachment(wWorkDirectory, margin * 2);
    fdlOnError.right = new FormAttachment(middle, -margin);
    wlOnError.setLayoutData(fdlOnError);

    wOnError = new CCombo(wLoaderComp, SWT.BORDER | SWT.READ_ONLY);
    wOnError.setEditable(false);
    PropsUi.setLook(wOnError);
    wOnError.addModifyListener(lsMod);
    wOnError.addSelectionListener(lsFlags);
    FormData fdOnError = new FormData();
    fdOnError.left = new FormAttachment(middle, 0);
    fdOnError.top = new FormAttachment(wWorkDirectory, margin * 2);
    fdOnError.right = new FormAttachment(100, 0);
    wOnError.setLayoutData(fdOnError);
    for (String onError : ON_ERROR_COMBO) {
      wOnError.add(onError);
    }

    // Error Limit line
    wlErrorLimit = new Label(wLoaderComp, SWT.RIGHT);
    wlErrorLimit.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Label"));
    wlErrorLimit.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Tooltip"));
    PropsUi.setLook(wlErrorLimit);
    FormData fdlErrorLimit = new FormData();
    fdlErrorLimit.left = new FormAttachment(0, 0);
    fdlErrorLimit.top = new FormAttachment(wOnError, margin);
    fdlErrorLimit.right = new FormAttachment(middle, -margin);
    wlErrorLimit.setLayoutData(fdlErrorLimit);

    wErrorLimit = new TextVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorLimit);
    wErrorLimit.addModifyListener(lsMod);
    FormData fdErrorLimit = new FormData();
    fdErrorLimit.left = new FormAttachment(middle, 0);
    fdErrorLimit.top = new FormAttachment(wOnError, margin);
    fdErrorLimit.right = new FormAttachment(100, 0);
    wErrorLimit.setLayoutData(fdErrorLimit);

    // Size limit line
    Label wlSplitSize = new Label(wLoaderComp, SWT.RIGHT);
    wlSplitSize.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.SplitSize.Label"));
    wlSplitSize.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.SplitSize.Tooltip"));
    PropsUi.setLook(wlSplitSize);
    FormData fdlSplitSize = new FormData();
    fdlSplitSize.left = new FormAttachment(0, 0);
    fdlSplitSize.top = new FormAttachment(wErrorLimit, margin);
    fdlSplitSize.right = new FormAttachment(middle, -margin);
    wlSplitSize.setLayoutData(fdlSplitSize);

    wSplitSize = new TextVar(variables, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSplitSize);
    wSplitSize.addModifyListener(lsMod);
    FormData fdSplitSize = new FormData();
    fdSplitSize.left = new FormAttachment(middle, 0);
    fdSplitSize.top = new FormAttachment(wErrorLimit, margin);
    fdSplitSize.right = new FormAttachment(100, 0);
    wSplitSize.setLayoutData(fdSplitSize);

    // Remove files line
    //
    Label wlRemoveFiles = new Label(wLoaderComp, SWT.RIGHT);
    wlRemoveFiles.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.RemoveFiles.Label"));
    wlRemoveFiles.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.RemoveFiles.Tooltip"));
    PropsUi.setLook(wlRemoveFiles);
    FormData fdlRemoveFiles = new FormData();
    fdlRemoveFiles.left = new FormAttachment(0, 0);
    fdlRemoveFiles.top = new FormAttachment(wSplitSize, margin);
    fdlRemoveFiles.right = new FormAttachment(middle, -margin);
    wlRemoveFiles.setLayoutData(fdlRemoveFiles);

    wRemoveFiles = new Button(wLoaderComp, SWT.CHECK);
    PropsUi.setLook(wRemoveFiles);
    FormData fdRemoveFiles = new FormData();
    fdRemoveFiles.left = new FormAttachment(middle, 0);
    fdRemoveFiles.top = new FormAttachment(wSplitSize, margin);
    fdRemoveFiles.right = new FormAttachment(100, 0);
    wRemoveFiles.setLayoutData(fdRemoveFiles);
    wRemoveFiles.addSelectionListener(bMod);

    FormData fdLoaderComp = new FormData();
    fdLoaderComp.left = new FormAttachment(0, 0);
    fdLoaderComp.top = new FormAttachment(0, 0);
    fdLoaderComp.right = new FormAttachment(100, 0);
    fdLoaderComp.bottom = new FormAttachment(100, 0);
    wLoaderComp.setLayoutData(fdLoaderComp);

    wLoaderComp.layout();
    wLoaderTab.setControl(wLoaderComp);

    /* ********************************************************
     * End Loader tab
     * ********************************************************/

    /* ********************************************************
     * Start data type tab
     * ********************************************************/

    CTabItem wDataTypeTab = new CTabItem(wTabFolder, SWT.NONE);
    wDataTypeTab.setFont(GuiResource.getInstance().getFontDefault());
    wDataTypeTab.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.DataTypeTab.TabTitle"));

    Composite wDataTypeComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wDataTypeComp);

    FormLayout dataTypeLayout = new FormLayout();
    dataTypeLayout.marginWidth = 3;
    dataTypeLayout.marginHeight = 3;
    wDataTypeComp.setLayout(dataTypeLayout);

    // Data Type Line
    //
    Label wlDataType = new Label(wDataTypeComp, SWT.RIGHT);
    wlDataType.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.DataType.Label"));
    PropsUi.setLook(wlDataType);
    FormData fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment(0, 0);
    fdlDataType.top = new FormAttachment(0, margin);
    fdlDataType.right = new FormAttachment(middle, -margin);
    wlDataType.setLayoutData(fdlDataType);

    wDataType = new CCombo(wDataTypeComp, SWT.BORDER | SWT.READ_ONLY);
    wDataType.setEditable(false);
    PropsUi.setLook(wDataType);
    wDataType.addModifyListener(lsMod);
    wDataType.addSelectionListener(lsFlags);
    FormData fdDataType = new FormData();
    fdDataType.left = new FormAttachment(middle, 0);
    fdDataType.top = new FormAttachment(0, margin);
    fdDataType.right = new FormAttachment(100, 0);
    wDataType.setLayoutData(fdDataType);
    for (String dataType : DATA_TYPE_COMBO) {
      wDataType.add(dataType);
    }

    /////////////////////
    // Start CSV Group
    /////////////////////
    gCsvGroup = new Group(wDataTypeComp, SWT.SHADOW_ETCHED_IN);
    gCsvGroup.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.CSVGroup.Label"));
    FormLayout csvLayout = new FormLayout();
    csvLayout.marginWidth = 3;
    csvLayout.marginHeight = 3;
    gCsvGroup.setLayout(csvLayout);
    PropsUi.setLook(gCsvGroup);

    FormData fdgCsvGroup = new FormData();
    fdgCsvGroup.left = new FormAttachment(0, 0);
    fdgCsvGroup.right = new FormAttachment(100, 0);
    fdgCsvGroup.top = new FormAttachment(wDataType, margin * 2);
    fdgCsvGroup.bottom = new FormAttachment(100, -margin * 2);
    gCsvGroup.setLayoutData(fdgCsvGroup);

    // Trim Whitespace line
    Label wlTrimWhitespace = new Label(gCsvGroup, SWT.RIGHT);
    wlTrimWhitespace.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.TrimWhitespace.Label"));
    wlTrimWhitespace.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.TrimWhitespace.Tooltip"));
    PropsUi.setLook(wlTrimWhitespace);
    FormData fdlTrimWhitespace = new FormData();
    fdlTrimWhitespace.left = new FormAttachment(0, 0);
    fdlTrimWhitespace.top = new FormAttachment(0, margin);
    fdlTrimWhitespace.right = new FormAttachment(middle, -margin);
    wlTrimWhitespace.setLayoutData(fdlTrimWhitespace);

    wTrimWhitespace = new Button(gCsvGroup, SWT.CHECK);
    PropsUi.setLook(wTrimWhitespace);
    FormData fdTrimWhitespace = new FormData();
    fdTrimWhitespace.left = new FormAttachment(middle, 0);
    fdTrimWhitespace.top = new FormAttachment(0, margin);
    fdTrimWhitespace.right = new FormAttachment(100, 0);
    wTrimWhitespace.setLayoutData(fdTrimWhitespace);
    wTrimWhitespace.addSelectionListener(bMod);

    // Null if line
    Label wlNullIf = new Label(gCsvGroup, SWT.RIGHT);
    wlNullIf.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.NullIf.Label"));
    wlNullIf.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.NullIf.Tooltip"));
    PropsUi.setLook(wlNullIf);
    FormData fdlNullIf = new FormData();
    fdlNullIf.left = new FormAttachment(0, 0);
    fdlNullIf.top = new FormAttachment(wTrimWhitespace, margin);
    fdlNullIf.right = new FormAttachment(middle, -margin);
    wlNullIf.setLayoutData(fdlNullIf);

    wNullIf = new TextVar(variables, gCsvGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNullIf);
    wNullIf.addModifyListener(lsMod);
    FormData fdNullIf = new FormData();
    fdNullIf.left = new FormAttachment(middle, 0);
    fdNullIf.top = new FormAttachment(wTrimWhitespace, margin);
    fdNullIf.right = new FormAttachment(100, 0);
    wNullIf.setLayoutData(fdNullIf);

    // Error mismatch line
    Label wlColumnMismatch = new Label(gCsvGroup, SWT.RIGHT);
    wlColumnMismatch.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ColumnMismatch.Label"));
    wlColumnMismatch.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ColumnMismatch.Tooltip"));
    PropsUi.setLook(wlColumnMismatch);
    FormData fdlColumnMismatch = new FormData();
    fdlColumnMismatch.left = new FormAttachment(0, 0);
    fdlColumnMismatch.top = new FormAttachment(wNullIf, margin);
    fdlColumnMismatch.right = new FormAttachment(middle, -margin);
    wlColumnMismatch.setLayoutData(fdlColumnMismatch);

    wColumnMismatch = new Button(gCsvGroup, SWT.CHECK);
    PropsUi.setLook(wColumnMismatch);
    FormData fdColumnMismatch = new FormData();
    fdColumnMismatch.left = new FormAttachment(middle, 0);
    fdColumnMismatch.top = new FormAttachment(wNullIf, margin);
    fdColumnMismatch.right = new FormAttachment(100, 0);
    wColumnMismatch.setLayoutData(fdColumnMismatch);
    wColumnMismatch.addSelectionListener(bMod);

    ///////////////////////////
    // End CSV Group
    ///////////////////////////

    ///////////////////////////
    // Start JSON Group
    ///////////////////////////
    gJsonGroup = new Group(wDataTypeComp, SWT.SHADOW_ETCHED_IN);
    gJsonGroup.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.JsonGroup.Label"));
    FormLayout jsonLayout = new FormLayout();
    jsonLayout.marginWidth = 3;
    jsonLayout.marginHeight = 3;
    gJsonGroup.setLayout(jsonLayout);
    PropsUi.setLook(gJsonGroup);

    FormData fdgJsonGroup = new FormData();
    fdgJsonGroup.left = new FormAttachment(0, 0);
    fdgJsonGroup.right = new FormAttachment(100, 0);
    fdgJsonGroup.top = new FormAttachment(wDataType, margin * 2);
    fdgJsonGroup.bottom = new FormAttachment(100, -margin * 2);
    gJsonGroup.setLayoutData(fdgJsonGroup);

    // Strip Null line
    Label wlStripNull = new Label(gJsonGroup, SWT.RIGHT);
    wlStripNull.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.StripNull.Label"));
    wlStripNull.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.StripNull.Tooltip"));
    PropsUi.setLook(wlStripNull);
    FormData fdlStripNull = new FormData();
    fdlStripNull.left = new FormAttachment(0, 0);
    fdlStripNull.top = new FormAttachment(0, margin);
    fdlStripNull.right = new FormAttachment(middle, -margin);
    wlStripNull.setLayoutData(fdlStripNull);

    wStripNull = new Button(gJsonGroup, SWT.CHECK);
    PropsUi.setLook(wStripNull);
    FormData fdStripNull = new FormData();
    fdStripNull.left = new FormAttachment(middle, 0);
    fdStripNull.top = new FormAttachment(0, margin);
    fdStripNull.right = new FormAttachment(100, 0);
    wStripNull.setLayoutData(fdStripNull);
    wStripNull.addSelectionListener(bMod);

    // Ignore UTF8 line
    Label wlIgnoreUtf8 = new Label(gJsonGroup, SWT.RIGHT);
    wlIgnoreUtf8.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.IgnoreUtf8.Label"));
    wlIgnoreUtf8.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.IgnoreUtf8.Tooltip"));
    PropsUi.setLook(wlIgnoreUtf8);
    FormData fdlIgnoreUtf8 = new FormData();
    fdlIgnoreUtf8.left = new FormAttachment(0, 0);
    fdlIgnoreUtf8.top = new FormAttachment(wStripNull, margin);
    fdlIgnoreUtf8.right = new FormAttachment(middle, -margin);
    wlIgnoreUtf8.setLayoutData(fdlIgnoreUtf8);

    wIgnoreUtf8 = new Button(gJsonGroup, SWT.CHECK);
    PropsUi.setLook(wIgnoreUtf8);
    FormData fdIgnoreUtf8 = new FormData();
    fdIgnoreUtf8.left = new FormAttachment(middle, 0);
    fdIgnoreUtf8.top = new FormAttachment(wStripNull, margin);
    fdIgnoreUtf8.right = new FormAttachment(100, 0);
    wIgnoreUtf8.setLayoutData(fdIgnoreUtf8);
    wIgnoreUtf8.addSelectionListener(bMod);

    // Allow duplicate elements line
    Label wlAllowDuplicate = new Label(gJsonGroup, SWT.RIGHT);
    wlAllowDuplicate.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.AllowDuplicate.Label"));
    wlAllowDuplicate.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.AllowDuplicate.Tooltip"));
    PropsUi.setLook(wlAllowDuplicate);
    FormData fdlAllowDuplicate = new FormData();
    fdlAllowDuplicate.left = new FormAttachment(0, 0);
    fdlAllowDuplicate.top = new FormAttachment(wIgnoreUtf8, margin);
    fdlAllowDuplicate.right = new FormAttachment(middle, -margin);
    wlAllowDuplicate.setLayoutData(fdlAllowDuplicate);

    wAllowDuplicate = new Button(gJsonGroup, SWT.CHECK);
    PropsUi.setLook(wAllowDuplicate);
    FormData fdAllowDuplicate = new FormData();
    fdAllowDuplicate.left = new FormAttachment(middle, 0);
    fdAllowDuplicate.top = new FormAttachment(wIgnoreUtf8, margin);
    fdAllowDuplicate.right = new FormAttachment(100, 0);
    wAllowDuplicate.setLayoutData(fdAllowDuplicate);
    wAllowDuplicate.addSelectionListener(bMod);

    // Enable Octal line
    Label wlEnableOctal = new Label(gJsonGroup, SWT.RIGHT);
    wlEnableOctal.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.EnableOctal.Label"));
    wlEnableOctal.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.EnableOctal.Tooltip"));
    PropsUi.setLook(wlEnableOctal);
    FormData fdlEnableOctal = new FormData();
    fdlEnableOctal.left = new FormAttachment(0, 0);
    fdlEnableOctal.top = new FormAttachment(wAllowDuplicate, margin);
    fdlEnableOctal.right = new FormAttachment(middle, -margin);
    wlEnableOctal.setLayoutData(fdlEnableOctal);

    wEnableOctal = new Button(gJsonGroup, SWT.CHECK);
    PropsUi.setLook(wEnableOctal);
    FormData fdEnableOctal = new FormData();
    fdEnableOctal.left = new FormAttachment(middle, 0);
    fdEnableOctal.top = new FormAttachment(wAllowDuplicate, margin);
    fdEnableOctal.right = new FormAttachment(100, 0);
    wEnableOctal.setLayoutData(fdEnableOctal);
    wEnableOctal.addSelectionListener(bMod);

    ////////////////////////
    // End JSON Group
    ////////////////////////

    FormData fdDataTypeComp = new FormData();
    fdDataTypeComp.left = new FormAttachment(0, 0);
    fdDataTypeComp.top = new FormAttachment(0, 0);
    fdDataTypeComp.right = new FormAttachment(100, 0);
    fdDataTypeComp.bottom = new FormAttachment(100, 0);
    wDataTypeComp.setLayoutData(fdDataTypeComp);

    wDataTypeComp.layout();
    wDataTypeTab.setControl(wDataTypeComp);

    /* ******************************************
     * End Data type tab
     * ******************************************/

    /* ******************************************
     * Start Fields tab
     * This tab is used to specify the field mapping
     * to the Snowflake table
     * ******************************************/

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.FieldsTab.TabTitle"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // Specify Fields line
    wSpecifyFields = new Button(wFieldsComp, SWT.CHECK);
    wSpecifyFields.setText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.SpecifyFields.Label"));
    wSpecifyFields.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.SpecifyFields.Tooltip"));

    PropsUi.setLook(wSpecifyFields);
    FormData fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment(0, 0);
    fdSpecifyFields.top = new FormAttachment(0, margin);
    fdSpecifyFields.right = new FormAttachment(100, 0);
    wSpecifyFields.setLayoutData(fdSpecifyFields);
    wSpecifyFields.addSelectionListener(bMod);
    wSpecifyFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            setFlags();
          }
        });

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));
    fdGet = new FormData();
    fdGet.right = new FormAttachment(50, -margin);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    wDoMapping = new Button(wFieldsComp, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.DoMapping.Label"));
    wDoMapping.setToolTipText(
        BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.DoMapping.Tooltip"));
    FormData fdbDoMapping = new FormData();
    fdbDoMapping.left = new FormAttachment(50, margin);
    fdbDoMapping.bottom = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdbDoMapping);

    final int FieldsCols = 2;
    final int FieldsRows = input.getSnowflakeBulkLoaderFields().size();

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.StreamField.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.TableField.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    tableFieldColumns.add(colinf[1]);

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wSpecifyFields, margin * 3);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    // JSON Field Line
    //
    wlJsonField = new Label(wFieldsComp, SWT.RIGHT);
    wlJsonField.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.JsonField.Label"));
    PropsUi.setLook(wlJsonField);
    FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment(0, 0);
    fdlJsonField.top = new FormAttachment(0, margin);
    fdlJsonField.right = new FormAttachment(middle, -margin);
    wlJsonField.setLayoutData(fdlJsonField);

    wJsonField = new CCombo(wFieldsComp, SWT.BORDER | SWT.READ_ONLY);
    wJsonField.setEditable(false);
    PropsUi.setLook(wJsonField);
    wJsonField.addModifyListener(lsMod);
    FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment(middle, 0);
    fdJsonField.top = new FormAttachment(0, margin);
    fdJsonField.right = new FormAttachment(100, 0);
    wJsonField.setLayoutData(fdJsonField);
    wJsonField.addFocusListener(
        new FocusAdapter() {
          /**
           * Get the fields from the previous transform and populate the JSON Field drop down
           *
           * @param focusEvent The event
           */
          @Override
          public void focusGained(FocusEvent focusEvent) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
              String jsonField = wJsonField.getText();
              wJsonField.setItems(row.getFieldNames());
              if (jsonField != null) {
                wJsonField.setText(jsonField);
              }
            } catch (Exception ex) {
              String jsonField = wJsonField.getText();
              wJsonField.setItems(new String[] {});
              wJsonField.setText(jsonField);
            }
          }
        });

    //
    // Search the fields in the background and populate the CSV Field mapping table's stream field
    // column
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row =
                  pipelineMeta.getPrevTransformFields(
                      variables, SnowflakeBulkLoaderDialog.this.transformMeta);

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

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wSql = new Button(shell, SWT.PUSH);
    wSql.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.SQL.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wSql, wCancel}, margin, null);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wbTable.addListener(SWT.Selection, e -> getTableName());
    wbSchema.addListener(SWT.Selection, e -> getSchemaNames());

    // Whenever something changes, set the tooltip to the expanded version:
    wSchema.addModifyListener(e -> wSchema.setToolTipText(variables.resolve(wSchema.getText())));

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });
    wSql.addListener(SWT.Selection, e -> create());
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());
    wGet.addListener(SWT.Selection, e -> get());
    wDoMapping.addListener(SWT.Selection, e -> generateMappings());

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    wTabFolder.setSelection(0);

    // Set the shell size, based upon previous time...
    setSize();

    getData();

    setTableFieldCombo();
    setFlags();

    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Sets the input stream field names in the JSON field drop down, and the Stream field drop down
   * in the field mapping table.
   */
  private void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  private void getData() {
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    if (input.getTargetSchema() != null) {
      wSchema.setText(input.getTargetSchema());
    }

    if (input.getTargetTable() != null) {
      wTable.setText(input.getTargetTable());
    }

    if (input.getLocationType() != null) {
      wLocationType.setText(LOCATION_TYPE_COMBO[input.getLocationTypeId()]);
    }

    if (input.getStageName() != null) {
      wStageName.setText(input.getStageName());
    }

    if (input.getWorkDirectory() != null) {
      wWorkDirectory.setText(input.getWorkDirectory());
    }

    if (input.getOnError() != null) {
      wOnError.setText(ON_ERROR_COMBO[input.getOnErrorId()]);
    }

    if (input.getErrorLimit() != null) {
      wErrorLimit.setText(input.getErrorLimit());
    }

    if (input.getSplitSize() != null) {
      wSplitSize.setText(input.getSplitSize());
    }

    wRemoveFiles.setSelection(input.isRemoveFiles());

    if (input.getDataType() != null) {
      wDataType.setText(DATA_TYPE_COMBO[input.getDataTypeId()]);
    }

    wTrimWhitespace.setSelection(input.isTrimWhitespace());

    if (input.getNullIf() != null) {
      wNullIf.setText(input.getNullIf());
    }

    wColumnMismatch.setSelection(input.isErrorColumnMismatch());

    wStripNull.setSelection(input.isStripNull());

    wIgnoreUtf8.setSelection(input.isIgnoreUtf8());

    wAllowDuplicate.setSelection(input.isAllowDuplicateElements());

    wEnableOctal.setSelection(input.isEnableOctal());

    wSpecifyFields.setSelection(input.isSpecifyFields());

    if (input.getJsonField() != null) {
      wJsonField.setText(input.getJsonField());
    }

    logDebug("getting fields info...");

    for (int i = 0; i < input.getSnowflakeBulkLoaderFields().size(); i++) {
      SnowflakeBulkLoaderField field = input.getSnowflakeBulkLoaderFields().get(i);

      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getStreamField(), ""));
      item.setText(2, Const.NVL(field.getTableField(), ""));
    }

    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  /**
   * Cancel making changes. Do not save any of the changes and do not set the pipeline as changed.
   */
  private void cancel() {
    transformName = null;

    input.setChanged(backupChanged);

    dispose();
  }

  /**
   * Save the transform settings to the transform metadata
   *
   * @param sbl The transform metadata
   */
  private void getInfo(SnowflakeBulkLoaderMeta sbl) {
    sbl.setConnection(wConnection.getText());
    sbl.setTargetSchema(wSchema.getText());
    sbl.setTargetTable(wTable.getText());
    sbl.setLocationTypeById(wLocationType.getSelectionIndex());
    sbl.setStageName(wStageName.getText());
    sbl.setWorkDirectory(wWorkDirectory.getText());
    sbl.setOnErrorById(wOnError.getSelectionIndex());
    sbl.setErrorLimit(wErrorLimit.getText());
    sbl.setSplitSize(wSplitSize.getText());
    sbl.setRemoveFiles(wRemoveFiles.getSelection());

    sbl.setDataTypeById(wDataType.getSelectionIndex());
    sbl.setTrimWhitespace(wTrimWhitespace.getSelection());
    sbl.setNullIf(wNullIf.getText());
    sbl.setErrorColumnMismatch(wColumnMismatch.getSelection());
    sbl.setStripNull(wStripNull.getSelection());
    sbl.setIgnoreUtf8(wIgnoreUtf8.getSelection());
    sbl.setAllowDuplicateElements(wAllowDuplicate.getSelection());
    sbl.setEnableOctal(wEnableOctal.getSelection());

    sbl.setSpecifyFields(wSpecifyFields.getSelection());
    sbl.setJsonField(wJsonField.getText());

    int nrfields = wFields.nrNonEmpty();

    List<SnowflakeBulkLoaderField> fields = new ArrayList<>();

    for (int i = 0; i < nrfields; i++) {
      SnowflakeBulkLoaderField field = new SnowflakeBulkLoaderField();

      TableItem item = wFields.getNonEmpty(i);
      field.setStreamField(item.getText(1));
      field.setTableField(item.getText(2));
      fields.add(field);
    }
    sbl.setSnowflakeBulkLoaderFields(fields);
  }

  /** Save the transform settings and close the dialog */
  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  /**
   * Get the fields from the previous transform and load the field mapping table with a direct
   * mapping of input fields to table fields.
   */
  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /**
   * Reads in the fields from the previous transform and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "SnowflakeBulkLoader.Dialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "SnowflakeBulkLoader.Dialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }

    // refresh data
    input.setConnection(wConnection.getText());
    input.setTargetTable(variables.resolve(wTable.getText()));
    input.setTargetSchema(variables.resolve(wSchema.getText()));
    ITransformMeta iTransformMeta = transformMeta.getTransform();
    try {
      targetFields = iTransformMeta.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "SnowflakeBulkLoader.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "SnowflakeBulkLoader.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      String source = item.getText(1);
      String target = item.getText(2);

      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      int targetIndex = targetFields.indexOfValue(target);
      if (targetIndex < 0) {
        missingTargetFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      if (sourceIndex < 0 || targetIndex < 0) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
      mappings.add(mapping);
    }

    // show a confirm dialog if some missing field was found
    //
    if (!missingSourceFields.isEmpty() || !missingTargetFields.isEmpty()) {

      String message = "";
      if (!missingSourceFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "SnowflakeBulkLoader.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "SnowflakeBulkLoader.DoMapping.SomeTargetFieldsNotFound",
                    missingTargetFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      shell.setImage(GuiResource.getInstance().getImageHopUi());
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(PKG, "SnowflakeBulkLoader.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      boolean goOn = (answer & SWT.YES) != 0;
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            SnowflakeBulkLoaderDialog.this.shell,
            sourceFields.getFieldNames(),
            targetFields.getFieldNames(),
            mappings);
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if (mappings != null) {
      // Clear and re-populate!
      //
      wFields.table.removeAll();
      wFields.table.setItemCount(mappings.size());
      for (int i = 0; i < mappings.size(); i++) {
        SourceToTargetMapping mapping = mappings.get(i);
        TableItem item = wFields.table.getItem(i);
        item.setText(1, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
        item.setText(2, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }
  }

  /**
   * Presents a dialog box to select a schema from the database. Then sets the selected schema in
   * the dialog
   */
  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG,
                      "SnowflakeBulkLoader.Dialog.AvailableSchemas.Title",
                      wConnection.getText()),
                  BaseMessages.getString(
                      PKG,
                      "SnowflakeBulkLoader.Dialog.AvailableSchemas.Message",
                      wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorGettingSchemas"),
            e);
      }
    }
  }

  /** Opens a dialog to select a table name */
  private void getTableName() {
    // New class: SelectTableDialog
    int connr = wConnection.getSelectionIndex();
    if (connr >= 0) {
      DatabaseMeta inf = pipelineMeta.getDatabases().get(connr);

      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "SnowflakeBulkLoader.Dialog..Log.LookingAtConnection", inf.toString()));
      }

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(shell, SWT.NONE, variables, inf, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
    }
  }

  /** Sets the values for the combo box in the table field on the fields tab */
  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
            final String tableName = wTable.getText(),
                connectionName = wConnection.getText(),
                schemaName = wSchema.getText();

            // clear
            for (ColumnInfo tableField : tableFieldColumns) {
              tableField.setComboValues(new String[] {});
            }
            if (!StringUtils.isEmpty(tableName)) {
              DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
              if (databaseMeta != null) {
                try (Database db = new Database(loggingObject, variables, databaseMeta)) {
                  db.connect();

                  String schemaTable =
                      databaseMeta.getQuotedSchemaTableCombination(
                          variables, variables.resolve(schemaName), variables.resolve(tableName));
                  IRowMeta r = db.getTableFields(schemaTable);
                  if (null != r) {
                    String[] fieldNames = r.getFieldNames();
                    if (null != fieldNames) {
                      for (ColumnInfo tableField : tableFieldColumns) {
                        tableField.setComboValues(fieldNames);
                      }
                    }
                  }
                } catch (Exception e) {
                  for (ColumnInfo tableField : tableFieldColumns) {
                    tableField.setComboValues(new String[] {});
                  }
                }
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /** Enable and disable fields based on selection changes */
  private void setFlags() {
    /////////////////////////////////
    // On Error
    ////////////////////////////////
    if (wOnError.getSelectionIndex() == SnowflakeBulkLoaderMeta.ON_ERROR_SKIP_FILE) {
      wlErrorLimit.setText(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Label"));
      wlErrorLimit.setToolTipText(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Tooltip"));
      wlErrorLimit.setEnabled(true);
      wErrorLimit.setEnabled(true);
    } else if (wOnError.getSelectionIndex() == SnowflakeBulkLoaderMeta.ON_ERROR_SKIP_FILE_PERCENT) {
      wlErrorLimit.setText(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorPercentLimit.Label"));
      wlErrorLimit.setToolTipText(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Dialog.ErrorPercentLimit.Tooltip"));
      wlErrorLimit.setEnabled(true);
      wErrorLimit.setEnabled(true);
    } else {
      wlErrorLimit.setEnabled(false);
      wErrorLimit.setEnabled(false);
    }

    ////////////////////////////
    // Location Type
    ////////////////////////////
    if (wLocationType.getSelectionIndex() == SnowflakeBulkLoaderMeta.LOCATION_TYPE_INTERNAL_STAGE) {
      wStageName.setEnabled(true);
    } else {
      wStageName.setEnabled(false);
    }

    ////////////////////////////
    // Data Type
    ////////////////////////////

    if (wDataType.getSelectionIndex() == SnowflakeBulkLoaderMeta.DATA_TYPE_JSON) {
      gCsvGroup.setVisible(false);
      gJsonGroup.setVisible(true);
      wJsonField.setVisible(true);
      wlJsonField.setVisible(true);
      wSpecifyFields.setVisible(false);
      wFields.setVisible(false);
      wGet.setVisible(false);
      wDoMapping.setVisible(false);
    } else {
      gCsvGroup.setVisible(true);
      gJsonGroup.setVisible(false);
      wJsonField.setVisible(false);
      wlJsonField.setVisible(false);
      wSpecifyFields.setVisible(true);
      wFields.setVisible(true);
      wFields.setEnabled(wSpecifyFields.getSelection());
      wFields.table.setEnabled(wSpecifyFields.getSelection());
      if (wSpecifyFields.getSelection()) {
        wFields.setForeground(display.getSystemColor(SWT.COLOR_GRAY));
      } else {
        wFields.setForeground(display.getSystemColor(SWT.COLOR_BLACK));
      }
      wGet.setVisible(true);
      wGet.setEnabled(wSpecifyFields.getSelection());
      wDoMapping.setVisible(true);
      wDoMapping.setEnabled(wSpecifyFields.getSelection());
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);

    try {
      SnowflakeBulkLoaderMeta info = new SnowflakeBulkLoaderMeta();
      getInfo(info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);

      if (info.isSpecifyFields()) {
        // Only use the fields that were specified.
        IRowMeta prevNew = new RowMeta();

        for (int i = 0; i < info.getSnowflakeBulkLoaderFields().size(); i++) {
          SnowflakeBulkLoaderField sf = info.getSnowflakeBulkLoaderFields().get(i);
          IValueMeta insValue = prev.searchValueMeta(sf.getStreamField());
          if (insValue != null) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName(sf.getTableField());
            prevNew.addValueMeta(insertValue);
          } else {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "TableOutputDialog.FailedToFindField.Message", sf.getStreamField()));
          }
        }
        prev = prevNew;
      }

      if (isValidRowMeta(prev)) {
        SqlStatement sql =
            info.getSqlStatements(variables, pipelineMeta, transformMeta, prev, metadataProvider);
        if (!sql.hasError()) {
          if (sql.hasSql()) {
            SqlEditor sqledit =
                new SqlEditor(
                    shell, SWT.NONE, variables, databaseMeta, DbCache.getInstance(), sql.getSql());
            sqledit.open();
          } else {
            MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
            mb.setMessage(
                BaseMessages.getString(PKG, "SnowflakeBulkLoaderDialog.NoSQLNeeds.DialogMessage"));
            mb.setText(
                BaseMessages.getString(PKG, "SnowflakeBulkLoaderDialog.NoSQLNeeds.DialogTitle"));
            mb.open();
          }
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(sql.getError());
          mb.setText(BaseMessages.getString(PKG, "SnowBulkLoaderDialog.SQLError.DialogTitle"));
          mb.open();
        }
      }

    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SnowBulkLoaderDialog.BuildSQLError.DialogTitle"),
          BaseMessages.getString(PKG, "SnowBulkLoaderDialog.BuildSQLError.DialogMessage"),
          ke);
      ke.printStackTrace();
    }
  }

  private static boolean isValidRowMeta(IRowMeta rowMeta) {
    for (IValueMeta value : rowMeta.getValueMetaList()) {
      String name = value.getName();
      if (name == null || name.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
