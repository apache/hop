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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import com.mongodb.DBObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.NamedReadPreference;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

/** Dialog class for MongoDbDelete step */
public class MongoDbDeleteDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = MongoDbDeleteDialog.class;

  private MetaSelectionLine<MongoDbConnection> wConnection;
  protected MongoDbDeleteMeta currentMeta;
  protected MongoDbDeleteMeta originalMeta;
  private Button wbGetFields;
  private Button wbPreviewDocStruct;
  private CCombo wCollection;
  private CCombo wcbWriteConcern;
  private TextVar wtvTimeout;
  private Button wcbJournalWritesCheck;
  private CCombo wcbReadPreference;
  private TextVar wtvWriteRetries;
  private TextVar wtvWriteRetryDelay;
  private TableView wtvMongoFieldsView;
  private StyledTextComp wstJsonQueryView;
  private Button wbUseJsonQuery;
  private Label wlExecuteForEachRow;
  private Button wcbEcuteForEachRow;
  private ColumnInfo[] colInf;
  private Map<String, Integer> inputFields;

  public MongoDbDeleteDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String name) {

    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, name);
    currentMeta = (MongoDbDeleteMeta) in;
    originalMeta = (MongoDbDeleteMeta) currentMeta.clone();
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, currentMeta);

    // used to listen to a text field (wTransformName)
    ModifyListener lsMod = e -> currentMeta.setChanged();

    changed = currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    /** various UI bits and pieces for the dialog */
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.TransformName.Label"));
    props.setLook(wlTransformName);

    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fd);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    Control lastControl = wTransformName;

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fd);

    // The tabs of the dialog
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setSimple(false);

    // --- start of the options tab
    CTabItem wDeleteOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wDeleteOptionsTab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.DeleteTab.TabTitle"));
    Composite wOutputComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOutputComp);
    FormLayout outputLayout = new FormLayout();
    outputLayout.marginWidth = 3;
    outputLayout.marginHeight = 3;
    wOutputComp.setLayout(outputLayout);

    // The connection to use...
    //
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            MongoDbConnection.class,
            wOutputComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "MongoDbDeleteDialog.ConnectionName.Label"),
            BaseMessages.getString(PKG, "MongoDbDeleteDialog.ConnectionName.Tooltip"));
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, 0);
    wConnection.setLayoutData(fdConnection);
    lastControl = wConnection;

    try {
      wConnection.fillItems();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error loading list of MongoDB connection names", e);
    }

    // collection line
    Label wlCollection = new Label(wOutputComp, SWT.RIGHT);
    wlCollection.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.Collection.Label")); // $NON-NLS-1$
    wlCollection.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.Collection.TipText")); // $NON-NLS-1$
    props.setLook(wlCollection);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlCollection.setLayoutData(fd);

    Button wbGetCollections = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetCollections);
    wbGetCollections.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.GetCollections.Button")); // $NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(lastControl, 0);
    wbGetCollections.setLayoutData(fd);
    wbGetCollections.addListener(SWT.Selection, e -> getCollectionNames());

    wCollection = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wCollection);
    wCollection.addListener(
        SWT.Modify,
        e -> {
          currentMeta.setChanged();
          wCollection.setToolTipText(variables.resolve(wCollection.getText()));
        });
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(wbGetCollections, -margin);
    wCollection.setLayoutData(fd);

    // write concern
    Label writeConcernLab = new Label(wOutputComp, SWT.RIGHT);
    writeConcernLab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteConcern.Label")); // $NON-NLS-1$
    writeConcernLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteConcern.TipText")); // $NON-NLS-1$
    props.setLook(writeConcernLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wCollection, margin);
    fd.right = new FormAttachment(middle, -margin);
    writeConcernLab.setLayoutData(fd);

    Button getCustomWCBut = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
    props.setLook(getCustomWCBut);
    getCustomWCBut.setText(
        BaseMessages.getString(
            PKG, "MongoDbDeleteDialog.WriteConcern.CustomWriteConcerns")); // $NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wCollection, 0);
    getCustomWCBut.setLayoutData(fd);

    getCustomWCBut.addListener(SWT.Selection, e -> setupCustomWriteConcernNames());

    wcbWriteConcern = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wcbWriteConcern);
    wcbWriteConcern.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(getCustomWCBut, 0);
    fd.top = new FormAttachment(wCollection, margin);
    fd.left = new FormAttachment(middle, 0);
    wcbWriteConcern.setLayoutData(fd);

    // wTimeout
    Label wTimeoutLab = new Label(wOutputComp, SWT.RIGHT);
    wTimeoutLab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WTimeout.Label")); // $NON-NLS-1$
    wTimeoutLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WTimeout.TipText")); // $NON-NLS-1$
    props.setLook(wTimeoutLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wcbWriteConcern, margin);
    fd.right = new FormAttachment(middle, -margin);
    wTimeoutLab.setLayoutData(fd);

    wtvTimeout = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wtvTimeout);
    wtvTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wcbWriteConcern, margin);
    fd.left = new FormAttachment(middle, 0);
    wtvTimeout.setLayoutData(fd);
    wtvTimeout.addModifyListener(
        e -> wtvTimeout.setToolTipText(variables.resolve(wtvTimeout.getText())));

    Label journalWritesLab = new Label(wOutputComp, SWT.RIGHT);
    journalWritesLab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.JournalWrites.Label")); // $NON-NLS-1$
    journalWritesLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.JournalWrites.TipText")); // $NON-NLS-1$
    props.setLook(journalWritesLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wtvTimeout, margin);
    fd.right = new FormAttachment(middle, -margin);
    journalWritesLab.setLayoutData(fd);

    wcbJournalWritesCheck = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wcbJournalWritesCheck);
    wcbJournalWritesCheck.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            currentMeta.setChanged();
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(journalWritesLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wcbJournalWritesCheck.setLayoutData(fd);

    // read preference
    Label readPrefL = new Label(wOutputComp, SWT.RIGHT);
    readPrefL.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.ReadPreferenceLabel")); // $NON-NLS-1$
    readPrefL.setToolTipText(
        BaseMessages.getString(
            PKG, "MongoDbDeleteDialog.ReadPreferenceLabel.TipText")); // $NON-NLS-1$
    props.setLook(readPrefL);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wcbJournalWritesCheck, margin * 3);
    fd.right = new FormAttachment(middle, -margin);
    readPrefL.setLayoutData(fd);

    wcbReadPreference = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wcbReadPreference);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wcbJournalWritesCheck, margin * 3);
    fd.right = new FormAttachment(100, 0);
    wcbReadPreference.setLayoutData(fd);
    wcbReadPreference.addModifyListener(
        e -> {
          currentMeta.setChanged();
          wcbReadPreference.setToolTipText(variables.resolve(wcbReadPreference.getText()));
        });
    wcbReadPreference.add(NamedReadPreference.PRIMARY.getName());
    wcbReadPreference.add(NamedReadPreference.PRIMARY_PREFERRED.getName());
    wcbReadPreference.add(NamedReadPreference.SECONDARY.getName());
    wcbReadPreference.add(NamedReadPreference.SECONDARY_PREFERRED.getName());
    wcbReadPreference.add(NamedReadPreference.NEAREST.getName());

    // retries stuff
    Label retriesLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesLab);
    retriesLab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetries.Label")); // $NON-NLS-1$
    retriesLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetries.TipText")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wcbReadPreference, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesLab.setLayoutData(fd);

    wtvWriteRetries = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wtvWriteRetries);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wcbReadPreference, margin);
    fd.right = new FormAttachment(100, 0);
    wtvWriteRetries.setLayoutData(fd);
    wtvWriteRetries.addModifyListener(
        e -> wtvWriteRetries.setToolTipText(variables.resolve(wtvWriteRetries.getText())));

    Label retriesDelayLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesDelayLab);
    retriesDelayLab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetriesDelay.Label")); // $NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wtvWriteRetries, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesDelayLab.setLayoutData(fd);

    wtvWriteRetryDelay = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wtvWriteRetryDelay);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wtvWriteRetries, margin);
    fd.right = new FormAttachment(100, 0);
    wtvWriteRetryDelay.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wOutputComp.setLayoutData(fd);

    wOutputComp.layout();
    wDeleteOptionsTab.setControl(wOutputComp);

    // --- start of the fields tab
    CTabItem mWQueryTab = new CTabItem(wTabFolder, SWT.NONE);
    mWQueryTab.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.QueryTab.TabTitle")); // $NON-NLS-1$
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = 3;
    filterLayout.marginHeight = 3;
    wFieldsComp.setLayout(filterLayout);

    // use query
    Label useDefinedQueryLab = new Label(wFieldsComp, SWT.RIGHT);
    useDefinedQueryLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.useQuery.Label"));
    useDefinedQueryLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.useQuery.TipText"));
    props.setLook(useDefinedQueryLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(middle, -margin);
    useDefinedQueryLab.setLayoutData(fd);

    wbUseJsonQuery = new Button(wFieldsComp, SWT.CHECK);
    props.setLook(wbUseJsonQuery);
    wbUseJsonQuery.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            currentMeta.setChanged();
            if (wbUseJsonQuery.getSelection()) {
              // show query
              setQueryJsonVisibility(true);
              // hide m_mongoFields
              setQueryFieldVisiblity(false);
            } else {
              // show m_mongoFieldsView
              setQueryFieldVisiblity(true);
              // hide query
              setQueryJsonVisibility(false);
            }
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, -margin);
    fd.top = new FormAttachment(0, margin * 3);
    fd.left = new FormAttachment(middle, 0);
    wbUseJsonQuery.setLayoutData(fd);

    colInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Path"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Comparator"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Incoming1"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Incoming2"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false)
        };

    colInf[1].setComboValues(Comparator.asLabel());
    colInf[1].setReadOnly(true);

    // Search the fields in the background
    final Runnable runnable =
        new Runnable() {
          public void run() {
            TransformMeta stepMeta = pipelineMeta.findTransform(transformName);
            if (stepMeta != null) {
              try {
                IRowMeta row = pipelineMeta.getPrevTransformFields(variables, stepMeta);

                // Remember these fields...
                for (int i = 0; i < row.size(); i++) {
                  inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                }

                setComboBoxes();
              } catch (HopTransformException e) {
                log.logError(
                    toString(),
                    BaseMessages.getString(PKG, "MongoDbDeleteDialog.Log.UnableToFindInput"));
              }
            }
          }
        };
    new Thread(runnable).start();

    // get fields but
    wbGetFields = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.GetFieldsBut"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, margin);
    wbGetFields.setLayoutData(fd);

    wbGetFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getFields();
          }
        });

    wbPreviewDocStruct = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbPreviewDocStruct);
    wbPreviewDocStruct.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewDocStructBut"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(wbGetFields, margin);
    wbPreviewDocStruct.setLayoutData(fd);
    wbPreviewDocStruct.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            previewDocStruct();
          }
        });

    wtvMongoFieldsView =
        new TableView(
            variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colInf, 1, lsMod, props);
    fd = new FormData();
    fd.top = new FormAttachment(wbUseJsonQuery, margin * 2);
    fd.bottom = new FormAttachment(wbGetFields, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wtvMongoFieldsView.setLayoutData(fd);

    // JSON Query
    wlExecuteForEachRow = new Label(wFieldsComp, SWT.RIGHT);
    wlExecuteForEachRow.setText(
        BaseMessages.getString(PKG, "MongoDbDeleteDialog.execEachRow.Label"));
    props.setLook(wlExecuteForEachRow);
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, margin);
    wlExecuteForEachRow.setLayoutData(fd);

    wcbEcuteForEachRow = new Button(wFieldsComp, SWT.CHECK);
    props.setLook(wcbEcuteForEachRow);
    wcbEcuteForEachRow.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            currentMeta.setChanged();
          }
        });
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(wlExecuteForEachRow, margin);
    wcbEcuteForEachRow.setLayoutData(fd);

    wstJsonQueryView =
        new StyledTextComp(
            variables,
            wFieldsComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wstJsonQueryView, Props.WIDGET_STYLE_FIXED);
    wstJsonQueryView.addModifyListener(lsMod);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, -margin * 3);
    fd.top = new FormAttachment(wbUseJsonQuery, margin * 2);
    fd.bottom = new FormAttachment(wlExecuteForEachRow, -margin * 2);
    wstJsonQueryView.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fd);

    wFieldsComp.layout();
    mWQueryTab.setControl(wFieldsComp);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fd);

    // Buttons inherited from BaseStepDialog
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK")); // $NON-NLS-1$

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); // $NON-NLS-1$

    setButtonPositions(new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    wTransformName.addListener(SWT.Selection, e -> ok());

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    wTabFolder.setSelection(0);
    setSize();

    getData();

    // hide if not use json query
    if (currentMeta.isUseJsonQuery()) {
      setQueryFieldVisiblity(false);
    } else {
      setQueryJsonVisibility(false);
    }

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void cancel() {
    transformName = null;
    currentMeta.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtil.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();

    getInfo(currentMeta);

    if ((!currentMeta.isUseJsonQuery())
        && (currentMeta.getMongoFields() == null || currentMeta.getMongoFields().isEmpty())) {
      // popup dialog warning that no paths have been defined
      showNoFieldMessageDialog();
    } else if (currentMeta.isUseJsonQuery() && StringUtil.isEmpty(currentMeta.getJsonQuery())) {
      showNoQueryWarningDialog();
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  private void setupCustomWriteConcernNames() {
    try {
      String connectionName = variables.resolve(wConnection.getText());
      MongoDbConnection connection =
          metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);

      if (!StringUtil.isEmpty(connectionName)) {
        MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
        getInfo(meta);
        try {
          MongoClientWrapper wrapper = connection.createWrapper(variables, log);
          List<String> custom = new ArrayList<>();
          try {
            custom = wrapper.getLastErrorModes();
          } finally {
            wrapper.dispose();
          }

          if (!custom.isEmpty()) {
            String current = wcbWriteConcern.getText();
            wcbWriteConcern.removeAll();

            for (String s : custom) {
              wcbWriteConcern.add(s);
            }

            if (!StringUtil.isEmpty(current)) {
              wcbWriteConcern.setText(current);
            }
          }
        } catch (Exception e) {
          logError(
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e);
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage." + "UnableToConnect"),
              BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"),
              e);
        }
      } else {
        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_WARNING | SWT.OK,
                BaseMessages.getString(
                    PKG, "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails.Title"),
                BaseMessages.getString(
                    PKG,
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails",
                    "host name(s)"));
        smd.open();
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting collections", e);
    }
  }

  private void getFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wtvMongoFieldsView, 1, new int[] {1, 3}, null, -1, -1, null);
      }
    } catch (HopTransformException e) {
      logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), e);
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          e);
    }
  }

  private void getInfo(MongoDbDeleteMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setCollection(wCollection.getText());
    meta.setWriteConcern(wcbWriteConcern.getText());
    meta.setWTimeout(wtvTimeout.getText());
    meta.setUseJournaledWrite(wcbJournalWritesCheck.getSelection());
    meta.setReadPreference(wcbReadPreference.getText());
    meta.setWriteRetries(wtvWriteRetries.getText());
    meta.setWriteRetryDelay(wtvWriteRetryDelay.getText());
    meta.setUseJsonQuery(wbUseJsonQuery.getSelection());
    meta.setExecuteForEachIncomingRow(wcbEcuteForEachRow.getSelection());
    meta.setJsonQuery(wstJsonQueryView.getText());
    meta.setMongoFields(tableToMongoFieldList());
  }

  private List<MongoDbDeleteField> tableToMongoFieldList() {
    int numNonEmpty = wtvMongoFieldsView.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoDbDeleteField> mongoFields = new ArrayList<>();

      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wtvMongoFieldsView.getNonEmpty(i);
        String path = item.getText(1).trim();
        String comparator = item.getText(2).trim();
        String field1 = item.getText(3).trim();
        String field2 = item.getText(4).trim();

        MongoDbDeleteField newField = new MongoDbDeleteField();
        newField.mongoDocPath = path;
        if (StringUtil.isEmpty(comparator)) {
          comparator = Comparator.EQUAL.getValue();
        }
        newField.comparator = comparator;
        newField.incomingField1 = field1;
        newField.incomingField2 = field2;
        mongoFields.add(newField);
      }

      return mongoFields;
    }

    return null;
  }

  private void getData() {
    wConnection.setText(Const.NVL(currentMeta.getConnectionName(), ""));
    wCollection.setText(Const.NVL(currentMeta.getCollection(), "")); // $NON-NLS-1$

    wcbWriteConcern.setText(Const.NVL(currentMeta.getWriteConcern(), "")); // $NON-NLS-1$
    wtvTimeout.setText(Const.NVL(currentMeta.getWTimeout(), "")); // $NON-NLS-1$
    wcbJournalWritesCheck.setSelection(currentMeta.isUseJournaledWrite());
    wcbReadPreference.setText(Const.NVL(currentMeta.getReadPreference(), "")); // $NON-NLS-1$
    wtvWriteRetries.setText(
        Const.NVL(
            currentMeta.getWriteRetries(),
            "" //$NON-NLS-1$
                + currentMeta.nbRetries));
    wtvWriteRetryDelay.setText(
        Const.NVL(
            currentMeta.getWriteRetryDelay(),
            "" //$NON-NLS-1$
                + currentMeta.nbRetries));

    wbUseJsonQuery.setSelection(currentMeta.isUseJsonQuery());
    wcbEcuteForEachRow.setSelection(currentMeta.isExecuteForEachIncomingRow());
    wstJsonQueryView.setText(Const.NVL(currentMeta.getJsonQuery(), ""));

    List<MongoDbDeleteField> mongoFields = currentMeta.getMongoFields();

    if (mongoFields != null && !mongoFields.isEmpty()) {
      for (MongoDbDeleteField field : mongoFields) {
        TableItem item = new TableItem(wtvMongoFieldsView.table, SWT.NONE);

        item.setText(1, Const.NVL(field.mongoDocPath, ""));
        item.setText(2, Const.NVL(field.comparator, ""));
        item.setText(3, Const.NVL(field.incomingField1, ""));
        item.setText(4, Const.NVL(field.incomingField2, ""));
      }

      wtvMongoFieldsView.removeEmptyRows();
      wtvMongoFieldsView.setRowNums();
      wtvMongoFieldsView.optWidth(true);
    }
  }

  protected void setComboBoxes() {
    final Map<String, Integer> fields = new HashMap<>();

    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String fieldNames[] = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    colInf[2].setComboValues(fieldNames);
    colInf[2].setReadOnly(false);
    colInf[3].setComboValues(fieldNames);
    colInf[3].setReadOnly(false);
  }

  private void getCollectionNames() {

    try {
      String connectionName = variables.resolve(wConnection.getText());

      String current = wCollection.getText();
      wCollection.removeAll();

      MongoDbConnection connection =
          metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
      String databaseName = variables.resolve(connection.getDbName());

      if (!StringUtils.isEmpty(connectionName)) {

        final MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
        getInfo(meta);
        try {
          MongoClientWrapper wrapper = connection.createWrapper(variables, log);
          Set<String> collections;
          try {
            collections = wrapper.getCollectionsNames(databaseName);
          } finally {
            wrapper.dispose();
          }

          for (String c : collections) {
            wCollection.add(c);
          }
        } catch (Exception e) {
          logError(
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              e);
        }
      } else {
        // popup some feedback

        String missingConnDetails = "";
        if (StringUtils.isEmpty(connectionName)) {
          missingConnDetails += "connection name";
        }
        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_WARNING | SWT.OK,
                BaseMessages.getString(
                    PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title"),
                BaseMessages.getString(
                    PKG,
                    "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                    missingConnDetails));
        smd.open();
      }

      if (!StringUtils.isEmpty(current)) {
        wCollection.setText(current);
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting collections", e);
    }
  }

  private void previewDocStruct() {
    List<MongoDbDeleteField> mongoFields = tableToMongoFieldList();

    if (mongoFields == null || mongoFields.isEmpty()) {
      // popup dialog warning that no paths have been defined
      showNoFieldMessageDialog();
      return;
    }

    // Try and get meta data on incoming fields
    IRowMeta actualR = null;
    IRowMeta r;
    boolean gotGenuineRowMeta = false;
    try {
      actualR = pipelineMeta.getPrevTransformFields(variables, transformName);
      gotGenuineRowMeta = true;
    } catch (HopTransformException e) {
      // don't complain if we can't
    }
    r = new RowMeta();

    Object[] dummyRow =
        new Object
            [mongoFields.size()
                * 2]; // multiply by 2, because possiblity use between that required 2 value
    int i = 0;
    try {
      for (MongoDbDeleteField field : mongoFields) {
        // set up dummy row meta
        if (!StringUtil.isEmpty(field.incomingField1)
            && !StringUtil.isEmpty(field.incomingField2)) {
          IValueMeta vm1 = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
          vm1.setName(field.incomingField1);
          r.addValueMeta(vm1);

          IValueMeta vm2 = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
          vm2.setName(field.incomingField2);
          r.addValueMeta(vm2);

          String val1 = getValueToDisplay(gotGenuineRowMeta, actualR, field.incomingField1);
          dummyRow[i++] = val1;

          String val2 = getValueToDisplay(gotGenuineRowMeta, actualR, field.incomingField2);
          dummyRow[i++] = val2;

        } else {
          IValueMeta vm = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
          vm.setName(field.incomingField1);
          r.addValueMeta(vm);
          String val = getValueToDisplay(gotGenuineRowMeta, actualR, field.incomingField1);
          dummyRow[i++] = val;
        }
      }

      IVariables vs = new Variables();
      for (MongoDbDeleteField m : mongoFields) {
        m.init(vs);
      }

      String toDisplay = "";
      String windowTitle =
          BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewDocStructure.Title");
      DBObject query = MongoDbDeleteData.getQueryObject(mongoFields, r, dummyRow, vs);
      toDisplay =
          BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewModifierUpdate.Heading1")
              + ": \n\n"
              + prettyPrintDocStructure(query.toString());

      ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_INFORMATION | SWT.OK, windowTitle, toDisplay, true);
      smd.open();
    } catch (Exception ex) {
      logError(
          BaseMessages.getString(
                  PKG, "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              + ":\n\n"
              + ex.getMessage(),
          ex);
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Title"),
          BaseMessages.getString(
                  PKG, "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              + ":\n\n"
              + ex.getMessage(),
          ex);
      return;
    }
  }

  private String getValueToDisplay(boolean genuineRowMeta, IRowMeta rmi, String fieldName) {
    String val = "";
    if (genuineRowMeta && rmi.indexOfValue(fieldName) >= 0) {
      int index = rmi.indexOfValue(fieldName);
      switch (rmi.getValueMeta(index).getType()) {
        case IValueMeta.TYPE_STRING:
          val = "<string val>";
          break;
        case IValueMeta.TYPE_INTEGER:
          val = "<integer val>";
          break;
        case IValueMeta.TYPE_NUMBER:
          val = "<number val>";
          break;
        case IValueMeta.TYPE_BOOLEAN:
          val = "<bool val>";
          break;
        case IValueMeta.TYPE_DATE:
          val = "<date val>";
          break;
        case IValueMeta.TYPE_BINARY:
          val = "<binary val>";
          break;
        default:
          val = "<unsupported value type>";
      }
    } else {
      val = "<value>";
    }
    return val;
  }

  private static enum Element {
    OPEN_BRACE,
    CLOSE_BRACE,
    OPEN_BRACKET,
    CLOSE_BRACKET,
    COMMA
  };

  private static void pad(StringBuffer toPad, int numBlanks) {
    for (int i = 0; i < numBlanks; i++) {
      toPad.append(' ');
    }
  }

  public static String prettyPrintDocStructure(String toFormat) {
    StringBuffer result = new StringBuffer();
    int indent = 0;
    String source = toFormat.replaceAll("[ ]*,", ","); // $NON-NLS-1$ //$NON-NLS-2$
    Element next = Element.OPEN_BRACE;

    while (source.length() > 0) {
      source = source.trim();
      String toIndent = ""; // $NON-NLS-1$
      int minIndex = Integer.MAX_VALUE;
      char targetChar = '{';
      if (source.indexOf('{') > -1 && source.indexOf('{') < minIndex) {
        next = Element.OPEN_BRACE;
        minIndex = source.indexOf('{');
        targetChar = '{';
      }
      if (source.indexOf('}') > -1 && source.indexOf('}') < minIndex) {
        next = Element.CLOSE_BRACE;
        minIndex = source.indexOf('}');
        targetChar = '}';
      }
      if (source.indexOf('[') > -1 && source.indexOf('[') < minIndex) {
        next = Element.OPEN_BRACKET;
        minIndex = source.indexOf('[');
        targetChar = '[';
      }
      if (source.indexOf(']') > -1 && source.indexOf(']') < minIndex) {
        next = Element.CLOSE_BRACKET;
        minIndex = source.indexOf(']');
        targetChar = ']';
      }
      if (source.indexOf(',') > -1 && source.indexOf(',') < minIndex) {
        next = Element.COMMA;
        minIndex = source.indexOf(',');
        targetChar = ',';
      }

      if (minIndex == 0) {
        if (next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET) {
          indent -= 2;
        }
        pad(result, indent);
        String comma = ""; // $NON-NLS-1$
        int offset = 1;
        if (source.length() >= 2 && source.charAt(1) == ',') {
          comma = ","; // $NON-NLS-1$
          offset = 2;
        }
        result.append(targetChar).append(comma).append("\n"); // $NON-NLS-1$
        source = source.substring(offset, source.length());
      } else {
        pad(result, indent);
        if (next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET) {
          toIndent = source.substring(0, minIndex);
          source = source.substring(minIndex, source.length());
        } else {
          toIndent = source.substring(0, minIndex + 1);
          source = source.substring(minIndex + 1, source.length());
        }
        result.append(toIndent.trim()).append("\n"); // $NON-NLS-1$
      }

      if (next == Element.OPEN_BRACE || next == Element.OPEN_BRACKET) {
        indent += 2;
      }
    }

    return result.toString();
  }

  private void showNoFieldMessageDialog() {
    ShowMessageDialog smd =
        new ShowMessageDialog(
            shell,
            SWT.ICON_WARNING | SWT.OK | SWT.CENTER,
            BaseMessages.getString(
                PKG, "MongoDbDeleteDialog.ErrorMessage.NoFieldPathsDefined.Title"),
            BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.NoFieldPathsDefined"));
    smd.open();
  }

  private void showNoQueryWarningDialog() {
    ShowMessageDialog smd =
        new ShowMessageDialog(
            shell,
            SWT.ICON_WARNING | SWT.OK | SWT.CENTER,
            BaseMessages.getString(
                PKG, "MongoDbDeleteDialog.ErrorMessage.NoJsonQueryDefined.Title"),
            BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.NoJsonQueryDefined"));
    smd.open();
  }

  private void setQueryFieldVisiblity(boolean visible) {
    wtvMongoFieldsView.setVisible(visible);
    wbGetFields.setVisible(visible);
    wbPreviewDocStruct.setVisible(visible);
  }

  private void setQueryJsonVisibility(boolean visible) {
    wstJsonQueryView.setVisible(visible);
    wlExecuteForEachRow.setVisible(visible);
    wcbEcuteForEachRow.setVisible(visible);
  }
}
