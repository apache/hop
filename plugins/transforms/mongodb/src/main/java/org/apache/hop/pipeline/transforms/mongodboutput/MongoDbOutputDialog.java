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

package org.apache.hop.pipeline.transforms.mongodboutput;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Dialog class for the MongoDB output transform */
public class MongoDbOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  protected MongoDbOutputMeta currentMeta;
  protected MongoDbOutputMeta originalMeta;

  private MetaSelectionLine<MongoDbConnection> wConnection;
  private CCombo wCollectionField;

  private TextVar wBatchInsertSizeField;

  private Button wbTruncate;
  private Button wbUpdate;
  private Button wbUpsert;
  private Button wbMulti;
  private Button wbModifierUpdate;

  private TextVar wWriteRetries;
  private TextVar wWriteRetryDelay;

  private TableView wMongoFields;
  private TableView wMongoIndexes;

  public MongoDbOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String name) {

    super(parent, variables, (BaseTransformMeta) in, tr, name);

    currentMeta = (MongoDbOutputMeta) in;
    originalMeta = (MongoDbOutputMeta) currentMeta.clone();
  }

  @Override
  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

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
    shell.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons inherited from BaseTransformDialog
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK")); //
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.TransformName.Label"));
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

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fd);

    /** various UI bits and pieces for the dialog */
    // The tabs of the dialog
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // --- start of the options tab
    CTabItem wOutputOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOutputOptionsTab.setText("Output options");
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
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ConnectionName.Label"),
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ConnectionName.Tooltip"));
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, 0);
    wConnection.setLayoutData(fdConnection);
    Control lastControl = wConnection;

    try {
      wConnection.fillItems();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error loading list of MongoDB connection names", e);
    }

    // collection line
    Label collectionLab = new Label(wOutputComp, SWT.RIGHT);
    collectionLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Collection.Label"));
    collectionLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.Collection.TipText"));
    props.setLook(collectionLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    collectionLab.setLayoutData(fd);

    Button wbGetCollections = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetCollections);
    wbGetCollections.setText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.GetCollections.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(collectionLab, 0, SWT.CENTER);
    wbGetCollections.setLayoutData(fd);

    wbGetCollections.addListener(SWT.Selection, e -> getCollectionNames(false));

    wCollectionField = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wCollectionField);
    wCollectionField.addModifyListener(
        e -> {
          currentMeta.setChanged();

          wCollectionField.setToolTipText(variables.resolve(wCollectionField.getText()));
        });
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(collectionLab, 0, SWT.CENTER);
    fd.right = new FormAttachment(wbGetCollections, -margin);
    wCollectionField.setLayoutData(fd);
    lastControl = wbGetCollections;

    // batch insert line
    Label batchLab = new Label(wOutputComp, SWT.RIGHT);
    batchLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.BatchInsertSize.Label"));
    props.setLook(batchLab);
    batchLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.BatchInsertSize.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    batchLab.setLayoutData(fd);

    wBatchInsertSizeField = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchInsertSizeField);
    wBatchInsertSizeField.addModifyListener(lsMod);
    // set the tool tip to the contents with any env variables expanded
    wBatchInsertSizeField.addModifyListener(
        e ->
            wBatchInsertSizeField.setToolTipText(
                variables.resolve(wBatchInsertSizeField.getText())));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(batchLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wBatchInsertSizeField.setLayoutData(fd);
    lastControl = wBatchInsertSizeField;

    // truncate line
    Label truncateLab = new Label(wOutputComp, SWT.RIGHT);
    truncateLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Truncate.Label"));
    props.setLook(truncateLab);
    truncateLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Truncate.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    truncateLab.setLayoutData(fd);
    wbTruncate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbTruncate);
    wbTruncate.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Truncate.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(truncateLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbTruncate.setLayoutData(fd);
    wbTruncate.addListener(SWT.Selection, e -> currentMeta.setChanged());
    lastControl = truncateLab;

    // update line
    Label updateLab = new Label(wOutputComp, SWT.RIGHT);
    updateLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Update.Label"));
    props.setLook(updateLab);
    updateLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Update.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    updateLab.setLayoutData(fd);
    wbUpdate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbUpdate);
    wbUpdate.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Update.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(updateLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbUpdate.setLayoutData(fd);
    lastControl = updateLab;

    // multi update can only be used when the update document
    // contains modifier operations:
    // http://docs.mongodb.org/manual/reference/method/db.collection.update/#multi-parameter
    wbUpdate.addListener(
        SWT.Selection,
        e -> {
          currentMeta.setChanged();
          wbUpsert.setEnabled(wbUpdate.getSelection());
          wbModifierUpdate.setEnabled(wbUpdate.getSelection());
          wbMulti.setEnabled(wbUpdate.getSelection());
          if (!wbUpdate.getSelection()) {
            wbModifierUpdate.setSelection(false);
            wbMulti.setSelection(false);
            wbUpsert.setSelection(false);
          }
          wbMulti.setEnabled(wbModifierUpdate.getSelection());
          if (!wbMulti.getEnabled()) {
            wbMulti.setSelection(false);
          }
        });

    // upsert line
    Label upsertLab = new Label(wOutputComp, SWT.RIGHT);
    upsertLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Upsert.Label"));
    props.setLook(upsertLab);
    upsertLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Upsert.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    upsertLab.setLayoutData(fd);
    wbUpsert = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbUpsert);
    wbUpsert.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Upsert.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(upsertLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbUpsert.setLayoutData(fd);
    lastControl = upsertLab;

    // multi line
    Label multiLab = new Label(wOutputComp, SWT.RIGHT);
    multiLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Multi.Label"));
    props.setLook(multiLab);
    multiLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Multi.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    multiLab.setLayoutData(fd);
    wbMulti = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbMulti);
    wbMulti.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Multi.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(multiLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbMulti.setLayoutData(fd);
    wbMulti.addListener(SWT.Selection, e -> currentMeta.setChanged());
    lastControl = multiLab;

    // modifier update
    Label modifierLab = new Label(wOutputComp, SWT.RIGHT);
    modifierLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Modifier.Label"));
    props.setLook(modifierLab);
    modifierLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbOutputDialog.Modifier.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    modifierLab.setLayoutData(fd);

    wbModifierUpdate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbModifierUpdate);
    wbModifierUpdate.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.Modifier.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(modifierLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbModifierUpdate.setLayoutData(fd);
    wbModifierUpdate.addListener(
        SWT.Selection,
        e -> {
          currentMeta.setChanged();
          wbMulti.setEnabled(wbModifierUpdate.getSelection());
          if (!wbModifierUpdate.getSelection()) {
            wbMulti.setSelection(false);
          }
        });
    lastControl = modifierLab;

    // retries stuff
    Label retriesLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesLab);
    retriesLab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.WriteRetries.Label"));
    retriesLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.WriteRetries.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesLab.setLayoutData(fd);

    wWriteRetries = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWriteRetries);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(retriesLab, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wWriteRetries.setLayoutData(fd);
    wWriteRetries.addModifyListener(
        e -> wWriteRetries.setToolTipText(variables.resolve(wWriteRetries.getText())));

    Label retriesDelayLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesDelayLab);
    retriesDelayLab.setText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.WriteRetriesDelay.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wWriteRetries, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesDelayLab.setLayoutData(fd);

    wWriteRetryDelay = new TextVar(variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWriteRetryDelay);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wWriteRetries, margin);
    fd.right = new FormAttachment(100, 0);
    wWriteRetryDelay.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wOutputComp.setLayoutData(fd);

    wOutputComp.layout();
    wOutputOptionsTab.setControl(wOutputComp);

    // --- start of the fields tab
    CTabItem wMongoFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoFieldsTab.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.FieldsTab.TabTitle"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = 3;
    filterLayout.marginHeight = 3;
    wFieldsComp.setLayout(filterLayout);

    final ColumnInfo[] colInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.Incoming"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.Path"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.UseIncomingName"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"}),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues.Insert"),
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues.Ignore")),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.JSON"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "Y",
              "N"),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.UpdateMatchField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "Y",
              "N"),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.ModifierUpdateOperation"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "N/A",
              "$set",
              "$inc",
              "$push"),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.ModifierApplyPolicy"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              "Insert&Update",
              "Insert",
              "Update")
        };

    // get fields but
    Button wbGetFields = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.GetFieldsBut"));
    fd = new FormData();
    // fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, margin);
    wbGetFields.setLayoutData(fd);
    wbGetFields.addListener(SWT.Selection, e -> getFields());

    Button wbPreviewDocStruct = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbPreviewDocStruct);
    wbPreviewDocStruct.setText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.PreviewDocStructBut"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(wbGetFields, margin);
    wbPreviewDocStruct.setLayoutData(fd);
    wbPreviewDocStruct.addListener(SWT.Selection, e -> previewDocStruct());

    wMongoFields =
        new TableView(
            variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colInf, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(0, margin * 2);
    fd.bottom = new FormAttachment(wbGetFields, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wMongoFields.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fd);

    wFieldsComp.layout();
    wMongoFieldsTab.setControl(wFieldsComp);

    // indexes tab ------------------
    CTabItem wMongoIndexesTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoIndexesTab.setText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.IndexesTab.TabTitle"));
    Composite wIndexesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wIndexesComp);
    FormLayout indexesLayout = new FormLayout();
    indexesLayout.marginWidth = 3;
    indexesLayout.marginHeight = 3;
    wIndexesComp.setLayout(indexesLayout);
    final ColumnInfo[] colInf2 =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Indexes.IndexFields"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Indexes.IndexOpp"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Indexes.Unique"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Indexes.Sparse"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
        };
    colInf2[1].setComboValues(new String[] {"Create", "Drop"});
    colInf2[1].setReadOnly(true);
    colInf2[2].setComboValues(new String[] {"Y", "N"}); //
    colInf2[2].setReadOnly(true);
    colInf2[3].setComboValues(new String[] {"Y", "N"}); //
    colInf2[3].setReadOnly(true);

    // get indexes but
    Button wbShowIndexes = new Button(wIndexesComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbShowIndexes);
    wbShowIndexes.setText(BaseMessages.getString(PKG, "MongoDbOutputDialog.ShowIndexesBut")); //
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, margin);
    wbShowIndexes.setLayoutData(fd);

    wbShowIndexes.addListener(SWT.Selection, e -> showIndexInfo());

    wMongoIndexes =
        new TableView(
            variables, wIndexesComp, SWT.FULL_SELECTION | SWT.MULTI, colInf2, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(0, margin * 2);
    fd.bottom = new FormAttachment(wbShowIndexes, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wMongoIndexes.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wIndexesComp.setLayoutData(fd);

    wIndexesComp.layout();
    wMongoIndexesTab.setControl(wIndexesComp);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fd);

    wTabFolder.setSelection(0);
    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void cancel() {
    transformName = null;
    currentMeta.setChanged(changed);

    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();

    getInfo(currentMeta);

    if (currentMeta.getMongoFields() == null) {
      // popup dialog warning that no paths have been defined
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString(
                  PKG, "MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined.Title"),
              BaseMessages.getString(
                  PKG, "MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined")); //
      smd.open();
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  private void getInfo(MongoDbOutputMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setCollection(wCollectionField.getText());
    meta.setBatchInsertSize(wBatchInsertSizeField.getText());
    meta.setUpdate(wbUpdate.getSelection());
    meta.setUpsert(wbUpsert.getSelection());
    meta.setMulti(wbMulti.getSelection());
    meta.setTruncate(wbTruncate.getSelection());
    meta.setModifierUpdate(wbModifierUpdate.getSelection());
    meta.setWriteRetries(wWriteRetries.getText());
    meta.setWriteRetryDelay(wWriteRetryDelay.getText());

    meta.setMongoFields(tableToMongoFieldList());

    // indexes
    int numNonEmpty = wMongoIndexes.nrNonEmpty();
    List<MongoDbOutputMeta.MongoIndex> mongoIndexes = new ArrayList<>();
    if (numNonEmpty > 0) {
      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wMongoIndexes.getNonEmpty(i);

        String indexFieldList = item.getText(1).trim();
        String indexOpp = item.getText(2).trim();
        String unique = item.getText(3).trim();
        String sparse = item.getText(4).trim();

        MongoDbOutputMeta.MongoIndex newIndex = new MongoDbOutputMeta.MongoIndex();
        newIndex.pathToFields = indexFieldList;
        newIndex.drop = indexOpp.equals("Drop"); //
        newIndex.unique = unique.equals("Y"); //
        newIndex.sparse = sparse.equals("Y"); //

        mongoIndexes.add(newIndex);
      }
    }
    meta.setMongoIndexes(mongoIndexes);
  }

  private List<MongoDbOutputMeta.MongoField> tableToMongoFieldList() {
    int numNonEmpty = wMongoFields.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoDbOutputMeta.MongoField> mongoFields = new ArrayList<>(numNonEmpty);

      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wMongoFields.getNonEmpty(i);
        String incoming = item.getText(1).trim();
        String path = item.getText(2).trim();
        String useIncoming = item.getText(3).trim();
        String allowNull = item.getText(4).trim();
        String json = item.getText(5).trim();
        String updateMatch = item.getText(6).trim();
        String modifierOp = item.getText(7).trim();
        String modifierPolicy = item.getText(8).trim();

        MongoDbOutputMeta.MongoField newField = new MongoDbOutputMeta.MongoField();
        newField.incomingFieldName = incoming;
        newField.mongoDocPath = path;
        newField.useIncomingFieldNameAsMongoFieldName =
            ((useIncoming.length() > 0) ? useIncoming.equals("Y") : true); //
        newField.insertNull =
            BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues.Insert")
                .equals(allowNull);
        newField.inputJson = ((json.length() > 0) ? json.equals("Y") : false); //
        newField.updateMatchField = (updateMatch.equals("Y")); //
        if (modifierOp.length() == 0) {
          newField.modifierUpdateOperation = "N/A"; //
        } else {
          newField.modifierUpdateOperation = modifierOp;
        }
        newField.modifierOperationApplyPolicy = modifierPolicy;
        mongoFields.add(newField);
      }

      return mongoFields;
    }

    return null;
  }

  private void getData() {
    wConnection.setText(Const.NVL(currentMeta.getConnectionName(), "")); //
    wCollectionField.setText(Const.NVL(currentMeta.getCollection(), "")); //
    wBatchInsertSizeField.setText(Const.NVL(currentMeta.getBatchInsertSize(), "")); //
    wbUpdate.setSelection(currentMeta.getUpdate());
    wbUpsert.setSelection(currentMeta.getUpsert());
    wbMulti.setSelection(currentMeta.getMulti());
    wbTruncate.setSelection(currentMeta.getTruncate());
    wbModifierUpdate.setSelection(currentMeta.getModifierUpdate());

    wbUpsert.setEnabled(wbUpdate.getSelection());
    wbModifierUpdate.setEnabled(wbUpdate.getSelection());
    wbMulti.setEnabled(wbUpdate.getSelection());
    if (!wbUpdate.getSelection()) {
      wbModifierUpdate.setSelection(false);
      wbMulti.setSelection(false);
    }
    wbMulti.setEnabled(wbModifierUpdate.getSelection());
    if (!wbMulti.getEnabled()) {
      wbMulti.setSelection(false);
    }

    wWriteRetries.setText(
        Const.NVL(
            currentMeta.getWriteRetries(),
            "" //
                + MongoDbOutputMeta.RETRIES));
    wWriteRetryDelay.setText(
        Const.NVL(
            currentMeta.getWriteRetryDelay(),
            "" //
                + MongoDbOutputMeta.RETRIES));

    List<MongoDbOutputMeta.MongoField> mongoFields = currentMeta.getMongoFields();

    if (mongoFields != null && mongoFields.size() > 0) {
      for (MongoDbOutputMeta.MongoField field : mongoFields) {
        TableItem item = new TableItem(wMongoFields.table, SWT.NONE);

        item.setText(1, Const.NVL(field.incomingFieldName, "")); //
        item.setText(2, Const.NVL(field.mongoDocPath, "")); //
        item.setText(3, field.useIncomingFieldNameAsMongoFieldName ? "Y" : "N"); //
        String insertNullString;
        if (field.insertNull) {
          insertNullString =
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues.Insert");
        } else {
          insertNullString =
              BaseMessages.getString(PKG, "MongoDbOutputDialog.Fields.NullValues.Ignore");
        }
        item.setText(4, insertNullString);
        item.setText(5, field.inputJson ? "Y" : "N"); //
        item.setText(6, field.updateMatchField ? "Y" : "N"); //
        item.setText(7, Const.NVL(field.modifierUpdateOperation, "")); //
        item.setText(8, Const.NVL(field.modifierOperationApplyPolicy, "")); //
      }

      wMongoFields.removeEmptyRows();
      wMongoFields.setRowNums();
      wMongoFields.optWidth(true);
    }

    List<MongoDbOutputMeta.MongoIndex> mongoIndexes = currentMeta.getMongoIndexes();

    if (mongoIndexes != null && mongoIndexes.size() > 0) {
      for (MongoDbOutputMeta.MongoIndex index : mongoIndexes) {
        TableItem item = new TableItem(wMongoIndexes.table, SWT.None);

        item.setText(1, Const.NVL(index.pathToFields, "")); //
        if (index.drop) {
          item.setText(2, "Drop"); //
        } else {
          item.setText(2, "Create"); //
        }

        item.setText(3, Const.NVL(index.unique ? "Y" : "N", "N")); //   //
        item.setText(4, Const.NVL(index.sparse ? "Y" : "N", "N")); //   //
      }

      wMongoIndexes.removeEmptyRows();
      wMongoIndexes.setRowNums();
      wMongoIndexes.optWidth(true);
    }
  }

  private void getCollectionNames(boolean quiet) {
    final MongoDbOutputMeta meta = new MongoDbOutputMeta();
    getInfo(meta);

    final String connectionName = variables.resolve(meta.getConnectionName());

    String current = wCollectionField.getText();
    wCollectionField.removeAll();

    if (!StringUtils.isEmpty(connectionName)) {
      try {
        MongoDbConnection connection =
            metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
        if (connection == null) {
          throw new HopException("Unable to find MongoDB connection " + connectionName);
        }

        String databaseName = variables.resolve(connection.getDbName());

        MongoClientWrapper clientWrapper = connection.createWrapper(variables, log);

        Set<String> collections;
        try {
          collections = clientWrapper.getCollectionsNames(databaseName);
        } finally {
          clientWrapper.dispose();
        }

        for (String c : collections) {
          wCollectionField.add(c);
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.UnableToConnect"), e); //
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            //
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            e); //
      }
    } else {
      // popup some feedback

      String missingConnDetails = ""; //
      if (StringUtils.isEmpty(connectionName)) {
        missingConnDetails += "connection name"; //
      }
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString(
                  PKG, "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG, //
                  "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails",
                  missingConnDetails)); //
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wCollectionField.setText(current);
    }
  }

  private void getFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wMongoFields, 1, new int[] {1}, null, -1, -1, null);
      }
    } catch (HopException e) {
      logError(
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), //
          e);
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          e); //
    }
  }

  private static enum Element {
    OPEN_BRACE,
    CLOSE_BRACE,
    OPEN_BRACKET,
    CLOSE_BRACKET,
    COMMA
  }

  private static void pad(StringBuffer toPad, int numBlanks) {
    for (int i = 0; i < numBlanks; i++) {
      toPad.append(' ');
    }
  }

  /**
   * Format JSON document structure for printing to the preview dialog
   *
   * @param toFormat the document to format
   * @return a String containing the formatted document structure
   */
  public static String prettyPrintDocStructure(String toFormat) {
    StringBuffer result = new StringBuffer();
    int indent = 0;
    String source = toFormat.replaceAll("[ ]*,", ","); //
    Element next = Element.OPEN_BRACE;

    while (source.length() > 0) {
      source = source.trim();
      String toIndent = ""; //
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
        String comma = ""; //
        int offset = 1;
        if (source.length() >= 2 && source.charAt(1) == ',') {
          comma = ","; //
          offset = 2;
        }
        result.append(targetChar).append(comma).append("\n"); //
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
        result.append(toIndent.trim()).append("\n"); //
      }

      if (next == Element.OPEN_BRACE || next == Element.OPEN_BRACKET) {
        indent += 2;
      }
    }

    return result.toString();
  }

  private void previewDocStruct() {
    List<MongoDbOutputMeta.MongoField> mongoFields = tableToMongoFieldList();

    if (mongoFields == null || mongoFields.size() == 0) {
      return;
    }

    // Try and get meta data on incoming fields
    IRowMeta actualR = null;
    IRowMeta r;
    boolean gotGenuineRowMeta = false;
    try {
      actualR = pipelineMeta.getPrevTransformFields(variables, transformName);
      gotGenuineRowMeta = true;
    } catch (HopException e) {
      // don't complain if we can't
    }
    r = new RowMeta();

    Object[] dummyRow = new Object[mongoFields.size()];
    int i = 0;
    try {
      // Initialize Variable variables to allow for environment substitution during doc preview.
      IVariables vs = new Variables();
      vs.initializeFrom(variables);
      boolean hasTopLevelJSONDocInsert =
          MongoDbOutputData.scanForInsertTopLevelJSONDoc(mongoFields);

      for (MongoDbOutputMeta.MongoField field : mongoFields) {
        field.init(vs);
        // set up dummy row meta
        IValueMeta vm = ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING);
        vm.setName(field.environUpdatedFieldName);
        r.addValueMeta(vm);

        String val = ""; //
        if (gotGenuineRowMeta && actualR.indexOfValue(field.environUpdatedFieldName) >= 0) {
          int index = actualR.indexOfValue(field.environUpdatedFieldName);
          switch (actualR.getValueMeta(index).getType()) {
            case IValueMeta.TYPE_STRING:
              if (field.inputJson) {
                if (!field.useIncomingFieldNameAsMongoFieldName
                    && StringUtils.isEmpty(field.environUpdateMongoDocPath)) {
                  // we will actually have to parse some kind of JSON doc
                  // here in the case where the matching doc/doc to be inserted is
                  // a full top-level incoming JSON doc
                  val = "{\"IncomingJSONDoc\" : \"<document content>\"}"; //
                } else {
                  val = "<JSON sub document>"; //
                  // turn this off for the purpose of doc structure
                  // visualization so that we don't screw up for the
                  // lack of a real JSON doc to parse :-)
                  field.inputJson = false;
                }
              } else {
                val = "<string val>"; //
              }
              break;
            case IValueMeta.TYPE_INTEGER:
              val = "<integer val>"; //
              break;
            case IValueMeta.TYPE_NUMBER:
              val = "<number val>"; //
              break;
            case IValueMeta.TYPE_BOOLEAN:
              val = "<bool val>"; //
              break;
            case IValueMeta.TYPE_DATE:
              val = "<date val>"; //
              break;
            case IValueMeta.TYPE_BINARY:
              val = "<binary val>"; //
              break;
            default:
              val = "<unsupported value type>"; //
          }
        } else {
          val = "<value>"; //
        }

        dummyRow[i++] = val;
      }

      MongoDbOutputData.MongoTopLevel topLevelStruct =
          MongoDbOutputData.checkTopLevelConsistency(mongoFields, vs);
      for (MongoDbOutputMeta.MongoField m : mongoFields) {
        m.modifierOperationApplyPolicy = "Insert&Update"; //
      }

      String toDisplay = ""; //
      String windowTitle =
          BaseMessages.getString(PKG, "MongoDbOutputDialog.PreviewDocStructure.Title"); //

      if (!wbModifierUpdate.getSelection()) {
        DBObject result =
            MongoDbOutputData.hopRowToMongo(
                mongoFields, r, dummyRow, topLevelStruct, hasTopLevelJSONDocInsert);
        toDisplay = prettyPrintDocStructure(result.toString());
      } else {
        DBObject query =
            MongoDbOutputData.getQueryObject(mongoFields, r, dummyRow, vs, topLevelStruct);
        DBObject modifier =
            new MongoDbOutputData()
                .getModifierUpdateObject(mongoFields, r, dummyRow, vs, topLevelStruct);
        toDisplay =
            BaseMessages.getString(PKG, "MongoDbOutputDialog.PreviewModifierUpdate.Heading1") //
                + ":\n\n" //
                + prettyPrintDocStructure(query.toString())
                + BaseMessages.getString(
                    PKG, "MongoDbOutputDialog.PreviewModifierUpdate.Heading2") //
                + ":\n\n" //
                + prettyPrintDocStructure(modifier.toString());
        windowTitle =
            BaseMessages.getString(PKG, "MongoDbOutputDialog.PreviewModifierUpdate.Title"); //
      }

      ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_INFORMATION | SWT.OK, windowTitle, toDisplay, true);
      smd.open();
    } catch (Exception ex) {
      logError(
          BaseMessages.getString(
                  PKG, "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              //
              + ":\n\n"
              + ex.getMessage(),
          ex); //
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Title"),
          //
          BaseMessages.getString(
                  PKG, "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              //
              + ":\n\n"
              + ex.getMessage(),
          ex); //
    }
  }

  private void showIndexInfo() {
    String connectionName = variables.resolve(wConnection.getText());

    if (!StringUtils.isEmpty(connectionName)) {
      MongoClient conn = null;
      try {
        MongoDbOutputMeta meta = new MongoDbOutputMeta();
        getInfo(meta);

        MongoDbConnection connection =
            metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
        if (connection == null) {
          throw new HopException("Unable to find MongoDB connection " + connectionName);
        }

        String databaseName = variables.resolve(connection.getDbName());
        String collectionName = variables.resolve(meta.getCollection());

        MongoClientWrapper wrapper = connection.createWrapper(variables, log);
        StringBuffer result = new StringBuffer();
        for (String index : wrapper.getIndexInfo(databaseName, collectionName)) {
          result.append(index).append("\n\n"); //
        }

        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_INFORMATION | SWT.OK,
                BaseMessages.getString(PKG, "MongoDbOutputDialog.IndexInfo", collectionName),
                result.toString(),
                true); //
        smd.open();
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.GeneralError.Message") //
                + ":\n\n"
                + e.getMessage(),
            e); //
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.IndexPreview.Title"),
            //
            BaseMessages.getString(PKG, "MongoDbOutputDialog.ErrorMessage.GeneralError.Message") //
                + ":\n\n"
                + e.getMessage(),
            e); //
      } finally {
        if (conn != null) {
          conn.close();
          conn = null;
        }
      }
    }
  }
}
