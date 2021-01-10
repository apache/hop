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
import org.apache.hop.mongo.NamedReadPreference;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
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

import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Dialog class for the MongoDB output transform */
public class MongoDbOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  protected MongoDbOutputMeta currentMeta;
  protected MongoDbOutputMeta originalMeta;

  private TextVar wHostnameField;
  private TextVar wPortField;
  private Button wbUseAllReplicaSetMembers;
  private TextVar wAuthDbName;
  private TextVar wUsernameField;
  private TextVar wPassField;
  private CCombo wDbAuthMec;

  private Button wbKerberos;

  private TextVar wConnectTimeout;
  private TextVar wSocketTimeout;

  private Button wbUseSSLSocketFactory;

  private CCombo wDbNameField;
  private CCombo wCollectionField;

  private TextVar wBatchInsertSizeField;

  private Button wbTruncate;
  private Button wbUpdate;
  private Button wbUpsert;
  private Button wbMulti;
  private Button wbModifierUpdate;

  private CCombo wWriteConcern;
  private TextVar wTimeout;
  private Button wbJournalWrites;
  private CCombo wReadPreference;

  private TextVar wWriteRetries;
  private TextVar wWriteRetryDelay;

  private TableView wMongoFields;
  private TableView wMongoIndexes;

  public MongoDbOutputDialog(Shell parent, IVariables variables, Object in, PipelineMeta tr, String name) {

    super(parent, variables, (BaseTransformMeta) in, tr, name );

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

    // used to listen to a text field (m_wTransformName)
    ModifyListener lsMod =
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            currentMeta.setChanged();
          }
        };

    changed = currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(getString("MongoDbOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons inherited from BaseTransformDialog
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(getString("System.Button.OK")); //
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(getString("System.Button.Cancel")); //
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(getString("MongoDbOutputDialog.TransformName.Label"));
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
    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB);

    // Start of the config tab
    CTabItem wConfigTab = new CTabItem( wTabFolder, SWT.NONE );
    wConfigTab.setText(getString("MongoDbOutputDialog.ConfigTab.TabTitle"));

    Composite wConfigComp = new Composite( wTabFolder, SWT.NONE);
    props.setLook(wConfigComp);

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout(configLayout);

    // hostname line
    Label hostnameLab = new Label(wConfigComp, SWT.RIGHT);
    hostnameLab.setText(getString("MongoDbOutputDialog.Hostname.Label"));
    hostnameLab.setToolTipText(getString("MongoDbOutputDialog.Hostname.TipText"));
    props.setLook(hostnameLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(middle, -margin);
    hostnameLab.setLayoutData(fd);

    wHostnameField = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wHostnameField);
    wHostnameField.addModifyListener(lsMod);
    // set the tool tip to the contents with any env variables expanded
    wHostnameField.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            wHostnameField.setToolTipText(
                variables.resolve(wHostnameField.getText()));
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(middle, 0);
    wHostnameField.setLayoutData(fd);

    // port line
    Label portLab = new Label(wConfigComp, SWT.RIGHT);
    portLab.setText(getString("MongoDbOutputDialog.Port.Label"));
    portLab.setToolTipText(getString("MongoDbOutputDialog.Port.Label.TipText"));
    props.setLook(portLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wHostnameField, margin);
    fd.right = new FormAttachment(middle, -margin);
    portLab.setLayoutData(fd);

    wPortField = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPortField);
    wPortField.addModifyListener(lsMod);
    // set the tool tip to the contents with any env variables expanded
    wPortField.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            wPortField.setToolTipText(variables.resolve(wPortField.getText()));
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wHostnameField, margin);
    fd.left = new FormAttachment(middle, 0);
    wPortField.setLayoutData(fd);

    // enable ssl connection
    Label useSSLSocketFactoryL = new Label(wConfigComp, SWT.RIGHT);
    useSSLSocketFactoryL.setText(
        BaseMessages.getString(PKG, "MongoDbOutputDialog.UseSSLSocketFactory.Label"));
    props.setLook(useSSLSocketFactoryL);
    useSSLSocketFactoryL.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(wPortField, 2 * margin)
            .right(middle, -margin)
            .result());

    wbUseSSLSocketFactory = new Button(wConfigComp, SWT.CHECK);
    props.setLook(wbUseSSLSocketFactory);
    wbUseSSLSocketFactory.addListener(SWT.Selection, e -> currentMeta.setChanged());
    wbUseSSLSocketFactory.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(useSSLSocketFactoryL, 0, SWT.CENTER)
            .right(100, 0)
            .result());

    // Use all replica set members check box
    Label useAllReplicaLab = new Label(wConfigComp, SWT.RIGHT);
    useAllReplicaLab.setText(getString("MongoDbOutputDialog.UseAllReplicaSetMembers.Label"));
    useAllReplicaLab.setToolTipText(
        getString("MongoDbOutputDialog.UseAllReplicaSetMembers.TipText"));
    props.setLook(useAllReplicaLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -2 * margin);
    fd.top = new FormAttachment(useSSLSocketFactoryL, margin);
    useAllReplicaLab.setLayoutData(fd);
    wbUseAllReplicaSetMembers = new Button(wConfigComp, SWT.CHECK);
    props.setLook(wbUseAllReplicaSetMembers);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(useAllReplicaLab, 0, SWT.CENTER);
    wbUseAllReplicaSetMembers.setLayoutData(fd);

    // authentication database field
    Label authBdLab = new Label(wConfigComp, SWT.RIGHT);
    authBdLab.setText(getString("MongoDbOutputDialog.AuthenticationDatabaseName.Label"));
    props.setLook(authBdLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(useAllReplicaLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    authBdLab.setLayoutData(fd);
    wAuthDbName = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAuthDbName);
    wAuthDbName.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(authBdLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wAuthDbName.setLayoutData(fd);

    // username field
    Label userLab = new Label(wConfigComp, SWT.RIGHT);
    userLab.setText(getString("MongoDbOutputDialog.Username.Label"));
    props.setLook(userLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wAuthDbName, margin);
    fd.right = new FormAttachment(middle, -margin);
    userLab.setLayoutData(fd);
    wUsernameField = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUsernameField);
    wUsernameField.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(userLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wUsernameField.setLayoutData(fd);

    // password field
    Label passLab = new Label(wConfigComp, SWT.RIGHT);
    passLab.setText(getString("MongoDbOutputDialog.Password.Label"));
    props.setLook(passLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wUsernameField, margin);
    fd.right = new FormAttachment(middle, -margin);
    passLab.setLayoutData(fd);
    wPassField = new PasswordTextVar(variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPassField);
    wPassField.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(passLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wPassField.setLayoutData(fd);
    Control lastControl = wPassField;

    // Authentication Mechanisms
    Label wlAuthMec = new Label(wConfigComp, SWT.RIGHT);
    wlAuthMec.setText(getString("MongoDbOutputDialog.AuthMechanism.Label"));
    props.setLook(wlAuthMec);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlAuthMec.setLayoutData(fd);
    wDbAuthMec = new CCombo(wConfigComp, SWT.BORDER);
    props.setLook(wDbAuthMec);
    wDbAuthMec.addModifyListener(
        e -> {
          pipelineMeta.setChanged();
          wDbAuthMec.setToolTipText(wDbAuthMec.getText());
        });
    wDbAuthMec.add("SCRAM-SHA-1");
    wDbAuthMec.add("MONGODB-CR");
    wDbAuthMec.add("PLAIN");
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wlAuthMec, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wDbAuthMec.setLayoutData(fd);
    lastControl = wlAuthMec;

    // use kerberos authentication
    Label kerbLab = new Label(wConfigComp, SWT.RIGHT);
    kerbLab.setText(getString("MongoDbOutputDialog.Kerberos.Label"));
    props.setLook(kerbLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    kerbLab.setLayoutData(fd);
    wbKerberos = new Button(wConfigComp, SWT.CHECK);
    props.setLook(wbKerberos);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(kerbLab, 0, SWT.CENTER);
    wbKerberos.setLayoutData(fd);
    wbKerberos.addListener(SWT.Selection, e -> wPassField.setEnabled(!wbKerberos.getSelection()));
    lastControl = kerbLab;

    // connection timeout
    Label connectTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    connectTimeoutL.setText(getString("MongoDbOutputDialog.ConnectionTimeout.Label"));
    props.setLook(connectTimeoutL);
    connectTimeoutL.setToolTipText(getString("MongoDbOutputDialog.ConnectionTimeout.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    connectTimeoutL.setLayoutData(fd);
    wConnectTimeout = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConnectTimeout);
    wConnectTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(connectTimeoutL, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wConnectTimeout.setLayoutData(fd);
    wConnectTimeout.addModifyListener(
        e ->
            wConnectTimeout.setToolTipText(
                variables.resolve(wConnectTimeout.getText())));
    lastControl = connectTimeoutL;

    // socket timeout
    Label socketTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    socketTimeoutL.setText(getString("MongoDbOutputDialog.SocketTimeout.Label"));
    props.setLook(socketTimeoutL);
    socketTimeoutL.setToolTipText(getString("MongoDbOutputDialog.SocketTimeout.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    socketTimeoutL.setLayoutData(fd);
    wSocketTimeout = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSocketTimeout);
    wSocketTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(socketTimeoutL, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wSocketTimeout.setLayoutData(fd);
    wSocketTimeout.addModifyListener(
        e ->
            wSocketTimeout.setToolTipText(
                variables.resolve(wSocketTimeout.getText())));
    lastControl = wSocketTimeout;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wConfigComp.setLayoutData(fd);

    wConfigComp.layout();
    wConfigTab.setControl(wConfigComp);

    // --- start of the options tab
    CTabItem wOutputOptionsTab = new CTabItem( wTabFolder, SWT.NONE );
    wOutputOptionsTab.setText("Output options");
    Composite wOutputComp = new Composite( wTabFolder, SWT.NONE);
    props.setLook(wOutputComp);
    FormLayout outputLayout = new FormLayout();
    outputLayout.marginWidth = 3;
    outputLayout.marginHeight = 3;
    wOutputComp.setLayout(outputLayout);

    // DB name
    Label dbNameLab = new Label(wOutputComp, SWT.RIGHT);
    dbNameLab.setText(getString("MongoDbOutputDialog.DBName.Label"));
    dbNameLab.setToolTipText(getString("MongoDbOutputDialog.DBName.TipText"));
    props.setLook(dbNameLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, margin);
    fd.right = new FormAttachment(middle, -margin);
    dbNameLab.setLayoutData(fd);

    Button wbGetDBs = new Button( wOutputComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetDBs );
    wbGetDBs.setText(getString("MongoDbOutputDialog.GetDBs.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    wbGetDBs.setLayoutData(fd);

    wDbNameField = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wDbNameField);

    wDbNameField.addModifyListener(
        e -> {
          currentMeta.setChanged();
          wDbNameField.setToolTipText(variables.resolve(wDbNameField.getText()));
        });

    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wPassField, margin);
    fd.right = new FormAttachment( wbGetDBs, -margin);
    wDbNameField.setLayoutData(fd);
    wbGetDBs.addListener(SWT.Selection, e -> setupDBNames());

    // collection line
    Label collectionLab = new Label(wOutputComp, SWT.RIGHT);
    collectionLab.setText(getString("MongoDbOutputDialog.Collection.Label"));
    collectionLab.setToolTipText(getString("MongoDbOutputDialog.Collection.TipText"));
    props.setLook(collectionLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wDbNameField, margin);
    fd.right = new FormAttachment(middle, -margin);
    collectionLab.setLayoutData(fd);

    Button wbGetCollections = new Button( wOutputComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetCollections );
    wbGetCollections.setText(getString("MongoDbOutputDialog.GetCollections.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wDbNameField, 0);
    wbGetCollections.setLayoutData(fd);

    wbGetCollections.addListener(SWT.Selection, e -> setupCollectionNamesForDB(false));

    wCollectionField = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wCollectionField);
    wCollectionField.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            currentMeta.setChanged();

            wCollectionField.setToolTipText(
                variables.resolve(wCollectionField.getText()));
          }
        });
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wDbNameField, margin);
    fd.right = new FormAttachment( wbGetCollections, -margin);
    wCollectionField.setLayoutData(fd);

    // batch insert line
    Label batchLab = new Label(wOutputComp, SWT.RIGHT);
    batchLab.setText(getString("MongoDbOutputDialog.BatchInsertSize.Label"));
    props.setLook(batchLab);
    batchLab.setToolTipText(getString("MongoDbOutputDialog.BatchInsertSize.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wCollectionField, margin);
    fd.right = new FormAttachment(middle, -margin);
    batchLab.setLayoutData(fd);

    wBatchInsertSizeField =
        new TextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchInsertSizeField);
    wBatchInsertSizeField.addModifyListener(lsMod);
    // set the tool tip to the contents with any env variables expanded
    wBatchInsertSizeField.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            wBatchInsertSizeField.setToolTipText(
                variables.resolve(wBatchInsertSizeField.getText()));
          }
        });
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wCollectionField, margin);
    fd.left = new FormAttachment(middle, 0);
    wBatchInsertSizeField.setLayoutData(fd);

    // truncate line
    Label truncateLab = new Label(wOutputComp, SWT.RIGHT);
    truncateLab.setText(getString("MongoDbOutputDialog.Truncate.Label"));
    props.setLook(truncateLab);
    truncateLab.setToolTipText(getString("MongoDbOutputDialog.Truncate.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wBatchInsertSizeField, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    truncateLab.setLayoutData(fd);
    wbTruncate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbTruncate);
    wbTruncate.setToolTipText(getString("MongoDbOutputDialog.Truncate.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(truncateLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbTruncate.setLayoutData(fd);
    wbTruncate.addListener(SWT.Selection, e -> currentMeta.setChanged());

    // update line
    Label updateLab = new Label(wOutputComp, SWT.RIGHT);
    updateLab.setText(getString("MongoDbOutputDialog.Update.Label"));
    props.setLook(updateLab);
    updateLab.setToolTipText(getString("MongoDbOutputDialog.Update.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(truncateLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    updateLab.setLayoutData(fd);
    wbUpdate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbUpdate);
    wbUpdate.setToolTipText(getString("MongoDbOutputDialog.Update.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(updateLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbUpdate.setLayoutData(fd);

    // multi update can only be used when the update document
    // contains modifier operations:
    // http://docs.mongodb.org/manual/reference/method/db.collection.update/#multi-parameter
    wbUpdate.addListener(
        SWT.Selection,
        e -> {
          currentMeta.setChanged();
          wbUpsert.setEnabled(wbUpdate.getSelection());
          wbModifierUpdate.setEnabled(wbUpdate.getSelection());
          wReadPreference.setEnabled(
              wbModifierUpdate.getEnabled() && wbModifierUpdate.getSelection());
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
    upsertLab.setText(getString("MongoDbOutputDialog.Upsert.Label"));
    props.setLook(upsertLab);
    upsertLab.setToolTipText(getString("MongoDbOutputDialog.Upsert.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(updateLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    upsertLab.setLayoutData(fd);
    wbUpsert = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbUpsert);
    wbUpsert.setToolTipText(getString("MongoDbOutputDialog.Upsert.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(upsertLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbUpsert.setLayoutData(fd);

    // multi line
    Label multiLab = new Label(wOutputComp, SWT.RIGHT);
    multiLab.setText(getString("MongoDbOutputDialog.Multi.Label"));
    props.setLook(multiLab);
    multiLab.setToolTipText(getString("MongoDbOutputDialog.Multi.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(upsertLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    multiLab.setLayoutData(fd);
    wbMulti = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbMulti);
    wbMulti.setToolTipText(getString("MongoDbOutputDialog.Multi.TipText"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(multiLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbMulti.setLayoutData(fd);
    wbMulti.addListener(SWT.Selection, e -> currentMeta.setChanged());

    // modifier update
    Label modifierLab = new Label(wOutputComp, SWT.RIGHT);
    modifierLab.setText(getString("MongoDbOutputDialog.Modifier.Label"));
    props.setLook(modifierLab);
    modifierLab.setToolTipText(getString("MongoDbOutputDialog.Modifier.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(multiLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    modifierLab.setLayoutData(fd);

    wbModifierUpdate = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbModifierUpdate);
    wbModifierUpdate.setToolTipText(getString("MongoDbOutputDialog.Modifier.TipText"));
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
          wReadPreference.setEnabled(
              wbModifierUpdate.getEnabled() && wbModifierUpdate.getSelection());
        });

    // write concern
    Label writeConcernLab = new Label(wOutputComp, SWT.RIGHT);
    writeConcernLab.setText(getString("MongoDbOutputDialog.WriteConcern.Label"));
    writeConcernLab.setToolTipText(getString("MongoDbOutputDialog.WriteConcern.TipText"));
    props.setLook(writeConcernLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wbModifierUpdate, margin);
    fd.right = new FormAttachment(middle, -margin);
    writeConcernLab.setLayoutData(fd);

    Button getCustomWCBut = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
    props.setLook(getCustomWCBut);
    getCustomWCBut.setText(getString("MongoDbOutputDialog.WriteConcern.CustomWriteConcerns"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wbModifierUpdate, 0);
    getCustomWCBut.setLayoutData(fd);

    getCustomWCBut.addListener(SWT.Selection, e -> setupCustomWriteConcernNames());

    wWriteConcern = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wWriteConcern);
    wWriteConcern.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(getCustomWCBut, 0);
    fd.top = new FormAttachment(wbModifierUpdate, margin);
    fd.left = new FormAttachment(middle, 0);
    wWriteConcern.setLayoutData(fd);

    // wTimeout
    Label wTimeoutLab = new Label(wOutputComp, SWT.RIGHT);
    wTimeoutLab.setText(getString("MongoDbOutputDialog.WTimeout.Label"));
    wTimeoutLab.setToolTipText(getString("MongoDbOutputDialog.WTimeout.TipText"));
    props.setLook(wTimeoutLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wWriteConcern, margin);
    fd.right = new FormAttachment(middle, -margin);
    wTimeoutLab.setLayoutData(fd);

    wTimeout = new TextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTimeout);
    wTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(wWriteConcern, margin);
    fd.left = new FormAttachment(middle, 0);
    wTimeout.setLayoutData(fd);
    wTimeout.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            wTimeout.setToolTipText(variables.resolve(wTimeout.getText()));
          }
        });

    Label journalWritesLab = new Label(wOutputComp, SWT.RIGHT);
    journalWritesLab.setText(getString("MongoDbOutputDialog.JournalWrites.Label"));
    journalWritesLab.setToolTipText(getString("MongoDbOutputDialog.JournalWrites.TipText"));
    props.setLook(journalWritesLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTimeout, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    journalWritesLab.setLayoutData(fd);
    wbJournalWrites = new Button(wOutputComp, SWT.CHECK);
    props.setLook(wbJournalWrites);
    wbJournalWrites.addListener(SWT.Selection, e -> currentMeta.setChanged());
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(journalWritesLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    wbJournalWrites.setLayoutData(fd);

    // read preference
    Label readPrefL = new Label(wOutputComp, SWT.RIGHT);
    readPrefL.setText(getString("MongoDbOutputDialog.ReadPreferenceLabel"));
    readPrefL.setToolTipText(getString("MongoDbOutputDialog.ReadPreferenceLabel.TipText"));
    props.setLook(readPrefL);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(journalWritesLab, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    readPrefL.setLayoutData(fd);
    wReadPreference = new CCombo(wOutputComp, SWT.BORDER);
    props.setLook(wReadPreference);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(readPrefL, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wReadPreference.setLayoutData(fd);
    wReadPreference.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            currentMeta.setChanged();
            wReadPreference.setToolTipText(
                variables.resolve(wReadPreference.getText()));
          }
        });
    wReadPreference.add(NamedReadPreference.PRIMARY.getName());
    wReadPreference.add(NamedReadPreference.PRIMARY_PREFERRED.getName());
    wReadPreference.add(NamedReadPreference.SECONDARY.getName());
    wReadPreference.add(NamedReadPreference.SECONDARY_PREFERRED.getName());
    wReadPreference.add(NamedReadPreference.NEAREST.getName());

    // retries stuff
    Label retriesLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesLab);
    retriesLab.setText(getString("MongoDbOutputDialog.WriteRetries.Label"));
    retriesLab.setToolTipText(getString("MongoDbOutputDialog.WriteRetries.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wReadPreference, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesLab.setLayoutData(fd);

    wWriteRetries = new TextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWriteRetries);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wReadPreference, margin);
    fd.right = new FormAttachment(100, 0);
    wWriteRetries.setLayoutData(fd);
    wWriteRetries.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            wWriteRetries.setToolTipText(
                variables.resolve(wWriteRetries.getText()));
          }
        });

    Label retriesDelayLab = new Label(wOutputComp, SWT.RIGHT);
    props.setLook(retriesDelayLab);
    retriesDelayLab.setText(getString("MongoDbOutputDialog.WriteRetriesDelay.Label"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(wWriteRetries, margin);
    fd.right = new FormAttachment(middle, -margin);
    retriesDelayLab.setLayoutData(fd);

    wWriteRetryDelay = new TextVar( variables, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
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
    CTabItem wMongoFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wMongoFieldsTab.setText(getString("MongoDbOutputDialog.FieldsTab.TabTitle"));
    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = 3;
    filterLayout.marginHeight = 3;
    wFieldsComp.setLayout(filterLayout);

    final ColumnInfo[] colInf =
        new ColumnInfo[] {
          new ColumnInfo(
              getString("MongoDbOutputDialog.Fields.Incoming"), ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              getString("MongoDbOutputDialog.Fields.Path"), ColumnInfo.COLUMN_TYPE_TEXT, false),
          createReadOnlyComboBox("MongoDbOutputDialog.Fields.UseIncomingName", "Y", "N"),
          createReadOnlyComboBox(
              "MongoDbOutputDialog.Fields.NullValues",
              getString("MongoDbOutputDialog.Fields.NullValues.Insert"),
              getString("MongoDbOutputDialog.Fields.NullValues.Ignore")),
          createReadOnlyComboBox("MongoDbOutputDialog.Fields.JSON", "Y", "N"),
          createReadOnlyComboBox("MongoDbOutputDialog.Fields.UpdateMatchField", "Y", "N"),
          createReadOnlyComboBox(
              "MongoDbOutputDialog.Fields.ModifierUpdateOperation", "N/A", "$set", "$inc", "$push"),
          createReadOnlyComboBox(
              "MongoDbOutputDialog.Fields.ModifierApplyPolicy", "Insert&Update", "Insert", "Update")
        };

    // get fields but
    Button wbGetFields = new Button( wFieldsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGetFields );
    wbGetFields.setText(getString("MongoDbOutputDialog.GetFieldsBut"));
    fd = new FormData();
    // fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, margin);
    wbGetFields.setLayoutData(fd);
    wbGetFields.addListener(SWT.Selection, e -> getFields());

    Button wbPreviewDocStruct = new Button( wFieldsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbPreviewDocStruct );
    wbPreviewDocStruct.setText(getString("MongoDbOutputDialog.PreviewDocStructBut"));
    fd = new FormData();
    // fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment( wbGetFields, margin);
    wbPreviewDocStruct.setLayoutData(fd);
    wbPreviewDocStruct.addListener(SWT.Selection, e -> previewDocStruct());

    wMongoFields =
        new TableView(
            variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colInf, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(0, margin * 2);
    fd.bottom = new FormAttachment( wbGetFields, -margin * 2);
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
    CTabItem wMongoIndexesTab = new CTabItem( wTabFolder, SWT.NONE );
    wMongoIndexesTab.setText(getString("MongoDbOutputDialog.IndexesTab.TabTitle"));
    Composite wIndexesComp = new Composite( wTabFolder, SWT.NONE);
    props.setLook(wIndexesComp);
    FormLayout indexesLayout = new FormLayout();
    indexesLayout.marginWidth = 3;
    indexesLayout.marginHeight = 3;
    wIndexesComp.setLayout(indexesLayout);
    final ColumnInfo[] colInf2 =
        new ColumnInfo[] {
          new ColumnInfo(
              getString("MongoDbOutputDialog.Indexes.IndexFields"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              getString("MongoDbOutputDialog.Indexes.IndexOpp"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              getString("MongoDbOutputDialog.Indexes.Unique"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              getString("MongoDbOutputDialog.Indexes.Sparse"),
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
    Button wbShowIndexes = new Button( wIndexesComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowIndexes );
    wbShowIndexes.setText(getString("MongoDbOutputDialog.ShowIndexesBut")); //
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
    fd.bottom = new FormAttachment( wbShowIndexes, -margin * 2);
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

    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);

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

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return transformName;
  }

  private ColumnInfo createReadOnlyComboBox(String i18nKey, String... values) {
    ColumnInfo info = new ColumnInfo(getString(i18nKey), ColumnInfo.COLUMN_TYPE_CCOMBO, false);
    info.setReadOnly(true);
    info.setComboValues(values);
    return info;
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
              getString("MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined.Title"),
              getString("MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined")); //
      smd.open();
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  private void getInfo(MongoDbOutputMeta meta) {
    meta.setHostnames(wHostnameField.getText());
    meta.setPort(wPortField.getText());
    meta.setUseAllReplicaSetMembers(wbUseAllReplicaSetMembers.getSelection());
    meta.setAuthenticationDatabaseName(wAuthDbName.getText());
    meta.setAuthenticationUser(wUsernameField.getText());
    meta.setAuthenticationPassword(wPassField.getText());
    meta.setAuthenticationMechanism(wDbAuthMec.getText());
    meta.setUseKerberosAuthentication(wbKerberos.getSelection());
    meta.setDbName(wDbNameField.getText());
    meta.setCollection(wCollectionField.getText());
    meta.setBatchInsertSize(wBatchInsertSizeField.getText());
    meta.setUpdate(wbUpdate.getSelection());
    meta.setUpsert(wbUpsert.getSelection());
    meta.setMulti(wbMulti.getSelection());
    meta.setTruncate(wbTruncate.getSelection());
    meta.setModifierUpdate(wbModifierUpdate.getSelection());
    meta.setConnectTimeout(wConnectTimeout.getText());
    meta.setSocketTimeout(wSocketTimeout.getText());
    meta.setUseSSLSocketFactory(wbUseSSLSocketFactory.getSelection());
    meta.setWriteConcern(wWriteConcern.getText());
    meta.setWTimeout(wTimeout.getText());
    meta.setJournal(wbJournalWrites.getSelection());
    meta.setReadPreference(wReadPreference.getText());
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
        newIndex.m_pathToFields = indexFieldList;
        newIndex.m_drop = indexOpp.equals("Drop"); //
        newIndex.m_unique = unique.equals("Y"); //
        newIndex.m_sparse = sparse.equals("Y"); //

        mongoIndexes.add(newIndex);
      }
    }
    meta.setMongoIndexes(mongoIndexes);
  }

  private List<MongoDbOutputMeta.MongoField> tableToMongoFieldList() {
    int numNonEmpty = wMongoFields.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoDbOutputMeta.MongoField> mongoFields =
        new ArrayList<>( numNonEmpty );

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
        newField.m_incomingFieldName = incoming;
        newField.m_mongoDocPath = path;
        newField.m_useIncomingFieldNameAsMongoFieldName =
            ((useIncoming.length() > 0) ? useIncoming.equals("Y") : true); //
        newField.insertNull =
            getString("MongoDbOutputDialog.Fields.NullValues.Insert").equals(allowNull);
        newField.m_JSON = ((json.length() > 0) ? json.equals("Y") : false); //
        newField.m_updateMatchField = (updateMatch.equals("Y")); //
        if (modifierOp.length() == 0) {
          newField.m_modifierUpdateOperation = "N/A"; //
        } else {
          newField.m_modifierUpdateOperation = modifierOp;
        }
        newField.m_modifierOperationApplyPolicy = modifierPolicy;
        mongoFields.add(newField);
      }

      return mongoFields;
    }

    return null;
  }

  private void getData() {
    wHostnameField.setText(Const.NVL(currentMeta.getHostnames(), "")); //
    wPortField.setText(Const.NVL(currentMeta.getPort(), "")); //
    wbUseAllReplicaSetMembers.setSelection(currentMeta.getUseAllReplicaSetMembers());
    wAuthDbName.setText(Const.NVL(currentMeta.getAuthenticationDatabaseName(), "")); //
    wUsernameField.setText(Const.NVL(currentMeta.getAuthenticationUser(), "")); //
    wPassField.setText(Const.NVL(currentMeta.getAuthenticationPassword(), "")); //
    wDbAuthMec.setText(Const.NVL(currentMeta.getAuthenticationMechanism(), ""));
    wbKerberos.setSelection(currentMeta.getUseKerberosAuthentication());
    wPassField.setEnabled(!wbKerberos.getSelection());
    wDbNameField.setText(Const.NVL(currentMeta.getDbName(), "")); //
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

    wReadPreference.setEnabled(wbModifierUpdate.getEnabled() && wbModifierUpdate.getSelection());
    wConnectTimeout.setText(Const.NVL(currentMeta.getConnectTimeout(), "")); //
    wSocketTimeout.setText(Const.NVL(currentMeta.getSocketTimeout(), "")); //
    wbUseSSLSocketFactory.setSelection(currentMeta.isUseSSLSocketFactory());
    wWriteConcern.setText(Const.NVL(currentMeta.getWriteConcern(), "")); //
    wTimeout.setText(Const.NVL(currentMeta.getWTimeout(), "")); //
    wbJournalWrites.setSelection(currentMeta.getJournal());
    wReadPreference.setText(Const.NVL(currentMeta.getReadPreference(), "")); //
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

        item.setText(1, Const.NVL(field.m_incomingFieldName, "")); //
        item.setText(2, Const.NVL(field.m_mongoDocPath, "")); //
        item.setText(3, field.m_useIncomingFieldNameAsMongoFieldName ? "Y" : "N"); //
        String insertNullKey =
            field.insertNull
                ? "MongoDbOutputDialog.Fields.NullValues.Insert"
                : "MongoDbOutputDialog.Fields.NullValues.Ignore";
        item.setText(4, getString(insertNullKey));
        item.setText(5, field.m_JSON ? "Y" : "N"); //
        item.setText(6, field.m_updateMatchField ? "Y" : "N"); //
        item.setText(7, Const.NVL(field.m_modifierUpdateOperation, "")); //
        item.setText(8, Const.NVL(field.m_modifierOperationApplyPolicy, "")); //
      }

      wMongoFields.removeEmptyRows();
      wMongoFields.setRowNums();
      wMongoFields.optWidth(true);
    }

    List<MongoDbOutputMeta.MongoIndex> mongoIndexes = currentMeta.getMongoIndexes();

    if (mongoIndexes != null && mongoIndexes.size() > 0) {
      for (MongoDbOutputMeta.MongoIndex index : mongoIndexes) {
        TableItem item = new TableItem(wMongoIndexes.table, SWT.None);

        item.setText(1, Const.NVL(index.m_pathToFields, "")); //
        if (index.m_drop) {
          item.setText(2, "Drop"); //
        } else {
          item.setText(2, "Create"); //
        }

        item.setText(3, Const.NVL(index.m_unique ? "Y" : "N", "N")); //   //
        item.setText(4, Const.NVL(index.m_sparse ? "Y" : "N", "N")); //   //
      }

      wMongoIndexes.removeEmptyRows();
      wMongoIndexes.setRowNums();
      wMongoIndexes.optWidth(true);
    }
  }

  private void setupCollectionNamesForDB(boolean quiet) {
    final String hostname = variables.resolve(wHostnameField.getText());
    final String dB = variables.resolve(wDbNameField.getText());

    String current = wCollectionField.getText();
    wCollectionField.removeAll();

    if (!StringUtils.isEmpty(hostname) && !StringUtils.isEmpty(dB)) {

      final MongoDbOutputMeta meta = new MongoDbOutputMeta();
      getInfo(meta);
      try {
        MongoClientWrapper clientWrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        Set<String> collections = new HashSet<>();
        try {
          collections = clientWrapper.getCollectionsNames(dB);
        } finally {
          clientWrapper.dispose();
        }

        for (String c : collections) {
          wCollectionField.add(c);
        }
      } catch (Exception e) {
        // Unwrap the PrivilegedActionException if it was thrown
        if (e instanceof PrivilegedActionException) {
          e = ((PrivilegedActionException) e).getException();
        }
        logError(getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"), e); //
        new ErrorDialog(
            shell,
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            //
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            e); //
      }
    } else {
      // popup some feedback

      String missingConnDetails = ""; //
      if (StringUtils.isEmpty(hostname)) {
        missingConnDetails += "host name(s)"; //
      }
      if (StringUtils.isEmpty(dB)) {
        missingConnDetails += " database"; //
      }
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              getString("MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title"),
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

  private void setupCustomWriteConcernNames() {
    String hostname = variables.resolve(wHostnameField.getText());

    if (!StringUtils.isEmpty(hostname)) {
      MongoDbOutputMeta meta = new MongoDbOutputMeta();
      getInfo(meta);
      try {
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        List<String> custom = new ArrayList<>();
        try {
          custom = wrapper.getLastErrorModes();
        } finally {
          wrapper.dispose();
        }

        if (custom.size() > 0) {
          String current = wWriteConcern.getText();
          wWriteConcern.removeAll();

          for (String s : custom) {
            wWriteConcern.add(s);
          }

          if (!StringUtils.isEmpty(current)) {
            wWriteConcern.setText(current);
          }
        }
      } catch (Exception e) {
        logError(getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"), e); //
        new ErrorDialog(
            shell,
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            //
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            e); //
      }
    } else {
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              getString("MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG, //
                  "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails",
                  "host name(s)")); //
      smd.open();
    }
  }

  private void setupDBNames() {
    String current = wDbNameField.getText();
    wDbNameField.removeAll();

    String hostname = variables.resolve(wHostnameField.getText());

    if (!StringUtils.isEmpty(hostname)) {
      try {
        final MongoDbOutputMeta meta = new MongoDbOutputMeta();
        getInfo(meta);
        List<String> dbNames = new ArrayList<>();
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }
        for (String s : dbNames) {
          wDbNameField.add(s);
        }

      } catch (Exception e) {
        logError(getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"), e); //
        new ErrorDialog(
            shell,
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            //
            getString("MongoDbOutputDialog.ErrorMessage.UnableToConnect"),
            e); //
      }
    } else {
      // popup some feedback
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              getString("MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG, //
                  "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails",
                  "host name(s)")); //
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wDbNameField.setText(current);
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
          getString("System.Dialog.GetFieldsFailed.Message"), //
          e);
      new ErrorDialog(
          shell,
          getString("System.Dialog.GetFieldsFailed.Title"),
          getString("System.Dialog.GetFieldsFailed.Message"),
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
              if (field.m_JSON) {
                if (!field.m_useIncomingFieldNameAsMongoFieldName
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
                  field.m_JSON = false;
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
        m.m_modifierOperationApplyPolicy = "Insert&Update"; //
      }

      String toDisplay = ""; //
      String windowTitle =
          getString("MongoDbOutputDialog.PreviewDocStructure.Title"); //

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
            getString("MongoDbOutputDialog.PreviewModifierUpdate.Heading1") //
                + ":\n\n" //
                + prettyPrintDocStructure(query.toString())
                + getString("MongoDbOutputDialog.PreviewModifierUpdate.Heading2") //
                + ":\n\n" //
                + prettyPrintDocStructure(modifier.toString());
        windowTitle = getString("MongoDbOutputDialog.PreviewModifierUpdate.Title"); //
      }

      ShowMessageDialog smd =
          new ShowMessageDialog(shell, SWT.ICON_INFORMATION | SWT.OK, windowTitle, toDisplay, true);
      smd.open();
    } catch (Exception ex) {
      logError(
          getString("MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              //
              + ":\n\n"
              + ex.getMessage(),
          ex); //
      new ErrorDialog(
          shell,
          getString("MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Title"),
          //
          getString("MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
              //
              + ":\n\n"
              + ex.getMessage(),
          ex); //
      return;
    }
  }

  private void showIndexInfo() {
    String hostname = variables.resolve(wHostnameField.getText());
    String dbName = variables.resolve(wDbNameField.getText());
    String collection = variables.resolve(wCollectionField.getText());

    if (!StringUtils.isEmpty(hostname)) {
      MongoClient conn = null;
      try {
        MongoDbOutputMeta meta = new MongoDbOutputMeta();
        getInfo(meta);
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        StringBuffer result = new StringBuffer();
        for (String index : wrapper.getIndexInfo(dbName, collection)) {
          result.append(index).append("\n\n"); //
        }

        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_INFORMATION | SWT.OK,
                BaseMessages.getString(PKG, "MongoDbOutputDialog.IndexInfo", collection),
                result.toString(),
                true); //
        smd.open();
      } catch (Exception e) {
        logError(
            getString("MongoDbOutputDialog.ErrorMessage.GeneralError.Message") //
                + ":\n\n"
                + e.getMessage(),
            e); //
        new ErrorDialog(
            shell,
            getString("MongoDbOutputDialog.ErrorMessage.IndexPreview.Title"),
            //
            getString("MongoDbOutputDialog.ErrorMessage.GeneralError.Message") //
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

  private String getString(String key) {
    return BaseMessages.getString(PKG, key);
  }
}
