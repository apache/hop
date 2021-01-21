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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.NamedReadPreference;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDbInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = MongoDbInputMeta.class; // For i18n - Translator

  private TextVar wHostname;
  private TextVar wPort;
  private Button wbUseAllReplicaSetMembers;
  private CCombo wDbName;
  private TextVar wFieldsName;
  private CCombo wCollection;
  private TextVar wJsonField;

  private StyledTextComp wJsonQuery;
  private Label wlJsonQuery;
  private Button wbQueryIsPipeline;

  private TextVar wAuthDbName;
  private TextVar wAuthUser;
  private TextVar wAuthPass;
  private CCombo wDbAuthMec;

  private Button wbKerberos;

  private Button wbOutputAsJson;
  private TableView wFields;

  private TextVar wConnectionTimeout;
  private TextVar wSocketTimeout;
  private CCombo wReadPreference;

  private TableView wTags;

  private Button wbExecuteForEachRow;
  private Button wbUseSSLSocketFactory;

  private final MongoDbInputMeta input;

  public MongoDbInputDialog(Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (MongoDbInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod =
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.TransformName.Label"));
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
    Control lastControl = wTransformName;

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // start of the config tab
    CTabItem wConfigTab = new CTabItem(wTabFolder, SWT.NONE);
    wConfigTab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.ConfigTab.TabTitle"));

    Composite wConfigComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wConfigComp);

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout(configLayout);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // Hostname(s) input ...
    //
    Label wlHostname = new Label(wConfigComp, SWT.RIGHT);
    wlHostname.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Hostname.Label"));
    wlHostname.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.Hostname.Label.TipText"));
    props.setLook(wlHostname);
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment(0, 0);
    fdlHostname.right = new FormAttachment(middle, -margin);
    fdlHostname.top = new FormAttachment(0, margin);
    wlHostname.setLayoutData(fdlHostname);
    wHostname = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wHostname);
    wHostname.addModifyListener(lsMod);
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment(middle, 0);
    fdHostname.top = new FormAttachment(0, margin);
    fdHostname.right = new FormAttachment(100, 0);
    wHostname.setLayoutData(fdHostname);
    lastControl = wHostname;

    // Port input ...
    //
    Label wlPort = new Label(wConfigComp, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Port.Label"));
    wlPort.setToolTipText(BaseMessages.getString(PKG, "MongoDbInputDialog.Port.Label.TipText"));
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.right = new FormAttachment(middle, -margin);
    fdlPort.top = new FormAttachment(lastControl, margin);
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPort);
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(lastControl, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);
    lastControl = wPort;

    // enable ssl connection
    Label useSSLSocketFactoryL = new Label(wConfigComp, SWT.RIGHT);
    useSSLSocketFactoryL.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.UseSSLSocketFactory.Label"));
    props.setLook(useSSLSocketFactoryL);
    useSSLSocketFactoryL.setLayoutData(
        new FormDataBuilder()
            .left(0, -margin)
            .top(lastControl, 2 * margin)
            .right(middle, -margin)
            .result());

    wbUseSSLSocketFactory = new Button(wConfigComp, SWT.CHECK);
    props.setLook(wbUseSSLSocketFactory);
    wbUseSSLSocketFactory.addListener(SWT.Selection, e -> input.setChanged());
    wbUseSSLSocketFactory.setLayoutData(
        new FormDataBuilder()
            .left(middle, 0)
            .top(useSSLSocketFactoryL, 0, SWT.CENTER)
            .right(100, 0)
            .result());
    lastControl = wbUseSSLSocketFactory;

    // Use all replica set members/mongos check box
    Label useAllReplicaLab = new Label(wConfigComp, SWT.RIGHT);
    useAllReplicaLab.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.UseAllReplicaSetMembers.Label"));
    useAllReplicaLab.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.UseAllReplicaSetMembers.TipText"));
    props.setLook(useAllReplicaLab);
    FormData fdlRep = new FormData();
    fdlRep.left = new FormAttachment(0, 0);
    fdlRep.right = new FormAttachment(middle, -margin);
    fdlRep.top = new FormAttachment(lastControl, 2 * margin);
    useAllReplicaLab.setLayoutData(fdlRep);

    wbUseAllReplicaSetMembers = new Button(wConfigComp, SWT.CHECK);
    props.setLook(wbUseAllReplicaSetMembers);
    FormData fdbRep = new FormData();
    fdbRep.left = new FormAttachment(middle, 0);
    fdbRep.top = new FormAttachment(useAllReplicaLab, 0, SWT.CENTER);
    fdbRep.right = new FormAttachment(100, 0);
    wbUseAllReplicaSetMembers.setLayoutData(fdbRep);
    wbUseAllReplicaSetMembers.addListener(SWT.Selection, e -> input.setChanged());
    lastControl = useAllReplicaLab;

    // Authentication...
    //

    // AuthDbName line
    Label wlAuthDbName = new Label(wConfigComp, SWT.RIGHT);
    wlAuthDbName.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.AuthenticationDatabaseName.Label"));
    props.setLook(wlAuthDbName);
    FormData fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment(0, -margin);
    fdlAuthUser.top = new FormAttachment(lastControl, 2 * margin);
    fdlAuthUser.right = new FormAttachment(middle, -margin);
    wlAuthDbName.setLayoutData(fdlAuthUser);

    wAuthDbName = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAuthDbName);
    wAuthDbName.addModifyListener(lsMod);
    FormData fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment(middle, 0);
    fdAuthUser.top = new FormAttachment(wlAuthDbName, 0, SWT.CENTER);
    fdAuthUser.right = new FormAttachment(100, 0);
    wAuthDbName.setLayoutData(fdAuthUser);
    lastControl = wAuthDbName;

    // Authentication Mechanisms
    Label wlAuthMec = new Label(wConfigComp, SWT.RIGHT);
    wlAuthMec.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.AuthMechanism.Label"));
    props.setLook(wlAuthMec);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    wlAuthMec.setLayoutData(fd);

    wDbAuthMec = new CCombo(wConfigComp, SWT.BORDER);
    props.setLook(wDbAuthMec);

    wDbAuthMec.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            pipelineMeta.setChanged();
            wDbAuthMec.setToolTipText(wDbAuthMec.getText());
          }
        });
    wDbAuthMec.add("SCRAM-SHA-1");
    wDbAuthMec.add("MONGODB-CR");
    wDbAuthMec.add("PLAIN");
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wlAuthMec, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wDbAuthMec.setLayoutData(fd);
    lastControl = wDbAuthMec;

    // AuthUser line
    Label wlAuthUser = new Label(wConfigComp, SWT.RIGHT);
    wlAuthUser.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.AuthenticationUser.Label"));
    props.setLook(wlAuthUser);
    fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment(0, 0);
    fdlAuthUser.right = new FormAttachment(middle, -margin);
    fdlAuthUser.top = new FormAttachment(lastControl, margin);
    wlAuthUser.setLayoutData(fdlAuthUser);
    wAuthUser = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAuthUser);
    wAuthUser.addModifyListener(lsMod);
    fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment(middle, 0);
    fdAuthUser.top = new FormAttachment(wlAuthUser, 0, SWT.CENTER);
    fdAuthUser.right = new FormAttachment(100, 0);
    wAuthUser.setLayoutData(fdAuthUser);
    lastControl = wAuthUser;

    // AuthPass line
    Label wlAuthPass = new Label(wConfigComp, SWT.RIGHT);
    wlAuthPass.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.AuthenticationPassword.Label"));
    props.setLook(wlAuthPass);
    FormData fdlAuthPass = new FormData();
    fdlAuthPass.left = new FormAttachment(0, -margin);
    fdlAuthPass.top = new FormAttachment(lastControl, margin);
    fdlAuthPass.right = new FormAttachment(middle, -margin);
    wlAuthPass.setLayoutData(fdlAuthPass);
    wAuthPass = new PasswordTextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAuthPass);
    wAuthPass.addModifyListener(lsMod);
    FormData fdAuthPass = new FormData();
    fdAuthPass.left = new FormAttachment(middle, 0);
    fdAuthPass.top = new FormAttachment(wlAuthPass, 0, SWT.CENTER);
    fdAuthPass.right = new FormAttachment(100, 0);
    wAuthPass.setLayoutData(fdAuthPass);
    lastControl = wAuthPass;

    // use kerberos authentication
    Label kerbLab = new Label(wConfigComp, SWT.RIGHT);
    kerbLab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Kerberos.Label"));
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
    wbKerberos.addListener(SWT.Selection, e -> wAuthPass.setEnabled(!wbKerberos.getSelection()));
    lastControl = kerbLab;

    // connection timeout
    Label connectTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    connectTimeoutL.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.ConnectionTimeout.Label"));
    props.setLook(connectTimeoutL);
    connectTimeoutL.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.ConnectionTimeout.TipText"));
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    connectTimeoutL.setLayoutData(fd);
    wConnectionTimeout = new TextVar( variables, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConnectionTimeout);
    wConnectionTimeout.addModifyListener(lsMod);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(connectTimeoutL, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wConnectionTimeout.setLayoutData(fd);
    lastControl = wConnectionTimeout;

    // socket timeout
    Label socketTimeoutL = new Label(wConfigComp, SWT.RIGHT);
    socketTimeoutL.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.SocketTimeout.Label"));
    props.setLook(socketTimeoutL);
    socketTimeoutL.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.SocketTimeout.TipText"));
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
    lastControl = wSocketTimeout;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wConfigComp.setLayoutData(fd);

    wConfigComp.layout();
    wConfigTab.setControl(wConfigComp);

    // Input options tab -----
    CTabItem wInputOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wInputOptionsTab.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.InputOptionsTab.TabTitle"));
    Composite wInputOptionsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wInputOptionsComp);
    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 3;
    inputLayout.marginHeight = 3;
    wInputOptionsComp.setLayout(inputLayout);

    // DbName input ...
    //
    Label wlDbName = new Label(wInputOptionsComp, SWT.RIGHT);
    wlDbName.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.DbName.Label"));
    props.setLook(wlDbName);
    FormData fdlDbName = new FormData();
    fdlDbName.left = new FormAttachment(0, 0);
    fdlDbName.right = new FormAttachment(middle, -margin);
    fdlDbName.top = new FormAttachment(0, margin);
    wlDbName.setLayoutData(fdlDbName);

    Button wbGetDbs = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetDbs);
    wbGetDbs.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.DbName.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    wbGetDbs.setLayoutData(fd);

    wbGetDbs.addListener(SWT.Selection, e -> setupDBNames());

    wDbName = new CCombo(wInputOptionsComp, SWT.BORDER);
    props.setLook(wDbName);
    wDbName.addModifyListener(lsMod);
    FormData fdDbName = new FormData();
    fdDbName.left = new FormAttachment(middle, 0);
    fdDbName.top = new FormAttachment(0, margin);
    fdDbName.right = new FormAttachment(wbGetDbs, 0);
    wDbName.setLayoutData(fdDbName);
    lastControl = wDbName;

    wDbName.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            input.setChanged();
            wDbName.setToolTipText(variables.resolve(wDbName.getText()));
          }
        });

    // Collection input ...
    //
    Label wlCollection = new Label(wInputOptionsComp, SWT.RIGHT);
    wlCollection.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Collection.Label"));
    props.setLook(wlCollection);
    FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment(0, 0);
    fdlCollection.right = new FormAttachment(middle, -margin);
    fdlCollection.top = new FormAttachment(lastControl, margin);
    wlCollection.setLayoutData(fdlCollection);

    Button wbGetCollections = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetCollections);
    wbGetCollections.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.GetCollections.Button"));
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(lastControl, 0);
    wbGetCollections.setLayoutData(fd);
    wbGetCollections.addListener(SWT.Selection, e -> setupCollectionNamesForDB());

    wCollection = new CCombo(wInputOptionsComp, SWT.BORDER);
    props.setLook(wCollection);
    wCollection.addModifyListener(lsMod);
    FormData fdCollection = new FormData();
    fdCollection.left = new FormAttachment(middle, 0);
    fdCollection.top = new FormAttachment(lastControl, margin);
    fdCollection.right = new FormAttachment(wbGetCollections, 0);
    wCollection.setLayoutData(fdCollection);
    lastControl = wCollection;
    wCollection.addListener(SWT.Selection, e -> updateQueryTitleInfo());
    wCollection.addListener(SWT.FocusOut, e -> updateQueryTitleInfo());

    // read preference
    Label readPrefL = new Label(wInputOptionsComp, SWT.RIGHT);
    readPrefL.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.ReadPreferenceLabel"));
    props.setLook(readPrefL);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    readPrefL.setLayoutData(fd);

    wReadPreference = new CCombo(wInputOptionsComp, SWT.BORDER);
    props.setLook(wReadPreference);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(100, 0);
    wReadPreference.setLayoutData(fd);
    wReadPreference.addModifyListener(
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            input.setChanged();
            wReadPreference.setToolTipText(
                variables.resolve(wReadPreference.getText()));
          }
        });

    for (NamedReadPreference preference : NamedReadPreference.values()) {
      wReadPreference.add(preference.getName());
    }

    lastControl = wReadPreference;

    // test tag set but
    Button testUserTagsBut = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(testUserTagsBut);
    testUserTagsBut.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.TestUserTags.Button"));
    testUserTagsBut.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.TestUserTags.Button.TipText"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.right = new FormAttachment(100, 0);
    testUserTagsBut.setLayoutData(fd);
    testUserTagsBut.addListener(SWT.Selection, e -> testUserSpecifiedTagSetsAgainstReplicaSet());

    Button joinTagsBut = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(joinTagsBut);
    joinTagsBut.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.GetTags.Button"));
    joinTagsBut.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.GetTags.Button.TipText"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.right = new FormAttachment(testUserTagsBut, -2 * margin);
    joinTagsBut.setLayoutData(fd);
    joinTagsBut.addListener(SWT.Selection, e -> concatenateTags());

    Button getTagsBut = new Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(getTagsBut);
    getTagsBut.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.GetTags.Button"));
    getTagsBut.setToolTipText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.GetTags.Button.TipText"));
    fd = new FormData();
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.right = new FormAttachment(joinTagsBut, -2 * margin);
    getTagsBut.setLayoutData(fd);
    getTagsBut.addListener(SWT.Selection, e -> setupTagSetComboValues());

    ColumnInfo[] tagsColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.TagSets.TagSetColumnTitle"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    tagsColumns[0].setReadOnly(false);

    Label tagSetsTitle = new Label(wInputOptionsComp, SWT.LEFT);
    tagSetsTitle.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.TagSets.Title"));
    props.setLook(tagSetsTitle);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.top = new FormAttachment(lastControl, margin);
    fd.right = new FormAttachment(middle, -margin);
    tagSetsTitle.setLayoutData(fd);
    lastControl = tagSetsTitle;

    wTags =
        new TableView(
            variables,
            wInputOptionsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            tagsColumns,
            1,
            lsMod,
            props);

    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin * 2);
    fd.bottom = new FormAttachment(100, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wTags.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wInputOptionsComp.setLayoutData(fd);

    wInputOptionsComp.layout();
    wInputOptionsTab.setControl(wInputOptionsComp);

    // Query tab -----
    CTabItem wMongoQueryTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoQueryTab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.QueryTab.TabTitle"));
    Composite wQueryComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wQueryComp);
    FormLayout queryLayout = new FormLayout();
    queryLayout.marginWidth = 3;
    queryLayout.marginHeight = 3;
    wQueryComp.setLayout(queryLayout);

    // fields input ...
    //
    Label wlFieldsName = new Label(wQueryComp, SWT.RIGHT);
    wlFieldsName.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.FieldsName.Label"));
    props.setLook(wlFieldsName);
    FormData fdlFieldsName = new FormData();
    fdlFieldsName.left = new FormAttachment(0, 0);
    fdlFieldsName.right = new FormAttachment(middle, -margin);
    fdlFieldsName.bottom = new FormAttachment(100, -margin);
    wlFieldsName.setLayoutData(fdlFieldsName);
    wFieldsName = new TextVar( variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFieldsName);
    wFieldsName.addModifyListener(lsMod);
    FormData fdFieldsName = new FormData();
    fdFieldsName.left = new FormAttachment(middle, 0);
    fdFieldsName.bottom = new FormAttachment(100, -margin);
    fdFieldsName.right = new FormAttachment(100, 0);
    wFieldsName.setLayoutData(fdFieldsName);
    lastControl = wFieldsName;

    Label executeForEachRLab = new Label(wQueryComp, SWT.RIGHT);
    executeForEachRLab.setText(
        BaseMessages.getString(PKG, "MongoDbInputDialog.ExecuteForEachRow.Label"));
    props.setLook(executeForEachRLab);
    fd = new FormData();
    fd.left = new FormAttachment(0, -margin);
    fd.bottom = new FormAttachment(lastControl, -2 * margin);
    fd.right = new FormAttachment(middle, -margin);
    executeForEachRLab.setLayoutData(fd);
    wbExecuteForEachRow = new Button(wQueryComp, SWT.CHECK);
    props.setLook(wbExecuteForEachRow);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(executeForEachRLab, 0, SWT.CENTER);
    wbExecuteForEachRow.setLayoutData(fd);
    lastControl = executeForEachRLab;

    Label queryIsPipelineL = new Label(wQueryComp, SWT.RIGHT);
    queryIsPipelineL.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Pipeline.Label"));
    props.setLook(queryIsPipelineL);
    fd = new FormData();
    fd.bottom = new FormAttachment(lastControl, -2 * margin);
    fd.left = new FormAttachment(0, -margin);
    fd.right = new FormAttachment(middle, -margin);
    queryIsPipelineL.setLayoutData(fd);
    wbQueryIsPipeline = new Button(wQueryComp, SWT.CHECK);
    props.setLook(wbQueryIsPipeline);
    fd = new FormData();
    fd.top = new FormAttachment(queryIsPipelineL, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wbQueryIsPipeline.setLayoutData(fd);
    wbQueryIsPipeline.addListener(SWT.Selection, e -> updateQueryTitleInfo());
    lastControl = queryIsPipelineL;

    // JSON Query input ...
    //
    wlJsonQuery = new Label(wQueryComp, SWT.NONE);
    wlJsonQuery.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label"));
    props.setLook(wlJsonQuery);
    FormData fdlJsonQuery = new FormData();
    fdlJsonQuery.left = new FormAttachment(0, 0);
    fdlJsonQuery.right = new FormAttachment(100, -margin);
    fdlJsonQuery.top = new FormAttachment(0, margin);
    wlJsonQuery.setLayoutData(fdlJsonQuery);

    wJsonQuery =
        new StyledTextComp(
            variables,
            wQueryComp,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL
        );
    props.setLook(wJsonQuery, PropsUi.WIDGET_STYLE_FIXED);
    wJsonQuery.addModifyListener(lsMod);

    /*
     * wJsonQuery = new TextVar( variables, wQueryComp, SWT.SINGLE | SWT.LEFT |
     * SWT.BORDER); props.setLook(wJsonQuery);
     * wJsonQuery.addModifyListener(lsMod);
     */
    FormData fdJsonQuery = new FormData();
    fdJsonQuery.left = new FormAttachment(0, 0);
    fdJsonQuery.top = new FormAttachment(wlJsonQuery, margin);
    fdJsonQuery.right = new FormAttachment(100, -2 * margin);
    fdJsonQuery.bottom = new FormAttachment(lastControl, -2 * margin);
    // wJsonQuery.setLayoutData(fdJsonQuery);
    wJsonQuery.setLayoutData(fdJsonQuery);
    // lastControl = wJsonQuery;
    lastControl = wJsonQuery;

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wQueryComp.setLayoutData(fd);

    wQueryComp.layout();
    wMongoQueryTab.setControl(wQueryComp);

    // fields tab -----
    CTabItem wMongoFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMongoFieldsTab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.FieldsTab.TabTitle"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // Output as Json check box
    Label outputJLab = new Label(wFieldsComp, SWT.RIGHT);
    outputJLab.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.OutputJson.Label"));
    props.setLook(outputJLab);
    fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -2 * margin);
    outputJLab.setLayoutData(fd);
    wbOutputAsJson = new Button(wFieldsComp, SWT.CHECK);
    props.setLook(wbOutputAsJson);
    fd = new FormData();
    fd.top = new FormAttachment(outputJLab, 0, SWT.CENTER);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wbOutputAsJson.setLayoutData(fd);
    wbOutputAsJson.addListener(
        SWT.Selection,
        e -> {
          input.setChanged();
          wGet.setEnabled(!wbOutputAsJson.getSelection());
          wJsonField.setEnabled(wbOutputAsJson.getSelection());
        });
    lastControl = wbOutputAsJson;

    // JsonField input ...
    //
    Label wlJsonField = new Label(wFieldsComp, SWT.RIGHT);
    wlJsonField.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonField.Label"));
    props.setLook(wlJsonField);
    FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment(0, 0);
    fdlJsonField.right = new FormAttachment(middle, -margin);
    fdlJsonField.top = new FormAttachment(lastControl, 2 * margin);
    wlJsonField.setLayoutData(fdlJsonField);
    wJsonField = new TextVar( variables, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wJsonField);
    wJsonField.addModifyListener(lsMod);
    FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment(middle, 0);
    fdJsonField.top = new FormAttachment(lastControl, margin);
    fdJsonField.right = new FormAttachment(100, 0);
    wJsonField.setLayoutData(fdJsonField);
    lastControl = wJsonField;

    // get fields button
    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.Button.GetFields"));
    props.setLook(wGet);
    fd = new FormData();
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fd);
    wGet.addListener(
        SWT.Selection,
        e -> {
          // populate table from schema
          MongoDbInputMeta newMeta = (MongoDbInputMeta) input.clone();
          getFields(newMeta);
        });

    // fields stuff
    final ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_NAME"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_PATH"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_TYPE"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.FIELD_INDEXED"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_ARRAYINFO"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_PERCENTAGE"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MongoDbInputDialog.Fields.SAMPLE_DISPARATE_TYPES"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[2].setComboValues(ValueMetaFactory.getAllValueMetaNames());
    colinf[4].setReadOnly(true);
    colinf[5].setReadOnly(true);
    colinf[6].setReadOnly(true);

    wFields =
        new TableView(
            variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props);

    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin * 2);
    fd.bottom = new FormAttachment(wGet, -margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    wFields.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fd);

    wFieldsComp.layout();
    wMongoFieldsTab.setControl(wFieldsComp);

    // --------------

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTransformName, margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fd);

    // Add listeners
    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wHostname.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData(input);
    input.setChanged(changed);

    wTabFolder.setSelection(0);
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData(MongoDbInputMeta meta) {
    wHostname.setText(Const.NVL(meta.getHostnames(), ""));
    wPort.setText(Const.NVL(meta.getPort(), ""));
    wbUseAllReplicaSetMembers.setSelection(meta.getUseAllReplicaSetMembers());
    wDbAuthMec.setText(Const.NVL(meta.getAuthenticationMechanism(), ""));
    wDbName.setText(Const.NVL(meta.getDbName(), ""));
    wFieldsName.setText(Const.NVL(meta.getFieldsName(), ""));
    wCollection.setText(Const.NVL(meta.getCollection(), ""));
    wJsonField.setText(Const.NVL(meta.getJsonFieldName(), ""));
    wJsonQuery.setText(Const.NVL(meta.getJsonQuery(), ""));

    wAuthDbName.setText(Const.NVL(meta.getAuthenticationDatabaseName(), ""));
    wAuthUser.setText(Const.NVL(meta.getAuthenticationUser(), ""));
    wAuthPass.setText(Const.NVL(meta.getAuthenticationPassword(), ""));
    wbKerberos.setSelection(meta.getUseKerberosAuthentication());
    wAuthPass.setEnabled(!wbKerberos.getSelection());
    wConnectionTimeout.setText(Const.NVL(meta.getConnectTimeout(), ""));
    wSocketTimeout.setText(Const.NVL(meta.getSocketTimeout(), ""));
    wbUseSSLSocketFactory.setSelection(meta.isUseSSLSocketFactory());
    wReadPreference.setText(Const.NVL(meta.getReadPreference(), ""));
    wbQueryIsPipeline.setSelection(meta.isQueryIsPipeline());
    wbOutputAsJson.setSelection(meta.isOutputJson());
    wbExecuteForEachRow.setSelection(meta.getExecuteForEachIncomingRow());

    setFieldTableFields(meta.getMongoFields());
    setTagsTableFields(meta.getReadPrefTagSets());

    wJsonField.setEnabled(meta.isOutputJson());
    wGet.setEnabled(!meta.isOutputJson());

    updateQueryTitleInfo();

    wTransformName.selectAll();
  }

  private void updateQueryTitleInfo() {
    if (wbQueryIsPipeline.getSelection()) {
      wlJsonQuery.setText(
          BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label2")
              + ": db."
              + Const.NVL(wCollection.getText(), "n/a")
              + ".aggregate(...");
      wFieldsName.setEnabled(false);
    } else {
      wlJsonQuery.setText(BaseMessages.getString(PKG, "MongoDbInputDialog.JsonQuery.Label"));
      wFieldsName.setEnabled(true);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(MongoDbInputMeta meta) {

    meta.setHostnames(wHostname.getText());
    meta.setPort(wPort.getText());
    meta.setUseAllReplicaSetMembers(wbUseAllReplicaSetMembers.getSelection());
    meta.setDbName(wDbName.getText());
    meta.setFieldsName(wFieldsName.getText());
    meta.setCollection(wCollection.getText());
    meta.setJsonFieldName(wJsonField.getText());
    meta.setJsonQuery(wJsonQuery.getText());

    meta.setAuthenticationDatabaseName(wAuthDbName.getText());
    meta.setAuthenticationUser(wAuthUser.getText());
    meta.setAuthenticationPassword(wAuthPass.getText());
    meta.setAuthenticationMechanism(wDbAuthMec.getText());
    meta.setUseKerberosAuthentication(wbKerberos.getSelection());
    meta.setConnectTimeout(wConnectionTimeout.getText());
    meta.setSocketTimeout(wSocketTimeout.getText());
    meta.setUseSSLSocketFactory(wbUseSSLSocketFactory.getSelection());
    meta.setReadPreference(wReadPreference.getText());
    meta.setOutputJson(wbOutputAsJson.getSelection());
    meta.setQueryIsPipeline(wbQueryIsPipeline.getSelection());
    meta.setExecuteForEachIncomingRow(wbExecuteForEachRow.getSelection());

    int numNonEmpty = wFields.nrNonEmpty();
    if (numNonEmpty > 0) {
      List<MongoField> outputFields = new ArrayList<>();
      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wFields.getNonEmpty(i);
        MongoField newField = new MongoField();

        newField.fieldName = item.getText(1).trim();
        newField.fieldPath = item.getText(2).trim();
        newField.hopType = item.getText(3).trim();

        if (!StringUtils.isEmpty(item.getText(4))) {
          newField.indexedValues = MongoDbInputData.indexedValsList(item.getText(4).trim());
        }

        outputFields.add(newField);
      }

      meta.setMongoFields(outputFields);
    }

    numNonEmpty = wTags.nrNonEmpty();

    List<String> tags = new ArrayList<>();
    if (numNonEmpty > 0) {

      for (int i = 0; i < numNonEmpty; i++) {
        TableItem item = wTags.getNonEmpty(i);
        String t = item.getText(1).trim();
        if (!t.startsWith("{")) {
          t = "{" + t;
        }
        if (!t.endsWith("}")) {
          t += "}";
        }

        tags.add(t);
      }
    }
    meta.setReadPrefTagSets(tags);
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  public boolean isTableDisposed() {
    return wFields.isDisposed();
  }

  private void setTagsTableFields(List<String> tags) {
    if (tags == null) {
      return;
    }

    wTags.clearAll();

    for (String t : tags) {
      TableItem item = new TableItem(wTags.table, SWT.NONE);
      item.setText(1, t);
    }

    wTags.removeEmptyRows();
    wTags.setRowNums();
    wTags.optWidth(true);
  }

  private void setFieldTableFields(List<MongoField> fields) {
    if (fields == null) {
      return;
    }

    wFields.clearAll();
    for (MongoField f : fields) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);

      updateTableItem(item, f);
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  public void updateFieldTableFields(List<MongoField> fields) {
    Map<String, MongoField> fieldMap = new HashMap<>( fields.size() );
    for (MongoField field : fields) {
      fieldMap.put(field.fieldName, field);
    }

    int index = 0;
    List<Integer> indicesToRemove = new ArrayList<>();
    for (TableItem tableItem : wFields.getTable().getItems()) {
      String name = tableItem.getText(1);
      MongoField mongoField = fieldMap.remove(name);
      if (mongoField == null) {
        // Value does not exist in incoming fields list and exists in table, remove old value from
        // table
        indicesToRemove.add(index);
      } else {
        // Value exists in incoming fields list and in table, update entry
        updateTableItem(tableItem, mongoField);
      }
      index++;
    }

    int[] indicesArray = new int[indicesToRemove.size()];
    for (int i = 0; i < indicesArray.length; i++) {
      indicesArray[i] = indicesToRemove.get(i);
    }

    for (MongoField mongoField : fieldMap.values()) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      updateTableItem(item, mongoField);
    }
    wFields.setRowNums();
    wFields.remove(indicesArray);
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void updateTableItem(TableItem tableItem, MongoField mongoField) {
    if (!StringUtils.isEmpty(mongoField.fieldName)) {
      tableItem.setText(1, mongoField.fieldName);
    }

    if (!StringUtils.isEmpty(mongoField.fieldPath)) {
      tableItem.setText(2, mongoField.fieldPath);
    }

    if (!StringUtils.isEmpty(mongoField.hopType)) {
      tableItem.setText(3, mongoField.hopType);
    }

    if (mongoField.indexedValues != null && mongoField.indexedValues.size() > 0) {
      tableItem.setText(4, MongoDbInputData.indexedValsList(mongoField.indexedValues));
    }

    if (!StringUtils.isEmpty(mongoField.arrayIndexInfo)) {
      tableItem.setText(5, mongoField.arrayIndexInfo);
    }

    if (!StringUtils.isEmpty(mongoField.occurrenceFraction)) {
      tableItem.setText(6, mongoField.occurrenceFraction);
    }

    if (mongoField.disparateTypes) {
      tableItem.setText(7, "Y");
    }
  }

  private boolean checkForUnresolved(MongoDbInputMeta meta, String title) {

    String query = variables.resolve(meta.getJsonQuery());

    boolean notOk = (query.contains("${") || query.contains("?{"));

    if (notOk) {
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              title,
              BaseMessages.getString(
                  PKG,
                  "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs"));
      smd.open();
    }

    return !notOk;
  }

  // Used to catch exceptions from discoverFields calls that come through the callback
  public void handleNotificationException(Exception exception) {
    new ErrorDialog(
        shell,
        transformName,
        BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling"),
        exception);
  }

  private void getFields(MongoDbInputMeta meta) {
    if (!StringUtils.isEmpty(wHostname.getText())
        && !StringUtils.isEmpty(wDbName.getText())
        && !StringUtils.isEmpty(wCollection.getText())) {
      EnterNumberDialog end =
          new EnterNumberDialog(
              shell,
              100,
              BaseMessages.getString(PKG, "MongoDbInputDialog.SampleDocuments.Title"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.SampleDocuments.Message"));
      int samples = end.open();
      if (samples > 0) {

        getInfo(meta);
        // Turn off execute for each incoming row (if set).
        // Query is still going to
        // be stuffed if the user has specified field replacement (i.e.
        // ?{...}) in the query string
        boolean current = meta.getExecuteForEachIncomingRow();
        meta.setExecuteForEachIncomingRow(false);

        if (!checkForUnresolved(
            meta,
            BaseMessages.getString(
                PKG,
                "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.SamplingTitle"))) {

          return;
        }

        try {
          discoverFields(meta, variables, samples, this);
          meta.setExecuteForEachIncomingRow(current);
        } catch (HopException e) {
          new ErrorDialog(
              shell,
              transformName,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling"),
              e);
        }
      }
    } else {
      // pop up an error dialog

      String missingConDetails = "";
      if (StringUtils.isEmpty(wHostname.getText())) {
        missingConDetails += " host name(s)";
      }
      if (StringUtils.isEmpty(wDbName.getText())) {
        missingConDetails += " database";
      }
      if (StringUtils.isEmpty(wCollection.getText())) {
        missingConDetails += " collection";
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
                  missingConDetails));
      smd.open();
    }
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    MongoDbInputMeta oneMeta = new MongoDbInputMeta();
    getInfo(oneMeta);

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow(false);

    if (!checkForUnresolved(
        oneMeta,
        BaseMessages.getString(
            PKG,
            "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.PreviewTitle"))) {
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            variables, metadataProvider, oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "MongoDbInputDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell, variables, previewMeta, new String[] {wTransformName.getText()}, new int[] {previewSize});
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              shell,
              variables,
              SWT.NONE,
              wTransformName.getText(),
              progressDialog.getPreviewRowsMeta(wTransformName.getText()),
              progressDialog.getPreviewRows(wTransformName.getText()),
              loggingText);
      prd.open();
    }
  }

  private void setupDBNames() {
    String current = wDbName.getText();
    wDbName.removeAll();

    String hostname = variables.resolve(wHostname.getText());

    if (!StringUtils.isEmpty(hostname)) {

      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo(meta);
      try {
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        List<String> dbNames = new ArrayList<>();
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }

        for (String s : dbNames) {
          wDbName.add(s);
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            e);
      }
    } else {
      // popup some feedback
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString(
                  PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails", "host name(s)"));
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wDbName.setText(current);
    }
  }

  private void setupCollectionNamesForDB() {
    final String hostname = variables.resolve(wHostname.getText());
    final String dB = variables.resolve(wDbName.getText());

    String current = wCollection.getText();
    wCollection.removeAll();

    if (!StringUtils.isEmpty(hostname) && !StringUtils.isEmpty(dB)) {

      final MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo(meta);
      try {
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        Set<String> collections = new HashSet<>();
        try {
          collections = wrapper.getCollectionsNames(dB);
        } finally {
          wrapper.dispose();
        }

        for (String c : collections) {
          wCollection.add(c);
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            e);
      }
    } else {
      // popup some feedback

      String missingConnDetails = "";
      if (StringUtils.isEmpty(hostname)) {
        missingConnDetails += "host name(s)";
      }
      if (StringUtils.isEmpty(dB)) {
        missingConnDetails += " database";
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
  }

  private void setupTagSetComboValues() {
    String hostname = variables.resolve(wHostname.getText());

    if (!StringUtils.isEmpty(hostname)) {
      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo(meta);

      try {
        MongoClientWrapper wrapper =
            MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
        List<String> repSetTags = new ArrayList<>();
        try {
          repSetTags = wrapper.getAllTags();
        } finally {
          wrapper.dispose();
        }

        if (repSetTags.size() == 0) {
          ShowMessageDialog smd =
              new ShowMessageDialog(
                  shell,
                  SWT.ICON_WARNING | SWT.OK,
                  BaseMessages.getString(
                      PKG, "MongoDbInputDialog.Info.Message.NoTagSetsDefinedOnServer"),
                  BaseMessages.getString(
                      PKG, "MongoDbInputDialog.Info.Message.NoTagSetsDefinedOnServer"));
          smd.open();
        } else {
          this.setTagsTableFields(repSetTags);
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
            e);
      }
    } else {
      // popup some feedback
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString(
                  PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title"),
              BaseMessages.getString(
                  PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails", "host name(s)"));
      smd.open();
    }
  }
  /* Only referenced in commented code, commenting out also
  private void deleteSelectedFromView() {
    if (m_tagsView.nrNonEmpty() > 0 && m_tagsView.getSelectionIndex() >= 0) {
      int selectedI = m_tagsView.getSelectionIndex();

      m_tagsView.remove(selectedI);
      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
    }
  }

  private void moveSelectedTagSetToEditor() {
    if (m_tagsView.nrNonEmpty() > 0 && m_tagsView.getSelectionIndex() >= 0) {
      int selectedI = m_tagsView.getSelectionIndex();

      String selected = m_tagsView.getItem(selectedI)[0];
      if (selected.startsWith("{")) {
        selected = selected.substring(1);
      }
      if (selected.endsWith("}")) {
        selected = selected.substring(0, selected.length() - 1);
      }

      m_tagsCombo.setText(selected);
      m_currentTagsState = selected;

      m_tagsView.remove(selectedI);
      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
    }
  }

  private void addTagsToTable() {
    if (!StringUtils.isEmpty(m_tagsCombo.getText())) {
      TableItem item = new TableItem(m_tagsView.table, SWT.NONE);
      String tagSet = m_tagsCombo.getText();
      if (!tagSet.startsWith("{")) {
        tagSet = "{" + tagSet;
      }
      if (!tagSet.endsWith("}")) {
        tagSet = tagSet + "}";
      }
      item.setText(1, tagSet);

      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
      m_tagsView.optWidth(true);

      m_currentTagsState = "";
      m_tagsCombo.setText("");
    }
  }*/

  private void testUserSpecifiedTagSetsAgainstReplicaSet() {
    if (wTags.nrNonEmpty() > 0) {
      List<DBObject> tagSets = new ArrayList<>();

      for (int i = 0; i < wTags.nrNonEmpty(); i++) {
        TableItem item = wTags.getNonEmpty(i);

        String set = item.getText(1).trim();
        if (!set.startsWith("{")) {
          set = "{" + set;
        }

        if (!set.endsWith("}")) {
          set = set + "}";
        }

        DBObject setO = (DBObject) JSON.parse(set);
        if (setO != null) {
          tagSets.add(setO);
        }
      }

      if (tagSets.size() > 0) {
        String hostname = variables.resolve(wHostname.getText());
        try {
          if (!StringUtils.isEmpty(hostname)) {
            MongoDbInputMeta meta = new MongoDbInputMeta();
            getInfo(meta);
            MongoClientWrapper wrapper = null;
            try {
              wrapper = MongoWrapperUtil.createMongoClientWrapper(meta, variables, log);
            } catch (MongoDbException e) {
              throw new HopException(e);
            }
            List<String> satisfy = new ArrayList<>();
            try {
              try {
                satisfy = wrapper.getReplicaSetMembersThatSatisfyTagSets(tagSets);
              } catch (MongoDbException e) {
                throw new HopException(e);
              }
            } finally {
              try {
                wrapper.dispose();
              } catch (MongoDbException e) {
                // Ignore
              }
            }

            if (satisfy.size() == 0) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets"));
              ShowMessageDialog smd =
                  new ShowMessageDialog(
                      shell,
                      SWT.ICON_INFORMATION | SWT.OK,
                      BaseMessages.getString(
                          PKG,
                          "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets.Title"),
                      BaseMessages.getString(
                          PKG, "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets"));
              smd.open();
            } else {
              StringBuilder builder = new StringBuilder();
              builder.append("\n");
              for (int i = 0; i < satisfy.size(); i++) {
                builder.append(satisfy.get(i)).append("\n");
              }

              ShowMessageDialog smd =
                  new ShowMessageDialog(
                      shell,
                      SWT.ICON_INFORMATION | SWT.OK,
                      BaseMessages.getString(
                          PKG, "MongoDbInputDialog.Info.Message.MatchingReplicaSetMembers.Title"),
                      builder.toString());
              smd.open();
            }
          } else {
            // popup dialog saying that no connection details are available
            ShowMessageDialog smd =
                new ShowMessageDialog(
                    shell,
                    SWT.ICON_ERROR | SWT.OK,
                    BaseMessages.getString(
                        PKG, "MongoDbInputDialog.ErrorMessage.NoConnectionDetailsSupplied.Title"),
                    BaseMessages.getString(
                        PKG, "MongoDbInputDialog.ErrorMessage.NoConnectionDetailsSupplied"));
            smd.open();
          }

        } catch (HopException ex) {
          // popup an error dialog
          logError(
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"), ex);
          new ErrorDialog(
              shell,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect"),
              ex);
        }
      } else {
        // popup a dialog stating that there were no parseable tag sets
        ShowMessageDialog smd =
            new ShowMessageDialog(
                shell,
                SWT.ICON_ERROR | SWT.OK,
                BaseMessages.getString(
                    PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets.Title"),
                BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets"));
        smd.open();
      }
    } else {
      // popup a dialog saying that there are no tag sets defined
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_ERROR | SWT.OK,
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.NoTagSetsDefined.Title"),
              BaseMessages.getString(PKG, "MongoDbInputDialog.ErrorMessage.NoTagSetsDefined"));
      smd.open();
    }
  }

  private void concatenateTags() {
    int[] selectedTags = this.wTags.getSelectionIndices();
    String concatenated = "";

    for (int i : selectedTags) {
      TableItem item = wTags.table.getItem(i);
      String t = item.getText(1).trim();
      concatenated =
          concatenated
              + ((concatenated.length() > 0) ? ((!concatenated.endsWith(",")) ? ", " : "") : "")
              + t; // //
    }
    TableItem item = new TableItem(wTags.table, SWT.NONE);
    item.setText(1, concatenated);
  }

  public static void discoverFields(
      final MongoDbInputMeta meta,
      final IVariables vars,
      final int docsToSample,
      final MongoDbInputDialog mongoDialog)
      throws HopException {
    MongoProperties.Builder propertiesBuilder =
        MongoWrapperUtil.createPropertiesBuilder(meta, vars);
    String db = vars.resolve(meta.getDbName());
    String collection = vars.resolve(meta.getCollection());
    String query = vars.resolve(meta.getJsonQuery());
    String fields = vars.resolve(meta.getFieldsName());
    int numDocsToSample = docsToSample;
    if (numDocsToSample < 1) {
      numDocsToSample = 100; // default
    }
    try {
      MongoDbInputData.getMongoDbInputDiscoverFieldsHolder()
          .getMongoDbInputDiscoverFields()
          .discoverFields(
              propertiesBuilder,
              db,
              collection,
              query,
              fields,
              meta.isQueryIsPipeline(),
              numDocsToSample,
              meta,
              new DiscoverFieldsCallback() {
                @Override
                public void notifyFields(final List<MongoField> fields) {
                  if (fields.size() > 0) {
                    mongoDialog
                        .getParent()
                        .getDisplay()
                        .asyncExec(
                            new Runnable() {
                              @Override
                              public void run() {
                                if (!mongoDialog.isTableDisposed()) {
                                  meta.setMongoFields(fields);
                                  mongoDialog.updateFieldTableFields(meta.getMongoFields());
                                }
                              }
                            });
                  }
                }

                @Override
                public void notifyException(Exception exception) {
                  mongoDialog.handleNotificationException(exception);
                }
              });
    } catch (HopException e) {
      throw new HopException("Unable to discover fields from MongoDB", e);
    }
  }

  public static boolean discoverFields(
      final MongoDbInputMeta meta, final IVariables vars, final int docsToSample)
      throws HopException {

    MongoProperties.Builder propertiesBuilder =
        MongoWrapperUtil.createPropertiesBuilder(meta, vars);
    try {
      String db = vars.resolve(meta.getDbName());
      String collection = vars.resolve(meta.getCollection());
      String query = vars.resolve(meta.getJsonQuery());
      String fields = vars.resolve(meta.getFieldsName());
      int numDocsToSample = docsToSample;
      if (numDocsToSample < 1) {
        numDocsToSample = 100; // default
      }
      List<MongoField> discoveredFields =
          MongoDbInputData.getMongoDbInputDiscoverFieldsHolder()
              .getMongoDbInputDiscoverFields()
              .discoverFields(
                  propertiesBuilder,
                  db,
                  collection,
                  query,
                  fields,
                  meta.isQueryIsPipeline(),
                  numDocsToSample,
                  meta);

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if (discoveredFields.size() > 0) {
        meta.setMongoFields(discoveredFields);
        return true;
      }
    } catch (Exception e) {
      if (e instanceof HopException) {
        throw (HopException) e;
      } else {
        throw new HopException("Unable to discover fields from MongoDB", e);
      }
    }
    return false;
  }
}
