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

package org.apache.hop.pipeline.transforms.odata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ODataInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ODataInputMeta.class;

  private final ODataInputMeta input;

  private TextVar wUrl;
  private ComboVar wEntitySet;
  private ComboVar wAuthType;
  private TextVar wUsername;
  private PasswordTextVar wPassword;
  private PasswordTextVar wToken;

  private TextVar wQuerySelect;
  private TextVar wQueryFilter;
  private TextVar wQueryOrder;
  private TextVar wQueryTop;
  private TextVar wQuerySkip;

  private TableView wFields;

  public ODataInputDialog(
      Shell parent, IVariables variables, ODataInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ODataInput.Name"));

    middle = props.getMiddlePct();
    margin = PropsUi.getFormMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ODataInputDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
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

    // Button bar at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "ODataInputDialog.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "ODataInputDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Tab Folder
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF CONNECTION TAB
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "ODataInputDialog.GeneralTab.Title"));
    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);
    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = margin;
    generalLayout.marginHeight = margin;
    wGeneralComp.setLayout(generalLayout);

    // URL line
    Label wlUrl = new Label(wGeneralComp, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "ODataInputDialog.ServiceUrl.Label"));
    PropsUi.setLook(wlUrl);
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.right = new FormAttachment(middle, -margin);
    fdlUrl.top = new FormAttachment(0, margin);
    wlUrl.setLayoutData(fdlUrl);
    wUrl = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.addModifyListener(lsMod);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.right = new FormAttachment(100, 0);
    fdUrl.top = new FormAttachment(0, margin);
    wUrl.setLayoutData(fdUrl);

    // AuthType line
    Label wlAuthType = new Label(wGeneralComp, SWT.RIGHT);
    wlAuthType.setText(BaseMessages.getString(PKG, "ODataInputDialog.AuthType.Label"));
    PropsUi.setLook(wlAuthType);
    FormData fdlAuthType = new FormData();
    fdlAuthType.left = new FormAttachment(0, 0);
    fdlAuthType.right = new FormAttachment(middle, -margin);
    fdlAuthType.top = new FormAttachment(wUrl, margin);
    wlAuthType.setLayoutData(fdlAuthType);
    wAuthType = new ComboVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAuthType.setItems(ODataAuthType.getDescriptions());
    PropsUi.setLook(wAuthType);
    wAuthType.addModifyListener(lsMod);
    FormData fdAuthType = new FormData();
    fdAuthType.left = new FormAttachment(middle, 0);
    fdAuthType.right = new FormAttachment(100, 0);
    fdAuthType.top = new FormAttachment(wUrl, margin);
    wAuthType.setLayoutData(fdAuthType);

    // Username line
    Label wlUsername = new Label(wGeneralComp, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "ODataInputDialog.Username.Label"));
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    fdlUsername.top = new FormAttachment(wAuthType, margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);
    wUsername.addModifyListener(lsMod);
    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(100, 0);
    fdUsername.top = new FormAttachment(wAuthType, margin);
    wUsername.setLayoutData(fdUsername);

    // Password line
    Label wlPassword = new Label(wGeneralComp, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "ODataInputDialog.Password.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    fdlPassword.top = new FormAttachment(wUsername, margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(100, 0);
    fdPassword.top = new FormAttachment(wUsername, margin);
    wPassword.setLayoutData(fdPassword);

    // Token line
    Label wlToken = new Label(wGeneralComp, SWT.RIGHT);
    wlToken.setText(BaseMessages.getString(PKG, "ODataInputDialog.Token.Label"));
    PropsUi.setLook(wlToken);
    FormData fdlToken = new FormData();
    fdlToken.left = new FormAttachment(0, 0);
    fdlToken.right = new FormAttachment(middle, -margin);
    fdlToken.top = new FormAttachment(wPassword, margin);
    wlToken.setLayoutData(fdlToken);
    wToken = new PasswordTextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wToken);
    wToken.addModifyListener(lsMod);
    FormData fdToken = new FormData();
    fdToken.left = new FormAttachment(middle, 0);
    fdToken.right = new FormAttachment(100, 0);
    fdToken.top = new FormAttachment(wPassword, margin);
    wToken.setLayoutData(fdToken);

    // EntitySet line
    Label wlEntitySet = new Label(wGeneralComp, SWT.RIGHT);
    wlEntitySet.setText(BaseMessages.getString(PKG, "ODataInputDialog.EntitySet.Label"));
    PropsUi.setLook(wlEntitySet);
    FormData fdlEntitySet = new FormData();
    fdlEntitySet.left = new FormAttachment(0, 0);
    fdlEntitySet.right = new FormAttachment(middle, -margin);
    fdlEntitySet.top = new FormAttachment(wToken, margin);
    wlEntitySet.setLayoutData(fdlEntitySet);

    Button wGetEntitySets = new Button(wGeneralComp, SWT.PUSH);
    wGetEntitySets.setText(BaseMessages.getString(PKG, "ODataInputDialog.Button.GetEntitySets"));
    wGetEntitySets.addListener(SWT.Selection, e -> getEntitySets());
    FormData fdGetEntitySets = new FormData();
    fdGetEntitySets.right = new FormAttachment(100, 0);
    fdGetEntitySets.top = new FormAttachment(wToken, margin);
    wGetEntitySets.setLayoutData(fdGetEntitySets);

    wEntitySet = new ComboVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEntitySet);
    wEntitySet.addModifyListener(lsMod);
    FormData fdEntitySet = new FormData();
    fdEntitySet.left = new FormAttachment(middle, 0);
    fdEntitySet.right = new FormAttachment(wGetEntitySets, -margin);
    fdEntitySet.top = new FormAttachment(wToken, margin);
    wEntitySet.setLayoutData(fdEntitySet);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);
    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ////////////////////////
    // START OF QUERY TAB
    // ////////////////////////
    CTabItem wQueryTab = new CTabItem(wTabFolder, SWT.NONE);
    wQueryTab.setText(BaseMessages.getString(PKG, "ODataInputDialog.QueryTab.Title"));
    Composite wQueryComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wQueryComp);
    FormLayout queryLayout = new FormLayout();
    queryLayout.marginWidth = margin;
    queryLayout.marginHeight = margin;
    wQueryComp.setLayout(queryLayout);

    // Select line
    Label wlQuerySelect = new Label(wQueryComp, SWT.RIGHT);
    wlQuerySelect.setText(BaseMessages.getString(PKG, "ODataInputDialog.QuerySelect.Label"));
    PropsUi.setLook(wlQuerySelect);
    FormData fdlQuerySelect = new FormData();
    fdlQuerySelect.left = new FormAttachment(0, 0);
    fdlQuerySelect.right = new FormAttachment(middle, -margin);
    fdlQuerySelect.top = new FormAttachment(0, margin);
    wlQuerySelect.setLayoutData(fdlQuerySelect);
    wQuerySelect = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wQuerySelect);
    wQuerySelect.addModifyListener(lsMod);
    FormData fdQuerySelect = new FormData();
    fdQuerySelect.left = new FormAttachment(middle, 0);
    fdQuerySelect.right = new FormAttachment(100, 0);
    fdQuerySelect.top = new FormAttachment(0, margin);
    wQuerySelect.setLayoutData(fdQuerySelect);

    // Filter line
    Label wlQueryFilter = new Label(wQueryComp, SWT.RIGHT);
    wlQueryFilter.setText(BaseMessages.getString(PKG, "ODataInputDialog.QueryFilter.Label"));
    PropsUi.setLook(wlQueryFilter);
    FormData fdlQueryFilter = new FormData();
    fdlQueryFilter.left = new FormAttachment(0, 0);
    fdlQueryFilter.right = new FormAttachment(middle, -margin);
    fdlQueryFilter.top = new FormAttachment(wQuerySelect, margin);
    wlQueryFilter.setLayoutData(fdlQueryFilter);
    wQueryFilter = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wQueryFilter);
    wQueryFilter.addModifyListener(lsMod);
    FormData fdQueryFilter = new FormData();
    fdQueryFilter.left = new FormAttachment(middle, 0);
    fdQueryFilter.right = new FormAttachment(100, 0);
    fdQueryFilter.top = new FormAttachment(wQuerySelect, margin);
    wQueryFilter.setLayoutData(fdQueryFilter);

    // OrderBy line
    Label wlQueryOrder = new Label(wQueryComp, SWT.RIGHT);
    wlQueryOrder.setText(BaseMessages.getString(PKG, "ODataInputDialog.QueryOrder.Label"));
    PropsUi.setLook(wlQueryOrder);
    FormData fdlQueryOrder = new FormData();
    fdlQueryOrder.left = new FormAttachment(0, 0);
    fdlQueryOrder.right = new FormAttachment(middle, -margin);
    fdlQueryOrder.top = new FormAttachment(wQueryFilter, margin);
    wlQueryOrder.setLayoutData(fdlQueryOrder);
    wQueryOrder = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wQueryOrder);
    wQueryOrder.addModifyListener(lsMod);
    FormData fdQueryOrder = new FormData();
    fdQueryOrder.left = new FormAttachment(middle, 0);
    fdQueryOrder.right = new FormAttachment(100, 0);
    fdQueryOrder.top = new FormAttachment(wQueryFilter, margin);
    wQueryOrder.setLayoutData(fdQueryOrder);

    // Top line
    Label wlQueryTop = new Label(wQueryComp, SWT.RIGHT);
    wlQueryTop.setText(BaseMessages.getString(PKG, "ODataInputDialog.QueryTop.Label"));
    PropsUi.setLook(wlQueryTop);
    FormData fdlQueryTop = new FormData();
    fdlQueryTop.left = new FormAttachment(0, 0);
    fdlQueryTop.right = new FormAttachment(middle, -margin);
    fdlQueryTop.top = new FormAttachment(wQueryOrder, margin);
    wlQueryTop.setLayoutData(fdlQueryTop);
    wQueryTop = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wQueryTop);
    wQueryTop.addModifyListener(lsMod);
    FormData fdQueryTop = new FormData();
    fdQueryTop.left = new FormAttachment(middle, 0);
    fdQueryTop.right = new FormAttachment(100, 0);
    fdQueryTop.top = new FormAttachment(wQueryOrder, margin);
    wQueryTop.setLayoutData(fdQueryTop);

    // Skip line
    Label wlQuerySkip = new Label(wQueryComp, SWT.RIGHT);
    wlQuerySkip.setText(BaseMessages.getString(PKG, "ODataInputDialog.QuerySkip.Label"));
    PropsUi.setLook(wlQuerySkip);
    FormData fdlQuerySkip = new FormData();
    fdlQuerySkip.left = new FormAttachment(0, 0);
    fdlQuerySkip.right = new FormAttachment(middle, -margin);
    fdlQuerySkip.top = new FormAttachment(wQueryTop, margin);
    wlQuerySkip.setLayoutData(fdlQuerySkip);
    wQuerySkip = new TextVar(variables, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wQuerySkip);
    wQuerySkip.addModifyListener(lsMod);
    FormData fdQuerySkip = new FormData();
    fdQuerySkip.left = new FormAttachment(middle, 0);
    fdQuerySkip.right = new FormAttachment(100, 0);
    fdQuerySkip.top = new FormAttachment(wQueryTop, margin);
    wQuerySkip.setLayoutData(fdQuerySkip);

    FormData fdQueryComp = new FormData();
    fdQueryComp.left = new FormAttachment(0, 0);
    fdQueryComp.right = new FormAttachment(100, 0);
    fdQueryComp.top = new FormAttachment(0, 0);
    fdQueryComp.bottom = new FormAttachment(100, 0);
    wQueryComp.setLayoutData(fdQueryComp);
    wQueryComp.layout();
    wQueryTab.setControl(wQueryComp);

    // ////////////////////////
    // START OF FIELDS TAB
    // ////////////////////////
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "ODataInputDialog.FieldsTab.Title"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = margin;
    fieldsLayout.marginHeight = margin;
    wFieldsComp.setLayout(fieldsLayout);

    Button wGetFields = new Button(wFieldsComp, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "ODataInputDialog.Button.GetFields"));
    wGetFields.addListener(SWT.Selection, e -> getFields());
    FormData fdGetFields = new FormData();
    fdGetFields.right = new FormAttachment(100, 0);
    fdGetFields.top = new FormAttachment(0, margin);
    wGetFields.setLayoutData(fdGetFields);

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ODataInputDialog.Fields.Column.HopName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ODataInputDialog.Fields.Column.ODataPath"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ODataInputDialog.Fields.Column.HopType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ODataInputDialog.Fields.Column.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3)
        };

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            1,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(wGetFields, margin);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);
    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    // Layout Tab Folder
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    // Populate data
    getData();
    input.setChanged(changed);

    shell.open();
    Point size = shell.computeSize(SWT.DEFAULT, SWT.DEFAULT);
    shell.setSize(Math.max(600, size.x), Math.max(500, size.y));
    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }
    return transformName;
  }

  private void getData() {
    wUrl.setText(Const.NVL(input.getUrl(), ""));
    wEntitySet.setText(Const.NVL(input.getEntitySet(), ""));
    wAuthType.setText(ODataAuthType.lookupCode(input.getAuthType()).getDescription());
    wUsername.setText(Const.NVL(input.getUsername(), ""));
    wPassword.setText(Const.NVL(input.getPassword(), ""));
    wToken.setText(Const.NVL(input.getToken(), ""));

    wQuerySelect.setText(Const.NVL(input.getQuerySelect(), ""));
    wQueryFilter.setText(Const.NVL(input.getQueryFilter(), ""));
    wQueryOrder.setText(Const.NVL(input.getQueryOrder(), ""));
    wQueryTop.setText(Const.NVL(input.getQueryTop(), ""));
    wQuerySkip.setText(Const.NVL(input.getQuerySkip(), ""));

    for (int i = 0; i < input.getFields().size(); i++) {
      ODataField field = input.getFields().get(i);
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getPath(), ""));
      item.setText(3, Const.NVL(ValueMetaFactory.getValueMetaName(field.getType()), ""));
      item.setText(4, Const.NVL(field.getFormat(), ""));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
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
    transformName = wTransformName.getText();

    input.setUrl(wUrl.getText());
    input.setEntitySet(wEntitySet.getText());
    input.setAuthType(ODataAuthType.lookupDescription(wAuthType.getText()).getCode());
    input.setUsername(wUsername.getText());
    input.setPassword(wPassword.getText());
    input.setToken(wToken.getText());

    input.setQuerySelect(wQuerySelect.getText());
    input.setQueryFilter(wQueryFilter.getText());
    input.setQueryOrder(wQueryOrder.getText());
    input.setQueryTop(wQueryTop.getText());
    input.setQuerySkip(wQuerySkip.getText());

    input.getFields().clear();
    int nrFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      String name = item.getText(1);
      String path = item.getText(2);
      int type = ValueMetaFactory.getIdForValueMeta(item.getText(3));
      String format = item.getText(4);
      input.getFields().add(new ODataField(name, path, type, format));
    }

    input.setChanged();
    dispose();
  }

  private void getEntitySets() {
    String urlStr = wUrl.getText();
    if (Utils.isEmpty(urlStr)) {
      return;
    }
    urlStr = variables.resolve(urlStr);
    try {
      HttpClientManager.HttpClientBuilderFacade builder =
          HttpClientManager.getInstance().createBuilder();
      ODataAuthType authType = ODataAuthType.lookupDescription(wAuthType.getText());
      if (ODataAuthType.BASIC == authType && !Utils.isEmpty(wUsername.getText())) {
        builder.setCredentials(
            variables.resolve(wUsername.getText()), variables.resolve(wPassword.getText()));
      }
      try (org.apache.hc.client5.http.impl.classic.CloseableHttpClient client = builder.build()) {
        HttpGet get = new HttpGet(urlStr);
        get.setHeader("Accept", "application/json");
        if (ODataAuthType.BEARER == authType && !Utils.isEmpty(wToken.getText())) {
          get.setHeader("Authorization", "Bearer " + variables.resolve(wToken.getText()));
        }
        try (CloseableHttpResponse response = client.execute(get)) {
          int code = response.getCode();
          if (code != 200) {
            throw new HopException("HTTP " + code);
          }
          String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
          ObjectMapper mapper = new ObjectMapper();
          JsonNode root = mapper.readTree(body);
          List<String> list = new ArrayList<>();
          if (root.has("value")) {
            JsonNode val = root.get("value");
            if (val.isArray()) {
              for (JsonNode it : val) {
                if (it.has("name")) {
                  list.add(it.get("name").asText());
                }
              }
            }
          } else if (root.has("d")) {
            JsonNode d = root.get("d");
            if (d.has("EntitySets") && d.get("EntitySets").isArray()) {
              for (JsonNode it : d.get("EntitySets")) {
                list.add(it.asText());
              }
            } else if (d.isArray()) {
              for (JsonNode it : d) {
                if (it.has("name")) {
                  list.add(it.get("name").asText());
                }
              }
            } else {
              d.fieldNames()
                  .forEachRemaining(
                      name -> {
                        if (!name.startsWith("__")) {
                          list.add(name);
                        }
                      });
            }
          }
          if (!list.isEmpty()) {
            wEntitySet.setItems(list.toArray(new String[0]));
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error fetching entity sets", e);
    }
  }

  private void getFields() {
    String urlStr = wUrl.getText();
    String entitySetName = wEntitySet.getText();
    if (Utils.isEmpty(urlStr) || Utils.isEmpty(entitySetName)) {
      return;
    }
    urlStr = variables.resolve(urlStr);
    entitySetName = variables.resolve(entitySetName);
    if (!urlStr.endsWith("/")) {
      urlStr += "/";
    }
    String metadataUrl = urlStr + "$metadata";
    try {
      HttpClientManager.HttpClientBuilderFacade builder =
          HttpClientManager.getInstance().createBuilder();
      ODataAuthType authType = ODataAuthType.lookupDescription(wAuthType.getText());
      if (ODataAuthType.BASIC == authType && !Utils.isEmpty(wUsername.getText())) {
        builder.setCredentials(
            variables.resolve(wUsername.getText()), variables.resolve(wPassword.getText()));
      }
      try (org.apache.hc.client5.http.impl.classic.CloseableHttpClient client = builder.build()) {
        HttpGet get = new HttpGet(metadataUrl);
        if (ODataAuthType.BEARER == authType && !Utils.isEmpty(wToken.getText())) {
          get.setHeader("Authorization", "Bearer " + variables.resolve(wToken.getText()));
        }
        try (CloseableHttpResponse response = client.execute(get)) {
          int code = response.getCode();
          if (code != 200) {
            throw new HopException("HTTP " + code);
          }
          String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

          javax.xml.parsers.DocumentBuilderFactory factory =
              javax.xml.parsers.DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          javax.xml.parsers.DocumentBuilder db = factory.newDocumentBuilder();
          org.w3c.dom.Document doc =
              db.parse(new java.io.ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));

          org.w3c.dom.NodeList entitySets = doc.getElementsByTagNameNS("*", "EntitySet");
          String entityType = null;
          for (int i = 0; i < entitySets.getLength(); i++) {
            org.w3c.dom.Element set = (org.w3c.dom.Element) entitySets.item(i);
            if (entitySetName.equalsIgnoreCase(set.getAttribute("Name"))) {
              entityType = set.getAttribute("EntityType");
              break;
            }
          }

          if (entityType == null) {
            throw new HopException("Entity Set not found in metadata: " + entitySetName);
          }

          String entityTypeName = entityType;
          int lastDot = entityType.lastIndexOf(".");
          if (lastDot != -1) {
            entityTypeName = entityType.substring(lastDot + 1);
          }

          org.w3c.dom.NodeList entityTypes = doc.getElementsByTagNameNS("*", "EntityType");
          org.w3c.dom.Element targetEntityType = null;
          for (int i = 0; i < entityTypes.getLength(); i++) {
            org.w3c.dom.Element type = (org.w3c.dom.Element) entityTypes.item(i);
            if (entityTypeName.equalsIgnoreCase(type.getAttribute("Name"))) {
              targetEntityType = type;
              break;
            }
          }

          if (targetEntityType == null) {
            throw new HopException(
                "EntityType definition not found in metadata: " + entityTypeName);
          }

          org.w3c.dom.NodeList properties =
              targetEntityType.getElementsByTagNameNS("*", "Property");
          wFields.table.removeAll();
          for (int i = 0; i < properties.getLength(); i++) {
            org.w3c.dom.Element prop = (org.w3c.dom.Element) properties.item(i);
            String name = prop.getAttribute("Name");
            String type = prop.getAttribute("Type");

            String hopType = "String";
            if (type != null) {
              if (type.contains("Int16")
                  || type.contains("Int32")
                  || type.contains("Int64")
                  || type.contains("Byte")) {
                hopType = "Integer";
              } else if (type.contains("Decimal")
                  || type.contains("Double")
                  || type.contains("Single")) {
                hopType = "Number";
              } else if (type.contains("Boolean")) {
                hopType = "Boolean";
              } else if (type.contains("DateTime") || type.contains("Date")) {
                hopType = "Date";
              }
            }

            TableItem item = new TableItem(wFields.table, SWT.NONE);
            item.setText(1, name);
            item.setText(2, name);
            item.setText(3, hopType);
            item.setText(4, "");
          }
          wFields.removeEmptyRows();
          wFields.setRowNums();
          wFields.optWidth(true);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error fetching fields metadata", e);
    }
  }
}
