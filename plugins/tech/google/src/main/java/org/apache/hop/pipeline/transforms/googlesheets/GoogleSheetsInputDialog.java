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
package org.apache.hop.pipeline.transforms.googlesheets;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class GoogleSheetsInputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = GoogleSheetsInputMeta.class;

  private final GoogleSheetsInputMeta meta;

  private Label wlTestServiceAccountInfo;
  private TextVar wPrivateKeyStore;
  private TextVar wAppname;
  private TextVar wTimeout;
  private TextVar wImpersonation;
  private TextVar wSpreadSheetKey;
  private TextVar wWorksheetId;
  private TextVar wSampleFields;
  private TableView wFields;
  private TextVar wProxyHost;
  private TextVar wProxyPort;
  private static final String C_BROWSE_BUTTON = "System.Button.Browse";
  private static final String C_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  public GoogleSheetsInputDialog(
      Shell parent,
      IVariables variables,
      GoogleSheetsInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.meta = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GoogleSheetsInput.transform.Name"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = meta.hasChanged();

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    /*
     * BEGIN Service Account Tab
     */
    CTabItem serviceAccountTab = new CTabItem(tabFolder, SWT.NONE);
    serviceAccountTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.ServiceAccount"));

    Composite serviceAccountComposite = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(serviceAccountComposite);

    FormLayout serviceAccountLayout = new FormLayout();
    serviceAccountLayout.marginWidth = PropsUi.getFormMargin();
    serviceAccountLayout.marginHeight = PropsUi.getFormMargin();
    serviceAccountComposite.setLayout(serviceAccountLayout);

    // privateKey json - Label
    Label wlPrivateKeyStore = new Label(serviceAccountComposite, SWT.RIGHT);
    wlPrivateKeyStore.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.PrivateKeyStore"));
    PropsUi.setLook(wlPrivateKeyStore);
    FormData fdlPrivateKeyStore = new FormData();
    fdlPrivateKeyStore.top = new FormAttachment(0, margin);
    fdlPrivateKeyStore.left = new FormAttachment(0, 0);
    fdlPrivateKeyStore.right = new FormAttachment(middle, -margin);
    wlPrivateKeyStore.setLayoutData(fdlPrivateKeyStore);

    // privateKey - Button
    Button wbPrivateKeyButton = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbPrivateKeyButton);
    wbPrivateKeyButton.setText(BaseMessages.getString(C_BROWSE_BUTTON));
    FormData fdbPrivateKeyStore = new FormData();
    fdbPrivateKeyStore.top = new FormAttachment(0, margin);
    fdbPrivateKeyStore.right = new FormAttachment(100, 0);
    wbPrivateKeyButton.setLayoutData(fdbPrivateKeyStore);
    wbPrivateKeyButton.addListener(SWT.Selection, e -> selectPrivateKeyStoreFile());

    // privatekey - Text
    wPrivateKeyStore =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrivateKeyStore);
    FormData fdPrivateKeyStore = new FormData();
    fdPrivateKeyStore.top = new FormAttachment(0, margin);
    fdPrivateKeyStore.left = new FormAttachment(middle, 0);
    fdPrivateKeyStore.right = new FormAttachment(wbPrivateKeyButton, -margin);
    wPrivateKeyStore.setLayoutData(fdPrivateKeyStore);

    // Appname - Label
    Label appNameLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    appNameLabel.setText(
        BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.ApplicationName.Label"));
    PropsUi.setLook(appNameLabel);
    FormData appNameLabelForm = new FormData();
    appNameLabelForm.top = new FormAttachment(wbPrivateKeyButton, margin);
    appNameLabelForm.left = new FormAttachment(0, 0);
    appNameLabelForm.right = new FormAttachment(middle, -margin);
    appNameLabel.setLayoutData(appNameLabelForm);

    // Appname - Text
    wAppname = new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAppname);
    FormData appNameData = new FormData();
    appNameData.top = new FormAttachment(wbPrivateKeyButton, margin);
    appNameData.left = new FormAttachment(middle, 0);
    appNameData.right = new FormAttachment(wbPrivateKeyButton, -margin);
    wAppname.setLayoutData(appNameData);

    // Timeout - Label
    Label timeoutLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    timeoutLabel.setText(BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.TimeOut.Label"));
    PropsUi.setLook(timeoutLabel);
    FormData timeoutLabelForm = new FormData();
    timeoutLabelForm.top = new FormAttachment(wAppname, margin);
    timeoutLabelForm.left = new FormAttachment(0, 0);
    timeoutLabelForm.right = new FormAttachment(middle, -margin);
    timeoutLabel.setLayoutData(timeoutLabelForm);

    // timeout - Text
    wTimeout = new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTimeout);
    FormData timeoutData = new FormData();
    timeoutData.top = new FormAttachment(wAppname, margin);
    timeoutData.left = new FormAttachment(middle, 0);
    timeoutData.right = new FormAttachment(wbPrivateKeyButton, -margin);
    wTimeout.setLayoutData(timeoutData);

    // Impersonation - Label
    Label impersonationLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    impersonationLabel.setText(
        BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.ImpersonationAccount"));
    PropsUi.setLook(impersonationLabel);
    FormData impersonationLabelForm = new FormData();
    impersonationLabelForm.top = new FormAttachment(wTimeout, margin);
    impersonationLabelForm.left = new FormAttachment(0, 0);
    impersonationLabelForm.right = new FormAttachment(middle, -margin);
    impersonationLabel.setLayoutData(impersonationLabelForm);

    // impersonation - Text
    wImpersonation =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wImpersonation);
    FormData impersonationData = new FormData();
    impersonationData.top = new FormAttachment(wTimeout, margin);
    impersonationData.left = new FormAttachment(middle, 0);
    impersonationData.right = new FormAttachment(wbPrivateKeyButton, -margin);
    wImpersonation.setLayoutData(impersonationData);

    // test service - Button
    Button wbTestServiceAccount = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTestServiceAccount);
    wbTestServiceAccount.setText(
        BaseMessages.getString(PKG, "GoogleSheetsDialog.Button.TestConnection"));
    FormData fdbTestServiceAccount = new FormData();
    fdbTestServiceAccount.top = new FormAttachment(wImpersonation, margin);
    fdbTestServiceAccount.left = new FormAttachment(0, 0);
    wbTestServiceAccount.setLayoutData(fdbTestServiceAccount);
    wbTestServiceAccount.addListener(SWT.Selection, e -> testServiceAccount());

    wlTestServiceAccountInfo = new Label(serviceAccountComposite, SWT.LEFT);
    PropsUi.setLook(wlTestServiceAccountInfo);
    FormData fdlTestServiceAccountInfo = new FormData();
    fdlTestServiceAccountInfo.top = new FormAttachment(wImpersonation, margin);
    fdlTestServiceAccountInfo.left = new FormAttachment(middle, 0);
    fdlTestServiceAccountInfo.right = new FormAttachment(100, 0);
    wlTestServiceAccountInfo.setLayoutData(fdlTestServiceAccountInfo);

    FormData fdServiceAccountComposite = new FormData();
    fdServiceAccountComposite.left = new FormAttachment(0, 0);
    fdServiceAccountComposite.top = new FormAttachment(0, 0);
    fdServiceAccountComposite.right = new FormAttachment(100, 0);
    fdServiceAccountComposite.bottom = new FormAttachment(100, 0);
    serviceAccountComposite.setLayoutData(fdServiceAccountComposite);

    serviceAccountComposite.layout();
    serviceAccountTab.setControl(serviceAccountComposite);

    /*
     * END Service Account Tab
     */

    /*
     * BEGIN Spreadsheet Tab
     */
    CTabItem spreadsheetTab = new CTabItem(tabFolder, SWT.NONE);
    spreadsheetTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.Spreadsheet"));

    Composite spreadsheetComposite = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(spreadsheetComposite);

    FormLayout spreadsheetLayout = new FormLayout();
    spreadsheetLayout.marginWidth = PropsUi.getFormMargin();
    spreadsheetLayout.marginHeight = PropsUi.getFormMargin();
    spreadsheetComposite.setLayout(spreadsheetLayout);

    // spreadsheetKey - Label
    Label wlSpreadSheetKey = new Label(spreadsheetComposite, SWT.RIGHT);
    wlSpreadSheetKey.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.SpreadsheetKey"));
    PropsUi.setLook(wlSpreadSheetKey);
    FormData fdlSpreadSheetKey = new FormData();
    fdlSpreadSheetKey.top = new FormAttachment(0, margin);
    fdlSpreadSheetKey.left = new FormAttachment(0, 0);
    fdlSpreadSheetKey.right = new FormAttachment(middle, -margin);
    wlSpreadSheetKey.setLayoutData(fdlSpreadSheetKey);

    // spreadsheetKey - Button
    Button wbSpreadSheetKey = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbSpreadSheetKey.setText(BaseMessages.getString(C_BROWSE_BUTTON));
    PropsUi.setLook(wbSpreadSheetKey);
    FormData fdbSpreadSheetKey = new FormData();
    fdbSpreadSheetKey.top = new FormAttachment(0, margin);
    fdbSpreadSheetKey.right = new FormAttachment(100, 0);
    wbSpreadSheetKey.setLayoutData(fdbSpreadSheetKey);
    wbSpreadSheetKey.addListener(SWT.Selection, e -> selectSpreadSheet());

    // spreadsheetKey - Text
    wSpreadSheetKey =
        new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSpreadSheetKey);
    FormData fdSpreadSheetKey = new FormData();
    fdSpreadSheetKey.top = new FormAttachment(0, margin);
    fdSpreadSheetKey.left = new FormAttachment(middle, 0);
    fdSpreadSheetKey.right = new FormAttachment(wbSpreadSheetKey, -margin);
    wSpreadSheetKey.setLayoutData(fdSpreadSheetKey);

    // worksheetId - Label
    Label wlWorksheetId = new Label(spreadsheetComposite, SWT.RIGHT);
    wlWorksheetId.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.WorksheetId"));
    PropsUi.setLook(wlWorksheetId);
    FormData fdlWorksheetId = new FormData();
    fdlWorksheetId.top = new FormAttachment(wbSpreadSheetKey, margin);
    fdlWorksheetId.left = new FormAttachment(0, 0);
    fdlWorksheetId.right = new FormAttachment(middle, -margin);
    wlWorksheetId.setLayoutData(fdlWorksheetId);

    // worksheetId - Button
    Button wbWorksheetId = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbWorksheetId.setText(BaseMessages.getString(C_BROWSE_BUTTON));
    PropsUi.setLook(wbWorksheetId);
    FormData fdbWorksheetId = new FormData();
    fdbWorksheetId.top = new FormAttachment(wbSpreadSheetKey, margin);
    fdbWorksheetId.right = new FormAttachment(100, 0);
    wbWorksheetId.setLayoutData(fdbWorksheetId);
    wbWorksheetId.addListener(SWT.Selection, e -> selectWorksheet());

    // worksheetId - Text
    wWorksheetId = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wWorksheetId);
    FormData fdWorksheetId = new FormData();
    fdWorksheetId.top = new FormAttachment(wbSpreadSheetKey, margin);
    fdWorksheetId.left = new FormAttachment(middle, 0);
    fdWorksheetId.right = new FormAttachment(wbWorksheetId, -margin);
    wWorksheetId.setLayoutData(fdWorksheetId);

    FormData fdSpreadsheetComposite = new FormData();
    fdSpreadsheetComposite.left = new FormAttachment(0, 0);
    fdSpreadsheetComposite.top = new FormAttachment(0, 0);
    fdSpreadsheetComposite.right = new FormAttachment(100, 0);
    fdSpreadsheetComposite.bottom = new FormAttachment(100, 0);
    spreadsheetComposite.setLayoutData(fdSpreadsheetComposite);

    spreadsheetComposite.layout();
    spreadsheetTab.setControl(spreadsheetComposite);
    /*
     * END Spreadsheet Tab
     */

    /*
     * BEGIN Proxy tab
     */

    CTabItem proxyTab = new CTabItem(tabFolder, SWT.NONE);
    proxyTab.setText(BaseMessages.getString(PKG, "GoogleSheetsInputDialog.Proxy"));

    Composite proxyComposite = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(proxyComposite);

    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = PropsUi.getFormMargin();
    proxyLayout.marginHeight = PropsUi.getFormMargin();
    proxyComposite.setLayout(proxyLayout);

    Label wlProxyHost = new Label(proxyComposite, SWT.RIGHT);
    wlProxyHost.setText(BaseMessages.getString(PKG, "GoogleSheetsInputDialog.ProxyHost"));
    PropsUi.setLook(wlProxyHost);
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.left = new FormAttachment(0, 0);
    fdlProxyHost.right = new FormAttachment(middle, -margin);
    fdlProxyHost.top = new FormAttachment(0, 0);
    wlProxyHost.setLayoutData(fdlProxyHost);

    wProxyHost = new TextVar(variables, proxyComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProxyHost);
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment(middle, 0);
    fdProxyHost.right = new FormAttachment(100, 0);
    fdProxyHost.top = new FormAttachment(0, 0);
    wProxyHost.setLayoutData(fdProxyHost);

    Label wlProxyPort = new Label(proxyComposite, SWT.RIGHT);
    wlProxyPort.setText(BaseMessages.getString(PKG, "GoogleSheetsInputDialog.ProxyPort"));
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.left = new FormAttachment(0, 0);
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    fdlProxyPort.top = new FormAttachment(wlProxyHost, margin);
    wlProxyPort.setLayoutData(fdlProxyPort);
    PropsUi.setLook(wlProxyPort);

    wProxyPort = new TextVar(variables, proxyComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProxyPort);
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment(middle, 0);
    fdProxyPort.right = new FormAttachment(100, 0);
    fdProxyPort.top = new FormAttachment(wlProxyHost, margin);
    wProxyPort.setLayoutData(fdProxyPort);

    proxyTab.setControl(proxyComposite);
    proxyComposite.layout();

    /*
     * END Proxy tab
     */

    /*
     * BEGIN Fields Tab
     */
    // Nb Sample Fields - Label
    CTabItem fieldsTab = new CTabItem(tabFolder, SWT.NONE);
    fieldsTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.Fields"));

    Composite fieldsComposite = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(fieldsComposite);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();
    fieldsComposite.setLayout(fieldsLayout);

    Label wlSampleFieldsLabel = new Label(fieldsComposite, SWT.RIGHT);
    wlSampleFieldsLabel.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.NrOfSampleLines"));
    PropsUi.setLook(wlSampleFieldsLabel);
    FormData fdlSampleFields = new FormData();
    fdlSampleFields.top = new FormAttachment(0, margin);
    fdlSampleFields.left = new FormAttachment(0, 0);
    fdlSampleFields.right = new FormAttachment(middle, -margin);
    wlSampleFieldsLabel.setLayoutData(fdlSampleFields);

    // sampleFields - Text
    wSampleFields = new TextVar(variables, fieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSampleFields);
    FormData fdSampleFields = new FormData();
    fdSampleFields.top = new FormAttachment(0, margin);
    fdSampleFields.left = new FormAttachment(middle, 0);
    fdSampleFields.right = new FormAttachment(100, -margin);
    wSampleFields.setLayoutData(fdSampleFields);

    wGet = new Button(fieldsComposite, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> getSpreadsheetFields());

    // Fields
    ColumnInfo[] columnInformation =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.TrimType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.trimTypeDesc)
        };

    wFields =
        new TableView(
            variables,
            fieldsComposite,
            SWT.FULL_SELECTION | SWT.MULTI,
            columnInformation,
            1,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(wSampleFields, margin);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComposite = new FormData();
    fdFieldsComposite.left = new FormAttachment(0, 0);
    fdFieldsComposite.top = new FormAttachment(0, 0);
    fdFieldsComposite.right = new FormAttachment(100, 0);
    fdFieldsComposite.bottom = new FormAttachment(100, 0);
    fieldsComposite.setLayoutData(fdFieldsComposite);

    setButtonPositions(new Button[] {wGet}, margin, null);

    fieldsComposite.layout();
    fieldsTab.setControl(fieldsComposite);
    /*
     * END Fields Tab
     */

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    tabFolder.setLayoutData(fdTabFolder);

    tabFolder.setSelection(0);

    getData(meta);
    meta.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void selectPrivateKeyStoreFile() {
    String filename =
        BaseDialog.presentFileDialog(
            shell,
            new String[] {"*json", "*"},
            new String[] {"credential JSON file", "All Files"},
            true);
    if (filename != null) {
      wPrivateKeyStore.setText(filename);
      meta.setChanged();
    }
  }

  private void testServiceAccount() {
    try {
      NetHttpTransport netHttpTransport =
          GoogleSheetsConnectionFactory.newTransport(meta.getProxyHost(), meta.getProxyPort());
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      // Test by building a drive connection
      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()),
              variables);
      //
      new Drive.Builder(
              netHttpTransport,
              jsonFactory,
              GoogleSheetsCredentials.setHttpTimeout(
                  credential, variables.resolve(meta.getTimeout())))
          .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
          .build();
      wlTestServiceAccountInfo.setText(
          BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.TestConnectionSuccess.Message"));
    } catch (Exception error) {
      wlTestServiceAccountInfo.setText(
          BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.TestConnectionFailed.Message"));
    }
  }

  private void selectSpreadSheet() {
    try {
      NetHttpTransport netHttpTransport =
          GoogleSheetsConnectionFactory.newTransport(meta.getProxyHost(), meta.getProxyPort());
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      String scope = "https://www.googleapis.com/auth/drive.readonly";
      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()),
              variables);
      //
      Drive service =
          new Drive.Builder(
                  netHttpTransport,
                  jsonFactory,
                  GoogleSheetsCredentials.setHttpTimeout(
                      credential, variables.resolve(meta.getTimeout())))
              .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
              .build();

      FileList result =
          service
              .files()
              .list()
              .setSupportsAllDrives(true)
              .setIncludeItemsFromAllDrives(true)
              .setQ("mimeType='application/vnd.google-apps.spreadsheet'")
              .setPageSize(100)
              .setFields("nextPageToken, files(id, name)")
              .execute();
      List<File> spreadsheets = result.getFiles();
      int selectedSpreadsheet = -1;
      int i = 0;
      String[] titles = new String[spreadsheets.size()];
      for (File spreadsheet : spreadsheets) {
        titles[i] = spreadsheet.getName() + " - " + spreadsheet.getId() + ")";

        if (spreadsheet.getId().equals(wSpreadSheetKey.getText())) {
          selectedSpreadsheet = i;
        }
        i++;
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(shell, titles, "Spreadsheets", "Select a Spreadsheet.");
      if (selectedSpreadsheet > -1) {
        esd.setSelectedNrs(new int[] {selectedSpreadsheet});
      }
      String s = esd.open();
      if (s != null) {
        if (esd.getSelectionIndeces().length > 0) {
          selectedSpreadsheet = esd.getSelectionIndeces()[0];
          File spreadsheet = spreadsheets.get(selectedSpreadsheet);
          wSpreadSheetKey.setText(spreadsheet.getId());
        } else {
          wSpreadSheetKey.setText("");
        }
      }

    } catch (Exception err) {
      new ErrorDialog(shell, C_DIALOG_ERROR_TITLE, err.getMessage(), err);
    }
  }

  private void selectWorksheet() {
    try {

      NetHttpTransport netHttpTransport =
          GoogleSheetsConnectionFactory.newTransport(meta.getProxyHost(), meta.getProxyPort());
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()),
              variables);
      //
      Sheets service =
          new Sheets.Builder(
                  netHttpTransport,
                  jsonFactory,
                  GoogleSheetsCredentials.setHttpTimeout(
                      credential, variables.resolve(meta.getTimeout())))
              .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
              .build();
      Spreadsheet response1 =
          service
              .spreadsheets()
              .get(variables.resolve(wSpreadSheetKey.getText()))
              .setIncludeGridData(false)
              .execute();

      List<Sheet> worksheets = response1.getSheets();
      String[] names = new String[worksheets.size()];
      int selectedSheet = -1;
      for (int i = 0; i < worksheets.size(); i++) {
        Sheet sheet = worksheets.get(i);
        names[i] = sheet.getProperties().getTitle();
        if (sheet.getProperties().getTitle().endsWith("/" + wWorksheetId.getText())) {
          selectedSheet = i;
        }
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(shell, names, "Worksheets", "Select a Worksheet.");
      if (selectedSheet > -1) {
        esd.setSelectedNrs(new int[] {selectedSheet});
      }
      String s = esd.open();
      if (s != null) {
        if (esd.getSelectionIndeces().length > 0) {
          selectedSheet = esd.getSelectionIndeces()[0];
          Sheet sheet = worksheets.get(selectedSheet);
          String id = sheet.getProperties().getTitle();
          wWorksheetId.setText(id.substring(id.lastIndexOf("/") + 1));
        } else {
          wWorksheetId.setText("");
        }
      }

    } catch (Exception err) {
      new ErrorDialog(
          shell, BaseMessages.getString(PKG, C_DIALOG_ERROR_TITLE), err.getMessage(), err);
    }
  }

  private void getData(GoogleSheetsInputMeta meta) {
    if (!StringUtils.isEmpty(meta.getSpreadsheetKey())) {
      this.wSpreadSheetKey.setText(meta.getSpreadsheetKey());
    }
    if (!StringUtils.isEmpty(meta.getWorksheetId())) {
      this.wWorksheetId.setText(meta.getWorksheetId());
    }
    if (!StringUtils.isEmpty(meta.getJsonCredentialPath())) {
      this.wPrivateKeyStore.setText(meta.getJsonCredentialPath());
    }
    if (!StringUtils.isEmpty(meta.getTimeout())) {
      this.wTimeout.setText(meta.getTimeout());
    }
    if (!StringUtils.isEmpty(meta.getImpersonation())) {
      this.wImpersonation.setText(meta.getImpersonation());
    }
    if (!StringUtils.isEmpty(meta.getAppName())) {
      this.wAppname.setText(meta.getAppName());
    }
    if (!StringUtils.isEmpty(meta.getProxyHost())) {
      this.wProxyHost.setText(meta.getProxyHost());
    }
    if (!StringUtils.isEmpty(meta.getProxyPort())) {
      this.wProxyPort.setText(meta.getProxyPort());
    }
    this.wSampleFields.setText(Integer.toString(meta.getSampleFields()));

    wFields.clearAll();
    List<GoogleSheetsInputField> fields = meta.getInputFields();
    for (GoogleSheetsInputField field : fields) {

      TableItem item = new TableItem(wFields.table, SWT.NONE);

      item.setText(1, Const.NVL(field.getName(), ""));
      String type = field.getTypeDesc();
      String format = field.getFormat();
      String length = "" + field.getLength();
      String precision = "" + field.getPrecision();
      String currencySymbol = field.getCurrencySymbol();
      String groupSymbol = field.getGroupSymbol();
      String decimalSymbol = field.getDecimalSymbol();
      String trim = field.getTrimTypeDesc();

      if (type != null) {
        item.setText(2, type);
      }
      if (format != null) {
        item.setText(3, format);
      }
      if (!"-1".equals(length)) {
        item.setText(4, length);
      }
      if (!"-1".equals(precision)) {
        item.setText(5, precision);
      }
      if (currencySymbol != null) {
        item.setText(6, currencySymbol);
      }
      if (decimalSymbol != null) {
        item.setText(7, decimalSymbol);
      }
      if (groupSymbol != null) {
        item.setText(8, groupSymbol);
      }
      if (trim != null) {
        item.setText(9, trim);
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);

    meta.setChanged();
  }

  private void setData(GoogleSheetsInputMeta meta) {

    meta.setJsonCredentialPath(this.wPrivateKeyStore.getText());
    meta.setSpreadsheetKey(this.wSpreadSheetKey.getText());
    meta.setWorksheetId(this.wWorksheetId.getText());
    meta.setTimeout(this.wTimeout.getText());
    meta.setAppName(this.wAppname.getText());
    meta.setImpersonation(this.wImpersonation.getText());
    if (this.wSampleFields != null && !this.wSampleFields.getText().isEmpty()) {
      meta.setSampleFields(Integer.parseInt(this.wSampleFields.getText()));
    } else {
      meta.setSampleFields(100);
    }

    meta.setProxyHost(wProxyHost.getText());
    meta.setProxyPort(wProxyPort.getText());

    int nrNonEmptyFields = wFields.nrNonEmpty();

    List<GoogleSheetsInputField> googleSheetsInputFields = new ArrayList<>();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      GoogleSheetsInputField field = new GoogleSheetsInputField();

      int colnr = 1;
      field.setName(item.getText(colnr++));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(colnr++)));
      field.setFormat(item.getText(colnr++));
      field.setLength(Const.toInt(item.getText(colnr++), -1));
      field.setPrecision(Const.toInt(item.getText(colnr++), -1));
      field.setCurrencySymbol(item.getText(colnr++));
      field.setDecimalSymbol(item.getText(colnr++));
      field.setGroupSymbol(item.getText(colnr++));
      field.setTrimType(ValueMetaBase.getTrimTypeByDesc(item.getText(colnr++)));
      googleSheetsInputFields.add(field);
    }
    meta.setInputFields(googleSheetsInputFields);
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
    meta.setChanged();
  }

  private void cancel() {
    transformName = null;
    meta.setChanged(changed);
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText();
    setData(this.meta);
    dispose();
  }

  private static String getColumnName(int n) {
    // initialize output String as empty
    StringBuilder res = new StringBuilder();
    if (n == 0) {
      res.append('A');
    } else {
      while (n > 0) {
        // find index of next letter and concatenate the letter
        // to the solution

        // Here index 0 corresponds to 'A' and 25 corresponds to 'Z'
        int index = (n - 1) % 26;
        res.append((char) (index + 'A'));
        n = (n - 1) / 26;
      }
    }

    return res.reverse().toString();
  }

  private void getSpreadsheetFields() {
    try {
      GoogleSheetsInputMeta meta = new GoogleSheetsInputMeta();
      setData(meta);
      NetHttpTransport netHttpTransport =
          GoogleSheetsConnectionFactory.newTransport(meta.getProxyHost(), meta.getProxyPort());
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;
      wFields.table.removeAll();

      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()),
              variables);
      Sheets service =
          new Sheets.Builder(
                  netHttpTransport,
                  jsonFactory,
                  GoogleSheetsCredentials.setHttpTimeout(
                      credential, variables.resolve(meta.getTimeout())))
              .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
              .build();
      // Fill in sample in order to guess types

      String range = variables.resolve(meta.getWorksheetId()) + "!" + "1:1";
      ValueRange result =
          service
              .spreadsheets()
              .values()
              .get(variables.resolve(meta.getSpreadsheetKey()), range)
              .execute();
      List<List<Object>> values = result.getValues();
      if (values != null || !values.isEmpty()) {
        for (List row : values) {
          for (int j = 0; j < row.size(); j++) {
            TableItem item = new TableItem(wFields.table, SWT.NONE);
            item.setText(1, Const.trim(row.get(j).toString()));
            // Fill in sample in order to guess types ___ GoogleSheetsInputFields( String
            // fieldname, int position, int length )
            GoogleSheetsInputField sampleInputFields = new GoogleSheetsInputField();
            String columnsLetter = getColumnName(j + 1);
            logDebug("column:" + Integer.toString(j) + ")" + columnsLetter);
            Integer nbSampleFields = Integer.parseInt(variables.resolve(wSampleFields.getText()));

            String sampleRange =
                variables.resolve(meta.getWorksheetId())
                    + "!"
                    + columnsLetter
                    + "2:"
                    + columnsLetter
                    + variables.resolve(wSampleFields.getText());
            logDebug("Guess Fieds : Range : " + sampleRange);
            ValueRange sampleResult =
                service
                    .spreadsheets()
                    .values()
                    .get(variables.resolve(meta.getSpreadsheetKey()), sampleRange)
                    .execute();
            List<List<Object>> sampleValues = sampleResult.getValues();
            if (sampleValues != null) {
              int m = 0;
              String[] tmpSampleColumnValues = new String[sampleValues.size()];
              for (List sampleRow : sampleValues) {

                if (sampleRow != null
                    && !sampleRow.isEmpty()
                    && sampleRow.get(0) != null
                    && !sampleRow.get(0).toString().isEmpty()) {
                  String tmp = sampleRow.get(0).toString();
                  logDebug(Integer.toString(m) + ")" + tmp);
                  tmpSampleColumnValues[m] = tmp;
                  m++;
                } else {
                  logBasic("no sample values");
                }
              }
              String[] sampleColumnValues = new String[m];
              System.arraycopy(tmpSampleColumnValues, 0, sampleColumnValues, 0, m);
              sampleInputFields.setSamples(sampleColumnValues);
              sampleInputFields.guess();
              if (!StringUtils.isEmpty(sampleInputFields.getTypeDesc())) {
                item.setText(2, sampleInputFields.getTypeDesc());
              }
              if (!StringUtils.isEmpty(sampleInputFields.getFormat())) {
                item.setText(3, sampleInputFields.getFormat());
              }
              if (!StringUtils.isEmpty(Integer.toString(sampleInputFields.getPrecision()))) {
                item.setText(5, Integer.toString(sampleInputFields.getPrecision()));
              }
              if (!StringUtils.isEmpty(sampleInputFields.getCurrencySymbol())) {
                item.setText(6, sampleInputFields.getCurrencySymbol());
              }
              if (!StringUtils.isEmpty(sampleInputFields.getDecimalSymbol())) {
                item.setText(7, sampleInputFields.getDecimalSymbol());
              }
              if (!StringUtils.isEmpty(sampleInputFields.getGroupSymbol())) {
                item.setText(8, sampleInputFields.getGroupSymbol());
              }
              if (!StringUtils.isEmpty(sampleInputFields.getTrimTypeDesc())) {
                item.setText(9, sampleInputFields.getTrimTypeDesc());
              }
            } else {
              item.setText(2, "String");
            }
          }
        }
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth(true);
    } catch (Exception e) {
      new ErrorDialog(
          shell, BaseMessages.getString(PKG, C_DIALOG_ERROR_TITLE), "Error getting Fields", e);
    }
  }
}
