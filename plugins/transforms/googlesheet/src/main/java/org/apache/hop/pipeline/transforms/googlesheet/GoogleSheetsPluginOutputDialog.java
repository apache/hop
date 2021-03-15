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
 *
 */
package org.apache.hop.pipeline.transforms.googlesheet;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
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
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class GoogleSheetsPluginOutputDialog extends BaseTransformDialog
    implements ITransformDialog {

  private static final Class<?> PKG = GoogleSheetsPluginOutputMeta.class;

  private final GoogleSheetsPluginOutputMeta meta;

  private Label testServiceAccountInfo;
  private Label privateKeyInfo;
  private TextVar privateKeyStore;
  private TextVar spreadsheetKey;
  private TextVar wWorksheetId;
  private TextVar shareEmail;
  private TextVar wShareDomainWise;
  private Button wCreate;
  private Button wAppend;
  private TableView wFields;

  public GoogleSheetsPluginOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String name) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, name);
    this.meta = (GoogleSheetsPluginOutputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = this.getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, meta);

    ModifyListener modifiedListener =
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent e) {
            meta.setChanged();
          }
        };

    SelectionAdapter lsSa =
        new SelectionAdapter() {
          public void modifySelect(SelectionEvent e) {
            meta.setChanged();
          }
        };

    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Google Spreadsheet Output APIV4");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName - Label
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "GoogleSheetsPluginOutputDialog.TransformName.Label" ));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);

    // transformName - Text
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(modifiedListener);
    fdTransformName = new FormData();
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(tabFolder, Props.WIDGET_STYLE_TAB);
    tabFolder.setSimple(false);

    /*
     * BEGIN Service Account Tab
     */
    CTabItem serviceAccountTab = new CTabItem(tabFolder, SWT.NONE);
    serviceAccountTab.setText("Service Account");

    Composite serviceAccountComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(serviceAccountComposite);

    FormLayout serviceAccountLayout = new FormLayout();
    serviceAccountLayout.marginWidth = 3;
    serviceAccountLayout.marginHeight = 3;
    serviceAccountComposite.setLayout(serviceAccountLayout);

    // privateKey json - Label
    Label privateKeyLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    privateKeyLabel.setText(
        "Json credential file (default .kettle directory/client-secret.json is used) :");
    props.setLook(privateKeyLabel);
    FormData privateKeyLabelForm = new FormData();
    privateKeyLabelForm.top = new FormAttachment(0, margin);
    privateKeyLabelForm.left = new FormAttachment(0, 0);
    privateKeyLabelForm.right = new FormAttachment(middle, -margin);
    privateKeyLabel.setLayoutData(privateKeyLabelForm);

    // privateKey - Button
    Button privateKeyButton = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(privateKeyButton);
    privateKeyButton.setText("Browse");
    FormData privateKeyButtonForm = new FormData();
    privateKeyButtonForm.top = new FormAttachment(0, margin);
    privateKeyButtonForm.right = new FormAttachment(100, 0);
    privateKeyButton.setLayoutData(privateKeyButtonForm);

    // privatekey - Text
    privateKeyStore =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(privateKeyStore);
    privateKeyStore.addModifyListener(modifiedListener);
    FormData privateKeyStoreData = new FormData();
    privateKeyStoreData.top = new FormAttachment(0, margin);
    privateKeyStoreData.left = new FormAttachment(middle, 0);
    privateKeyStoreData.right = new FormAttachment(privateKeyButton, -margin);
    privateKeyStore.setLayoutData(privateKeyStoreData);

    // test service - Button
    Button testServiceAccountButton = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(testServiceAccountButton);
    testServiceAccountButton.setText("Test Connection");
    FormData testServiceAccountButtonData = new FormData();
    testServiceAccountButtonData.top = new FormAttachment(privateKeyButton, margin);
    testServiceAccountButtonData.left = new FormAttachment(0, 0);
    testServiceAccountButton.setLayoutData(testServiceAccountButtonData);

    testServiceAccountInfo = new Label(serviceAccountComposite, SWT.LEFT);
    props.setLook(testServiceAccountInfo);
    FormData testServiceAccountInfoData = new FormData();
    testServiceAccountInfoData.top = new FormAttachment(privateKeyButton, margin);
    testServiceAccountInfoData.left = new FormAttachment(middle, 0);
    testServiceAccountInfoData.right = new FormAttachment(100, 0);
    testServiceAccountInfo.setLayoutData(testServiceAccountInfoData);

    FormData serviceAccountCompositeData = new FormData();
    serviceAccountCompositeData.left = new FormAttachment(0, 0);
    serviceAccountCompositeData.top = new FormAttachment(0, 0);
    serviceAccountCompositeData.right = new FormAttachment(100, 0);
    serviceAccountCompositeData.bottom = new FormAttachment(100, 0);
    serviceAccountComposite.setLayoutData(serviceAccountCompositeData);

    serviceAccountComposite.layout();
    serviceAccountTab.setControl(serviceAccountComposite);
    /*
     * END Service Account Tab
     */

    /*
     * BEGIN Spreadsheet Tab
     */
    CTabItem spreadsheetTab = new CTabItem(tabFolder, SWT.NONE);
    spreadsheetTab.setText("Spreadsheet");

    Composite spreadsheetComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(spreadsheetComposite);

    FormLayout spreadsheetLayout = new FormLayout();
    spreadsheetLayout.marginWidth = 3;
    spreadsheetLayout.marginHeight = 3;
    spreadsheetComposite.setLayout(spreadsheetLayout);

    // spreadsheetKey - Label
    Label spreadsheetKeyLabel = new Label(spreadsheetComposite, SWT.RIGHT);
    spreadsheetKeyLabel.setText("Spreadsheet Key");
    props.setLook(spreadsheetKeyLabel);
    FormData spreadsheetKeyLabelData = new FormData();
    spreadsheetKeyLabelData.top = new FormAttachment(0, margin);
    spreadsheetKeyLabelData.left = new FormAttachment(0, 0);
    spreadsheetKeyLabelData.right = new FormAttachment(middle, -margin);
    spreadsheetKeyLabel.setLayoutData(spreadsheetKeyLabelData);

    // spreadsheetKey - Button
    Button spreadsheetKeyButton = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    spreadsheetKeyButton.setText("Browse");
    props.setLook(spreadsheetKeyButton);
    FormData spreadsheetKeyButtonData = new FormData();
    spreadsheetKeyButtonData.top = new FormAttachment(0, margin);
    spreadsheetKeyButtonData.right = new FormAttachment(100, 0);
    spreadsheetKeyButton.setLayoutData(spreadsheetKeyButtonData);

    // spreadsheetKey - Text
    spreadsheetKey =
        new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(spreadsheetKey);
    spreadsheetKey.addModifyListener(modifiedListener);
    FormData spreadsheetKeyData = new FormData();
    spreadsheetKeyData.top = new FormAttachment(0, margin);
    spreadsheetKeyData.left = new FormAttachment(middle, 0);
    spreadsheetKeyData.right = new FormAttachment(spreadsheetKeyButton, -margin);
    spreadsheetKey.setLayoutData(spreadsheetKeyData);

    // worksheetId - Label
    Label worksheetIdLabel = new Label(spreadsheetComposite, SWT.RIGHT);
    worksheetIdLabel.setText("Worksheet Id");
    props.setLook(worksheetIdLabel);
    FormData worksheetIdLabelData = new FormData();
    worksheetIdLabelData.top = new FormAttachment(spreadsheetKeyButton, margin);
    worksheetIdLabelData.left = new FormAttachment(0, 0);
    worksheetIdLabelData.right = new FormAttachment(middle, -margin);
    worksheetIdLabel.setLayoutData(worksheetIdLabelData);

    // worksheetId - Button
    Button worksheetIdButton = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    worksheetIdButton.setText("Browse");
    props.setLook(worksheetIdButton);
    FormData worksheetIdButtonData = new FormData();
    worksheetIdButtonData.top = new FormAttachment(spreadsheetKeyButton, margin);
    worksheetIdButtonData.right = new FormAttachment(100, 0);
    worksheetIdButton.setLayoutData(worksheetIdButtonData);

    // worksheetId - Text
    wWorksheetId = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook( wWorksheetId );
    wWorksheetId.addModifyListener(modifiedListener);
    FormData worksheetIdData = new FormData();
    worksheetIdData.top = new FormAttachment(spreadsheetKeyButton, margin);
    worksheetIdData.left = new FormAttachment(middle, 0);
    worksheetIdData.right = new FormAttachment(worksheetIdButton, -margin);
    wWorksheetId.setLayoutData(worksheetIdData);

    // Append tick box label
    Label wlAppend = new Label(spreadsheetComposite, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "GoogleSheetsPluginOutputDialog.Append.Label"));
    props.setLook(wlAppend);
    FormData fdAppend = new FormData();
    fdAppend.top = new FormAttachment(worksheetIdButton, margin);
    fdAppend.left = new FormAttachment(0, 0);
    fdAppend.right = new FormAttachment(middle, -margin);
    wlAppend.setLayoutData(fdAppend);

    // Append tick box button
    wAppend = new Button(spreadsheetComposite, SWT.CHECK);
    props.setLook( wAppend );
    wAppend.addSelectionListener(lsSa);
    fdAppend = new FormData();
    fdAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    fdAppend.left = new FormAttachment(middle, 0);
    fdAppend.right = new FormAttachment(100, 0);
    wAppend.setLayoutData(fdAppend);

    // Create New Sheet tick box label
    Label wlCreate = new Label(spreadsheetComposite, SWT.RIGHT);
    wlCreate.setText(BaseMessages.getString(PKG, "GoogleSheetsPluginOutputDialog.Create.Label"));
    props.setLook(wlCreate);
    FormData fdCreate = new FormData();
    fdCreate.top = new FormAttachment( wlAppend, 2*margin);
    fdCreate.left = new FormAttachment(0, 0);
    fdCreate.right = new FormAttachment(middle, -margin);
    wlCreate.setLayoutData(fdCreate);

    // Create New Sheet tick box button
    wCreate = new Button(spreadsheetComposite, SWT.CHECK);
    props.setLook( wCreate );
    wCreate.addSelectionListener(lsSa);
    fdCreate = new FormData();
    fdCreate.top = new FormAttachment( wlCreate, 0, SWT.CENTER);
    fdCreate.left = new FormAttachment(middle, 0);
    fdCreate.right = new FormAttachment(100, 0);
    wCreate.setLayoutData(fdCreate);

    // Share spreadsheet with label
    Label wlShare = new Label(spreadsheetComposite, SWT.RIGHT);
    wlShare.setText(BaseMessages.getString(PKG, "GoogleSheetsPluginOutputDialog.Share.Label"));
    props.setLook(wlShare);
    FormData fdlShare = new FormData();
    fdlShare.top = new FormAttachment( wlCreate, 2*margin);
    fdlShare.left = new FormAttachment(0, 0);
    fdlShare.right = new FormAttachment(middle, -margin);
    wlShare.setLayoutData(fdlShare);
    // Share spreadsheet with label
    shareEmail = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(shareEmail);
    shareEmail.addModifyListener(modifiedListener);
    FormData fdShare = new FormData();
    fdShare.top = new FormAttachment( wlShare, 0, SWT.CENTER);
    fdShare.left = new FormAttachment(middle, 0);
    fdShare.right = new FormAttachment(100, 0);
    shareEmail.setLayoutData(fdShare);

    // Share domainwise with label
    Label wlShareDomainWise = new Label(spreadsheetComposite, SWT.RIGHT);
    wlShareDomainWise.setText(BaseMessages.getString(PKG, "GoogleSheetsPluginOutputDialog.Share.LabelDW"));
    props.setLook(wlShareDomainWise);
    FormData fdlShareDW = new FormData();
    fdlShareDW.top = new FormAttachment(shareEmail, margin);
    fdlShareDW.left = new FormAttachment(0, 0);
    fdlShareDW.right = new FormAttachment(middle, -margin);
    wlShareDomainWise.setLayoutData(fdlShareDW);
    // Share domainwise with label
    wShareDomainWise = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook( wShareDomainWise );
    wShareDomainWise.addModifyListener(modifiedListener);
    FormData fdShareDW = new FormData();
    fdShareDW.top = new FormAttachment(shareEmail, margin);
    fdShareDW.left = new FormAttachment(middle, 0);
    fdShareDW.right = new FormAttachment(100, 0);
    wShareDomainWise.setLayoutData(fdShareDW);

    FormData spreadsheetCompositeData = new FormData();
    spreadsheetCompositeData.left = new FormAttachment(0, 0);
    spreadsheetCompositeData.top = new FormAttachment(0, 0);
    spreadsheetCompositeData.right = new FormAttachment(100, 0);
    spreadsheetCompositeData.bottom = new FormAttachment(100, 0);
    spreadsheetComposite.setLayoutData(spreadsheetCompositeData);

    spreadsheetComposite.layout();
    spreadsheetTab.setControl(spreadsheetComposite);
    /*
     * END Spreadsheet Tab
     */

    FormData tabFolderData = new FormData();
    tabFolderData.left = new FormAttachment(0, 0);
    tabFolderData.top = new FormAttachment(wTransformName, margin);
    tabFolderData.right = new FormAttachment(100, 0);
    tabFolderData.bottom = new FormAttachment(100, -50);
    tabFolder.setLayoutData(tabFolderData);

    // OK and cancel buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, tabFolder);

    lsCancel =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            cancel();
          }
        };
    lsOk =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            ok();
          }
        };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);
    //  wGet.addListener(SWT.Selection, lsGet);
    // default listener (for hitting "enter")
    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wTransformName.addSelectionListener(lsDef);
    // credential.json file selection
    privateKeyButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            FileDialog dialog = new FileDialog(shell, SWT.OPEN);
            dialog.setFilterExtensions(new String[] {"*json", "*"});
            dialog.setFilterNames(new String[] {"credential JSON file", "All Files"});
            String filename = dialog.open();
            if (filename != null) {
              privateKeyStore.setText(filename);
              meta.setChanged();
            }
          }
        });

    // testing connection to Google with API V4
    testServiceAccountButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {
              NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
              String APPLICATION_NAME = "Hop-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              //  String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() +"/tokens";
              String scope = SheetsScopes.SPREADSHEETS_READONLY;
              Drive service =
                  new Drive.Builder(
                          HTTP_TRANSPORT,
                          JSON_FACTORY,
                          GoogleSheetsPluginCredentials.getCredentialsJson(
                              scope, variables.resolve(privateKeyStore.getText())))
                      .setApplicationName(APPLICATION_NAME)
                      .build();
              testServiceAccountInfo.setText("");

              if (service == null) {
                testServiceAccountInfo.setText("Connection Failed");
              } else {
                testServiceAccountInfo.setText("Google Drive API : Success!");
              }
            } catch (Exception error) {
              testServiceAccountInfo.setText("Connection Failed");
            }
          }
        });
    // Display spreadsheets
    spreadsheetKeyButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {
              NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
              String APPLICATION_NAME = "Hop-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              // String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() +"/tokens";
              String scope = "https://www.googleapis.com/auth/drive";
              Drive service =
                  new Drive.Builder(
                          HTTP_TRANSPORT,
                          JSON_FACTORY,
                          GoogleSheetsPluginCredentials.getCredentialsJson(
                              scope, variables.resolve(privateKeyStore.getText())))
                      .setApplicationName(APPLICATION_NAME)
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
              // FileList result =
              // service.files().list().setQ("mimeType='application/vnd.google-apps.spreadsheet'").setPageSize(100).setFields("nextPageToken, files(id, name)").execute();
              // FileList result =
              // service.files().list().setQ("mimeType='application/vnd.google-apps.spreadsheet'").setPageSize(100).setFields("nextPageToken, files(id, name)").execute();
              List<File> spreadsheets = result.getFiles();
              int selectedSpreadsheet = -1;
              int i = 0;
              String[] titles = new String[spreadsheets.size()];
              for (File spreadsheet : spreadsheets) {
                titles[i] = spreadsheet.getName() + " - " + spreadsheet.getId();
                if (spreadsheet.getId().equals(spreadsheetKey.getText())) {
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
                  spreadsheetKey.setText(spreadsheet.getId());
                } else {
                  spreadsheetKey.setText("");
                }
              }

            } catch (Exception err) {
              new ErrorDialog(shell, "System.Dialog.Error.Title", err.getMessage(), err);
            }
          }
        });
    // Display worksheets
    worksheetIdButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {

              NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
              String APPLICATION_NAME = "Hop-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              // String TOKENS_DIRECTORY_PATH = Const.getKettleDirectory() +"/tokens";
              String scope = SheetsScopes.SPREADSHEETS_READONLY;

              Sheets service =
                  new Sheets.Builder(
                          HTTP_TRANSPORT,
                          JSON_FACTORY,
                          GoogleSheetsPluginCredentials.getCredentialsJson(
                              scope, variables.resolve(privateKeyStore.getText())))
                      .setApplicationName(APPLICATION_NAME)
                      .build();
              Spreadsheet response1 =
                  service
                      .spreadsheets()
                      .get(spreadsheetKey.getText())
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
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  err.getMessage(),
                  err);
            }
          }
        });

    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    tabFolder.setSelection(0);
    setSize();
    getData(meta);
    meta.setChanged(changed);
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) display.sleep();
    }
    return transformName;
  }

  private void getData(GoogleSheetsPluginOutputMeta meta) {
    this.wTransformName.selectAll();

    this.spreadsheetKey.setText(meta.getSpreadsheetKey());
    this.wWorksheetId.setText(meta.getWorksheetId());
    this.shareEmail.setText(meta.getShareEmail());
    this.wCreate.setSelection(meta.getCreate());
    this.wAppend.setSelection(meta.getAppend());

    this.wShareDomainWise.setText(meta.getShareDomain());
    this.privateKeyStore.setText(meta.getJsonCredentialPath());
  }

  private void setData(GoogleSheetsPluginOutputMeta meta) {

    meta.setJsonCredentialPath(this.privateKeyStore.getText());
    meta.setSpreadsheetKey(this.spreadsheetKey.getText());
    meta.setWorksheetId(this.wWorksheetId.getText());
    meta.setShareEmail(this.shareEmail.getText());
    meta.setCreate(this.wCreate.getSelection());
    meta.setAppend(this.wAppend.getSelection());
    meta.setShareDomain(this.wShareDomainWise.getText());
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
}
