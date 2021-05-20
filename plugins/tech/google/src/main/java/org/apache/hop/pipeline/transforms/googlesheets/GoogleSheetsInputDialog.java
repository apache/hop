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
import com.google.api.services.sheets.v4.model.ValueRange;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
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
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class GoogleSheetsInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = GoogleSheetsInputMeta.class; // for Translator

  private final GoogleSheetsInputMeta meta;

  private Label wlTestServiceAccountInfo;
  private TextVar wPrivateKeyStore;
  private TextVar wSpreadSheetKey;
  private TextVar wWorksheetId;
  private TextVar wSampleFields;
  private TableView wFields;

  public GoogleSheetsInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String name) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, name);
    this.meta = (GoogleSheetsInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = this.getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, meta);

    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "GoogleSheetsInput.transform.Name"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // OK and cancel buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // transformName  - Label
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "GoogleSheetsInput.transform.Name"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);

    // transformName  - Text
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
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
    serviceAccountTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.ServiceAccount"));

    Composite serviceAccountComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(serviceAccountComposite);

    FormLayout serviceAccountLayout = new FormLayout();
    serviceAccountLayout.marginWidth = 3;
    serviceAccountLayout.marginHeight = 3;
    serviceAccountComposite.setLayout(serviceAccountLayout);

    // privateKey json - Label
    Label wlPrivateKeyStore = new Label(serviceAccountComposite, SWT.RIGHT);
    wlPrivateKeyStore.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.PrivateKeyStore"));
    props.setLook(wlPrivateKeyStore);
    FormData fdlPrivateKeyStore = new FormData();
    fdlPrivateKeyStore.top = new FormAttachment(0, margin);
    fdlPrivateKeyStore.left = new FormAttachment(0, 0);
    fdlPrivateKeyStore.right = new FormAttachment(middle, -margin);
    wlPrivateKeyStore.setLayoutData(fdlPrivateKeyStore);

    // privateKey - Button
    Button wbPrivateKeyStore = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(wbPrivateKeyStore);
    wbPrivateKeyStore.setText(BaseMessages.getString("System.Button.Browse"));
    FormData fdbPrivateKeyStore = new FormData();
    fdbPrivateKeyStore.top = new FormAttachment(0, margin);
    fdbPrivateKeyStore.right = new FormAttachment(100, 0);
    wbPrivateKeyStore.setLayoutData(fdbPrivateKeyStore);
    wbPrivateKeyStore.addListener(SWT.Selection, e -> selectPrivateKeyStoreFile());

    // privatekey - Text
    wPrivateKeyStore =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrivateKeyStore);
    FormData fdPrivateKeyStore = new FormData();
    fdPrivateKeyStore.top = new FormAttachment(0, margin);
    fdPrivateKeyStore.left = new FormAttachment(middle, 0);
    fdPrivateKeyStore.right = new FormAttachment(wbPrivateKeyStore, -margin);
    wPrivateKeyStore.setLayoutData(fdPrivateKeyStore);

    // test service - Button
    Button wbTestServiceAccount = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestServiceAccount);
    wbTestServiceAccount.setText(
        BaseMessages.getString(PKG, "GoogleSheetsDialog.Button.TestConnection"));
    FormData fdbTestServiceAccount = new FormData();
    fdbTestServiceAccount.top = new FormAttachment(wbPrivateKeyStore, margin);
    fdbTestServiceAccount.left = new FormAttachment(0, 0);
    wbTestServiceAccount.setLayoutData(fdbTestServiceAccount);
    wbTestServiceAccount.addListener(SWT.Selection, e -> testServiceAccount());

    wlTestServiceAccountInfo = new Label(serviceAccountComposite, SWT.LEFT);
    props.setLook(wlTestServiceAccountInfo);
    FormData fdlTestServiceAccountInfo = new FormData();
    fdlTestServiceAccountInfo.top = new FormAttachment(wbPrivateKeyStore, margin);
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
    props.setLook(spreadsheetComposite);

    FormLayout spreadsheetLayout = new FormLayout();
    spreadsheetLayout.marginWidth = 3;
    spreadsheetLayout.marginHeight = 3;
    spreadsheetComposite.setLayout(spreadsheetLayout);

    // spreadsheetKey - Label
    Label wlSpreadSheetKey = new Label(spreadsheetComposite, SWT.RIGHT);
    wlSpreadSheetKey.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.SpreadsheetKey"));
    props.setLook(wlSpreadSheetKey);
    FormData fdlSpreadSheetKey = new FormData();
    fdlSpreadSheetKey.top = new FormAttachment(0, margin);
    fdlSpreadSheetKey.left = new FormAttachment(0, 0);
    fdlSpreadSheetKey.right = new FormAttachment(middle, -margin);
    wlSpreadSheetKey.setLayoutData(fdlSpreadSheetKey);

    // spreadsheetKey - Button
    Button wbSpreadSheetKey = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbSpreadSheetKey.setText(BaseMessages.getString("System.Button.Browse"));
    props.setLook(wbSpreadSheetKey);
    FormData fdbSpreadSheetKey = new FormData();
    fdbSpreadSheetKey.top = new FormAttachment(0, margin);
    fdbSpreadSheetKey.right = new FormAttachment(100, 0);
    wbSpreadSheetKey.setLayoutData(fdbSpreadSheetKey);
    wbSpreadSheetKey.addListener(SWT.Selection, e -> selectSpreadSheet());

    // spreadsheetKey - Text
    wSpreadSheetKey =
        new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSpreadSheetKey);
    FormData fdSpreadSheetKey = new FormData();
    fdSpreadSheetKey.top = new FormAttachment(0, margin);
    fdSpreadSheetKey.left = new FormAttachment(middle, 0);
    fdSpreadSheetKey.right = new FormAttachment(wbSpreadSheetKey, -margin);
    wSpreadSheetKey.setLayoutData(fdSpreadSheetKey);

    // worksheetId - Label
    Label wlWorksheetId = new Label(spreadsheetComposite, SWT.RIGHT);
    wlWorksheetId.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.WorksheetId"));
    props.setLook(wlWorksheetId);
    FormData fdlWorksheetId = new FormData();
    fdlWorksheetId.top = new FormAttachment(wbSpreadSheetKey, margin);
    fdlWorksheetId.left = new FormAttachment(0, 0);
    fdlWorksheetId.right = new FormAttachment(middle, -margin);
    wlWorksheetId.setLayoutData(fdlWorksheetId);

    // worksheetId - Button
    Button wbWorksheetId = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbWorksheetId.setText(BaseMessages.getString("System.Button.Browse"));
    props.setLook(wbWorksheetId);
    FormData fdbWorksheetId = new FormData();
    fdbWorksheetId.top = new FormAttachment(wbSpreadSheetKey, margin);
    fdbWorksheetId.right = new FormAttachment(100, 0);
    wbWorksheetId.setLayoutData(fdbWorksheetId);
    wbWorksheetId.addListener(SWT.Selection, e -> selectWorksheet());

    // worksheetId - Text
    wWorksheetId = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWorksheetId);
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
     * BEGIN Fields Tab
     */
    // Nb Sample Fields - Label
    CTabItem fieldsTab = new CTabItem(tabFolder, SWT.NONE);
    fieldsTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.Fields"));

    Composite fieldsComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(fieldsComposite);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    fieldsComposite.setLayout(fieldsLayout);

    Label wlSampleFieldsLabel = new Label(fieldsComposite, SWT.RIGHT);
    wlSampleFieldsLabel.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.NrOfSampleLines"));
    props.setLook(wlSampleFieldsLabel);
    FormData fdlSampleFields = new FormData();
    fdlSampleFields.top = new FormAttachment(0, margin);
    fdlSampleFields.left = new FormAttachment(0, 0);
    fdlSampleFields.right = new FormAttachment(middle, -margin);
    wlSampleFieldsLabel.setLayoutData(fdlSampleFields);

    // sampleFields - Text
    wSampleFields = new TextVar(variables, fieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSampleFields);
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
          new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              "Type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true),
          new ColumnInfo("Format", ColumnInfo.COLUMN_TYPE_FORMAT, 2),
          new ColumnInfo("Length", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Precision", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Currency", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Decimal", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Group", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Trim type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaString.trimTypeDesc),
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
    fdFields.bottom = new FormAttachment(wGet, -margin * 2);
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
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    tabFolder.setLayoutData(fdTabFolder);

    tabFolder.setSelection(0);
    getData(meta);
    meta.setChanged(changed);

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
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      // Test by building a drive connection
      //
      new Drive.Builder(
              HTTP_TRANSPORT,
              JSON_FACTORY,
              GoogleSheetsCredentials.getCredentialsJson(
                  scope, variables.resolve(wPrivateKeyStore.getText())))
          .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
          .build();
      wlTestServiceAccountInfo.setText("Google Drive API : Success!");
    } catch (Exception error) {
      wlTestServiceAccountInfo.setText("Connection Failed");
    }
  }

  private void selectSpreadSheet() {
    try {
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = "https://www.googleapis.com/auth/drive.readonly";
      Drive service =
          new Drive.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsCredentials.getCredentialsJson(
                      scope, variables.resolve(wPrivateKeyStore.getText())))
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
      new ErrorDialog(shell, "System.Dialog.Error.Title", err.getMessage(), err);
    }
  }

  private void selectWorksheet() {
    try {

      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      Sheets service =
          new Sheets.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsCredentials.getCredentialsJson(
                      scope, variables.resolve(wPrivateKeyStore.getText())))
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
          shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), err.getMessage(), err);
    }
  }

  private void getData(GoogleSheetsInputMeta meta) {
    this.wTransformName.selectAll();

    this.wSpreadSheetKey.setText(meta.getSpreadsheetKey());
    this.wWorksheetId.setText(meta.getWorksheetId());
    this.wPrivateKeyStore.setText(meta.getJsonCredentialPath());
    this.wSampleFields.setText(Integer.toString(meta.getSampleFields()));

    for (int i = 0; i < meta.getInputFields().length; i++) {
      GoogleSheetsInputFields field = meta.getInputFields()[i];

      TableItem item = new TableItem(wFields.table, SWT.NONE);

      item.setText(1, Const.NVL(field.getName(), ""));
      String type = field.getTypeDesc();
      String format = field.getFormat();
      String position = "" + field.getPosition();
      String length = "" + field.getLength();
      String prec = "" + field.getPrecision();
      String curr = field.getCurrencySymbol();
      String group = field.getGroupSymbol();
      String decim = field.getDecimalSymbol();
      String trim = field.getTrimTypeDesc();

      if (type != null) {
        item.setText(2, type);
      }
      if (format != null) {
        item.setText(3, format);
      }
      /*if ( position != null && !"-1".equals( position ) ) {
      item.setText( , position );
       }*/
      /* if ( length != null && !"-1".equals( length ) ) {
      item.setText( 4, length );
       }*/
      if (prec != null && !"-1".equals(prec)) {
        item.setText(5, prec);
      }
      if (curr != null) {
        item.setText(5, curr);
      }
      if (decim != null) {
        item.setText(7, decim);
      }
      if (group != null) {
        item.setText(8, group);
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
    if (this.wSampleFields != null && !this.wSampleFields.getText().isEmpty()) {
      meta.setSampleFields(Integer.parseInt(this.wSampleFields.getText()));
    } else {
      meta.setSampleFields(100);
    }

    int nrNonEmptyFields = wFields.nrNonEmpty();
    meta.allocate(nrNonEmptyFields);

    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      meta.getInputFields()[i] = new GoogleSheetsInputFields();

      int colnr = 1;
      meta.getInputFields()[i].setName(item.getText(colnr++));
      meta.getInputFields()[i].setType(ValueMetaFactory.getIdForValueMeta(item.getText(colnr++)));
      meta.getInputFields()[i].setFormat(item.getText(colnr++));
      meta.getInputFields()[i].setLength(Const.toInt(item.getText(colnr++), -1));
      meta.getInputFields()[i].setPrecision(Const.toInt(item.getText(colnr++), -1));
      meta.getInputFields()[i].setCurrencySymbol(item.getText(colnr++));
      meta.getInputFields()[i].setDecimalSymbol(item.getText(colnr++));
      meta.getInputFields()[i].setGroupSymbol(item.getText(colnr++));
      meta.getInputFields()[i].setTrimType(
          ValueMetaString.getTrimTypeByDesc(item.getText(colnr++)));
    }
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
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;
      wFields.table.removeAll();

      Sheets service =
          new Sheets.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsCredentials.getCredentialsJson(
                      scope, variables.resolve(wPrivateKeyStore.getText())))
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
            GoogleSheetsInputFields sampleInputFields = new GoogleSheetsInputFields();
            String columnsLetter = getColumnName(j + 1);
            logBasic("column:" + Integer.toString(j) + ")" + columnsLetter);
            Integer nbSampleFields = Integer.parseInt(variables.resolve(wSampleFields.getText()));

            String sampleRange =
                variables.resolve(meta.getWorksheetId())
                    + "!"
                    + columnsLetter
                    + "2:"
                    + columnsLetter
                    + variables.resolve(wSampleFields.getText());
            logBasic("Guess Fieds : Range : " + sampleRange);
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
                    && sampleRow.size() > 0
                    && sampleRow.get(0) != null
                    && !sampleRow.get(0).toString().isEmpty()) {
                  String tmp = sampleRow.get(0).toString();
                  logBasic(Integer.toString(m) + ")" + tmp.toString());
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
              item.setText(2, sampleInputFields.getTypeDesc());
              item.setText(3, sampleInputFields.getFormat());
              item.setText(5, Integer.toString(sampleInputFields.getPrecision()));
              item.setText(6, sampleInputFields.getCurrencySymbol());
              item.setText(7, sampleInputFields.getDecimalSymbol());
              item.setText(8, sampleInputFields.getGroupSymbol());
              item.setText(9, sampleInputFields.getTrimTypeDesc());
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
          shell,
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          "Error getting Fields",
          e);
    }
  }
}
