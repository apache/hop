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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
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
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class GoogleSheetsPluginInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = GoogleSheetsPluginInputMeta.class;

  private final GoogleSheetsPluginInputMeta meta;

  private Label testServiceAccountInfo;
  private TextVar privateKeyStore;
  private TextVar spreadsheetKey;
  private TextVar worksheetId;
  private TextVar sampleFields;
  private TableView wFields;

  public GoogleSheetsPluginInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String name) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, name);
    this.meta = (GoogleSheetsPluginInputMeta) in;
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

    ModifyListener contentListener =
        new ModifyListener() {
          @Override
          public void modifyText(ModifyEvent arg0) {
            // asyncUpdatePreview();
          }
        };

    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Google Spreadsheet Input APIV4");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName  - Label
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "GoogleSheetsPluginInputDialog.TransformName.Label"));
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
    privateKeyLabel.setText("Json credential file :");
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
    worksheetId = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(worksheetId);
    worksheetId.addModifyListener(modifiedListener);
    FormData worksheetIdData = new FormData();
    worksheetIdData.top = new FormAttachment(spreadsheetKeyButton, margin);
    worksheetIdData.left = new FormAttachment(middle, 0);
    worksheetIdData.right = new FormAttachment(worksheetIdButton, -margin);
    worksheetId.setLayoutData(worksheetIdData);

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

    /*
     * BEGIN Fields Tab
     */
    // Nb Sample Fields - Label
    CTabItem fieldsTab = new CTabItem(tabFolder, SWT.NONE);
    fieldsTab.setText("Fields");

    Composite fieldsComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(fieldsComposite);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    fieldsComposite.setLayout(fieldsLayout);

    Label sampleFieldsLabel = new Label(fieldsComposite, SWT.RIGHT);
    sampleFieldsLabel.setText("Number of sample lines to guess field types : ");
    props.setLook(sampleFieldsLabel);
    FormData sampleFieldsLabelData = new FormData();
    sampleFieldsLabelData.top = new FormAttachment(0, margin);
    sampleFieldsLabelData.left = new FormAttachment(0, 0);
    sampleFieldsLabelData.right = new FormAttachment(middle, -margin);
    sampleFieldsLabel.setLayoutData(sampleFieldsLabelData);

    // sampleFields - Text
    sampleFields = new TextVar(variables, fieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(sampleFields);
    sampleFields.addModifyListener(modifiedListener);
    FormData sampleFieldsData = new FormData();
    sampleFieldsData.top = new FormAttachment(0, margin);
    sampleFieldsData.left = new FormAttachment(middle, 0);
    sampleFieldsData.right = new FormAttachment(100, -margin);
    sampleFields.setLayoutData(sampleFieldsData);

    wGet = new Button(fieldsComposite, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));

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
            modifiedListener,
            props);

    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(sampleFields, margin);
    fdFields.bottom = new FormAttachment(wGet, -margin * 2);
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
    wFields.setContentListener(contentListener);

    FormData fieldsCompositeData = new FormData();
    fieldsCompositeData.left = new FormAttachment(0, 0);
    fieldsCompositeData.top = new FormAttachment(0, 0);
    fieldsCompositeData.right = new FormAttachment(100, 0);
    fieldsCompositeData.bottom = new FormAttachment(100, 0);
    fieldsComposite.setLayoutData(fieldsCompositeData);

    setButtonPositions(new Button[] {wGet}, margin, null);

    fieldsComposite.layout();
    fieldsTab.setControl(fieldsComposite);
    /*
     * END Fields Tab
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
    lsGet =
        new Listener() {
          @Override
          public void handleEvent(Event e) {
            getSpreadsheetFields();
          }
        };

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);
    wGet.addListener(SWT.Selection, lsGet);

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
              String APPLICATION_NAME = "pentaho-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              // String TOKENS_DIRECTORY_PATH = "set your path here" +"/tokens";
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
              String APPLICATION_NAME = "pentaho-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              // String TOKENS_DIRECTORY_PATH = Const.getHopDirectory() +"/tokens";
              String scope = "https://www.googleapis.com/auth/drive.readonly";
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
              List<File> spreadsheets = result.getFiles();
              int selectedSpreadsheet = -1;
              int i = 0;
              String[] titles = new String[spreadsheets.size()];
              for (File spreadsheet : spreadsheets) {
                titles[i] = spreadsheet.getName() + " - " + spreadsheet.getId() + ")";

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
              String APPLICATION_NAME = "hop-google-sheets";
              JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
              // String TOKENS_DIRECTORY_PATH = Const.getHopDirectory() +"/tokens";
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
                      .get(variables.resolve(spreadsheetKey.getText()))
                      .setIncludeGridData(false)
                      .execute();

              List<Sheet> worksheets = response1.getSheets();
              String[] names = new String[worksheets.size()];
              int selectedSheet = -1;
              for (int i = 0; i < worksheets.size(); i++) {
                Sheet sheet = worksheets.get(i);
                names[i] = sheet.getProperties().getTitle();
                if (sheet.getProperties().getTitle().endsWith("/" + worksheetId.getText())) {
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
                  worksheetId.setText(id.substring(id.lastIndexOf("/") + 1));
                } else {
                  worksheetId.setText("");
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

  private void getData(GoogleSheetsPluginInputMeta meta) {
    this.wTransformName.selectAll();

    this.spreadsheetKey.setText(meta.getSpreadsheetKey());
    this.worksheetId.setText(meta.getWorksheetId());
    this.privateKeyStore.setText(meta.getJsonCredentialPath());
    this.sampleFields.setText(Integer.toString(meta.getSampleFields()));

    for (int i = 0; i < meta.getInputFields().length; i++) {
      GoogleSheetsPluginInputFields field = meta.getInputFields()[i];

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

  private void setData(GoogleSheetsPluginInputMeta meta) {

    meta.setJsonCredentialPath(this.privateKeyStore.getText());
    meta.setSpreadsheetKey(this.spreadsheetKey.getText());
    meta.setWorksheetId(this.worksheetId.getText());
    if (this.sampleFields != null && !this.sampleFields.getText().isEmpty()) {
      meta.setSampleFields(Integer.parseInt(this.sampleFields.getText()));
    } else {
      meta.setSampleFields(100);
    }

    int nrNonEmptyFields = wFields.nrNonEmpty();
    meta.allocate(nrNonEmptyFields);

    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      meta.getInputFields()[i] = new GoogleSheetsPluginInputFields();

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
    // initalize output String as empty
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
      GoogleSheetsPluginInputMeta meta = new GoogleSheetsPluginInputMeta();
      setData(meta);
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      String APPLICATION_NAME = "pentaho-sheets";
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String TOKENS_DIRECTORY_PATH = "tokens";
      String scope = SheetsScopes.SPREADSHEETS_READONLY;
      wFields.table.removeAll();

      Sheets service =
          new Sheets.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsPluginCredentials.getCredentialsJson(
                      scope, variables.resolve(privateKeyStore.getText())))
              .setApplicationName(APPLICATION_NAME)
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
            // Fill in sample in order to guess types ___ GoogleSheetsPluginInputFields( String
            // fieldname, int position, int length )
            GoogleSheetsPluginInputFields sampleInputFields = new GoogleSheetsPluginInputFields();
            String columnsLetter = getColumnName(j + 1);
            logBasic("column:" + Integer.toString(j) + ")" + columnsLetter);
            Integer nbSampleFields = Integer.parseInt(variables.resolve(sampleFields.getText()));

            String sampleRange =
                variables.resolve(meta.getWorksheetId())
                    + "!"
                    + columnsLetter
                    + "2:"
                    + columnsLetter
                    + variables.resolve(sampleFields.getText());
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
