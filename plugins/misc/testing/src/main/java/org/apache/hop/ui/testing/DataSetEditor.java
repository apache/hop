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

package org.apache.hop.ui.testing;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetCsvUtil;
import org.apache.hop.testing.DataSetField;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.File;
import java.util.List;

public class DataSetEditor extends MetadataEditor<DataSet> {
  private static final Class<?> PKG = DataSetEditor.class; // For Translator

  private Text wName;
  private Text wDescription;
  private Text wBaseFilename;
  private TextVar wFolderName;
  private TableView wFieldMapping;

  public DataSetEditor(HopGui hopGui, MetadataManager<DataSet> manager, DataSet dataSet) {
    super(hopGui, manager, dataSet);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    int margin = Const.MARGIN;

    // The name of the group...
    //
    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    props.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "DataSetDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);

    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 5);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -5);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // The description of the group...
    //
    Label wlDescription = new Label(parent, SWT.LEFT);
    props.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "DataSetDialog.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(spacer, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(100, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.MULTI | SWT.LEFT | SWT.V_SCROLL | SWT.BORDER);
    props.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.height = 50;
    fdDescription.top = new FormAttachment(wlDescription, margin);
    fdDescription.left = new FormAttachment(0, 0);
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);

    // The folder containing the set...
    //
    Label wlFolderName = new Label(parent, SWT.LEFT);
    props.setLook(wlFolderName);
    wlFolderName.setText(BaseMessages.getString(PKG, "DataSetDialog.FolderName.Label"));
    FormData fdlFolderName = new FormData();
    fdlFolderName.top = new FormAttachment(wDescription, margin);
    fdlFolderName.left = new FormAttachment(0, 0);
    fdlFolderName.right = new FormAttachment(100, 0);
    wlFolderName.setLayoutData(fdlFolderName);
    wFolderName = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFolderName);
    FormData fdFolderName = new FormData();
    fdFolderName.top = new FormAttachment(wlFolderName, margin);
    fdFolderName.left = new FormAttachment(0, 0);
    fdFolderName.right = new FormAttachment(100, 0);
    wFolderName.setLayoutData(fdFolderName);

    // The table storing the set...
    //
    Label wlBaseFilename = new Label(parent, SWT.LEFT);
    props.setLook(wlBaseFilename);
    wlBaseFilename.setText(BaseMessages.getString(PKG, "DataSetDialog.BaseFilename.Label"));
    FormData fdlBaseFilename = new FormData();
    fdlBaseFilename.top = new FormAttachment(wFolderName, margin);
    fdlBaseFilename.left = new FormAttachment(0, 0);
    fdlBaseFilename.right = new FormAttachment(100, 0);
    wlBaseFilename.setLayoutData(fdlBaseFilename);
    wBaseFilename = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBaseFilename);
    FormData fdBaseFilename = new FormData();
    fdBaseFilename.top = new FormAttachment(wlBaseFilename, margin);
    fdBaseFilename.left = new FormAttachment(0, 0);
    fdBaseFilename.right = new FormAttachment(100, 0);
    wBaseFilename.setLayoutData(fdBaseFilename);

    // The field mapping from the input to the data set...
    //
    Label wlFieldMapping = new Label(parent, SWT.NONE);
    wlFieldMapping.setText(BaseMessages.getString(PKG, "DataSetDialog.FieldMapping.Label"));
    props.setLook(wlFieldMapping);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment(0, 0);
    fdlUpIns.top = new FormAttachment(wBaseFilename, margin * 2);
    wlFieldMapping.setLayoutData(fdlUpIns);

    // the field mapping grid in between
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.FieldName"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.FieldType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getAllValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.FieldFormat"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.FieldLength"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              true,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.FieldPrecision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              true,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DataSetDialog.ColumnInfo.Comment"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };

    wFieldMapping =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            columns,
            getMetadata().getFields().size(),
            null,
            props);

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment(0, 0);
    fdFieldMapping.top = new FormAttachment(wlFieldMapping, margin);
    fdFieldMapping.right = new FormAttachment(100, 0);
    fdFieldMapping.bottom = new FormAttachment(100, -2 * margin);
    wFieldMapping.setLayoutData(fdFieldMapping);

    this.setWidgetsContent();

    // Add listener to detect change after loading data
    ModifyListener lsMod = e -> setChanged();
    wName.addModifyListener(lsMod);
    wDescription.addModifyListener(lsMod);
    wFolderName.addModifyListener(lsMod);
    wBaseFilename.addModifyListener(lsMod);
    wFieldMapping.addModifyListener(lsMod);
  }

  protected void editData() {

    // If the row count is too high, we don't want to load it into memory...
    // Too high simply means: above the preview size...
    //
    int previewSize = PropsUi.getInstance().getDefaultPreviewSize();
    try {

      verifySettings();

      DataSet set = new DataSet();
      getWidgetsContent(set);

      // get rows from the data set...
      //
      List<Object[]> rows = set.getAllRows(manager.getVariables(), LogChannel.UI);

      IRowMeta fieldsRowMeta = set.getSetRowMeta();

      boolean written = false;
      while (!written) {
        try {
          EditRowsDialog editRowsDialog =
              new EditRowsDialog(
                  getShell(),
                  SWT.NONE,
                  BaseMessages.getString(PKG, "DataSetDialog.EditRows.Title"),
                  BaseMessages.getString(PKG, "DataSetDialog.EditRows.Message", set.getName()),
                  fieldsRowMeta,
                  rows);
          List<Object[]> newList = editRowsDialog.open();
          if (newList != null) {
            File setFolder = new File(set.getActualDataSetFolder(manager.getVariables()));
            boolean folderExists = setFolder.exists();
            if (!folderExists) {
              MessageBox box =
                  new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
              box.setText("Create data sets folder?");
              box.setMessage(
                  "The data sets folder does not exist. Do you want to create it?"
                      + Const.CR
                      + set.getActualDataSetFolder(manager.getVariables()));
              int answer = box.open();
              if ((answer & SWT.YES) != 0) {
                setFolder.mkdirs();
                folderExists = true;
              } else if ((answer & SWT.CANCEL) != 0) {
                break;
              }
            }
            // Write the rows back to the data set
            //
            if (folderExists) {
              DataSetCsvUtil.writeDataSetData(manager.getVariables(), set, fieldsRowMeta, newList);
              written = true;
            }
          } else {
            // User hit cancel
            break;
          }
        } catch (Exception e) {
          new ErrorDialog(
              getShell(),
              "Error",
              "Error writing data to dataset file "
                  + set.getActualDataSetFilename(manager.getVariables()),
              e);
        }
      }

    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error previewing data from dataset table", e);
    }
  }

  @Override
  public void setWidgetsContent() {
    DataSet dataSet = getMetadata();

    wName.setText(Const.NVL(dataSet.getName(), ""));
    wDescription.setText(Const.NVL(dataSet.getDescription(), ""));
    wFolderName.setText(Const.NVL(dataSet.getFolderName(), ""));
    wBaseFilename.setText(Const.NVL(dataSet.getBaseFilename(), ""));
    for (int i = 0; i < dataSet.getFields().size(); i++) {
      DataSetField field = dataSet.getFields().get(i);
      int colNr = 1;
      wFieldMapping.setText(Const.NVL(field.getFieldName(), ""), colNr++, i);
      wFieldMapping.setText(ValueMetaFactory.getValueMetaName(field.getType()), colNr++, i);
      wFieldMapping.setText(Const.NVL(field.getFormat(), ""), colNr++, i);
      wFieldMapping.setText(
          field.getLength() >= 0 ? Integer.toString(field.getLength()) : "", colNr++, i);
      wFieldMapping.setText(
          field.getPrecision() >= 0 ? Integer.toString(field.getPrecision()) : "", colNr++, i);
      wFieldMapping.setText(Const.NVL(field.getComment(), ""), colNr++, i);
    }
  }

  @Override
  public void getWidgetsContent(DataSet dataSet) {
    dataSet.setName(wName.getText());
    dataSet.setDescription(wDescription.getText());
    dataSet.setFolderName(wFolderName.getText());
    dataSet.setBaseFilename(wBaseFilename.getText());
    dataSet.getFields().clear();
    int nrFields = wFieldMapping.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFieldMapping.getNonEmpty(i);
      int colnr = 1;
      String fieldName = item.getText(colnr++);
      int type = ValueMetaFactory.getIdForValueMeta(item.getText(colnr++));
      String format = item.getText(colnr++);
      int length = Const.toInt(item.getText(colnr++), -1);
      int precision = Const.toInt(item.getText(colnr++), -1);
      String comment = item.getText(colnr++);

      DataSetField field = new DataSetField(fieldName, type, length, precision, comment, format);
      dataSet.getFields().add(field);
    }
  }

  @Override
  public void save() throws HopException {

    try {
      verifySettings();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          "Error",
          BaseMessages.getString(PKG, "DataSetDialog.Error.ValidationError"),
          e);
    }

    getWidgetsContent(getMetadata());

    super.save();
    ;
  }

  private void verifySettings() throws HopException {
    try {
      if (StringUtil.isEmpty(wBaseFilename.getText())) {
        throw new HopException(BaseMessages.getString(PKG, "DataSetDialog.Error.NoTableSpecified"));
      }
    } catch (Exception e) {
      throw new HopException("Error validating group and table values", e);
    }
  }

  protected void viewData() {
    try {
      DataSet set = new DataSet();
      getWidgetsContent(set);
      verifySettings();

      List<Object[]> setRows = set.getAllRows(manager.getVariables(), LogChannel.UI);
      IRowMeta setRowMeta = set.getSetRowMeta();

      PreviewRowsDialog previewRowsDialog =
          new PreviewRowsDialog(
              getShell(), new Variables(), SWT.NONE, set.getName(), setRowMeta, setRows);
      previewRowsDialog.open();

    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error previewing data from dataset table", e);
    }
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    Button wEditData = new Button(parent, SWT.PUSH);
    wEditData.setText(BaseMessages.getString(PKG, "DataSetDialog.EditData.Button"));
    wEditData.addListener(SWT.Selection, e -> editData());

    Button wViewData = new Button(parent, SWT.PUSH);
    wViewData.setText(BaseMessages.getString(PKG, "DataSetDialog.ViewData.Button"));
    wViewData.addListener(SWT.Selection, e -> viewData());

    return new Button[] {wEditData, wViewData};
  }
}
