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

package org.apache.hop.pipeline.transforms.sasinput;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import com.epam.parso.SasFileProperties;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.InputStream;
import java.util.List;

public class SasInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = SasInputMeta.class; // for Translator

  private CCombo wAccField;

  private final SasInputMeta input;
  private boolean backupChanged;
  private TableView wFields;

  public SasInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (SasInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SASInputDialog.Dialog.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
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

    // Which field do we read from?
    //
    Label wlAccField = new Label(shell, SWT.RIGHT);
    wlAccField.setText(BaseMessages.getString(PKG, "SASInputDialog.AcceptField.Label"));
    props.setLook(wlAccField);
    FormData fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment(lastControl, margin);
    fdlAccField.left = new FormAttachment(0, 0);
    fdlAccField.right = new FormAttachment(middle, -margin);
    wlAccField.setLayoutData(fdlAccField);
    wAccField = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAccField.setToolTipText(BaseMessages.getString(PKG, "SASInputDialog.AcceptField.Tooltip"));
    props.setLook(wAccField);
    FormData fdAccField = new FormData();
    fdAccField.top = new FormAttachment(lastControl, margin);
    fdAccField.left = new FormAttachment(middle, 0);
    fdAccField.right = new FormAttachment(100, 0);
    wAccField.setLayoutData(fdAccField);
    lastControl = wAccField;

    // Fill in the source fields...
    //
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      wAccField.setItems(fields.getFieldNames());
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Couldn't get input fields for transform '" + transformMeta + "'", e);
    }

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> get());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "SASInputDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.top = new FormAttachment(lastControl, margin);
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(100, 0);
    wlFields.setLayoutData(fdlFields);
    lastControl = wlFields;

    // Fields
    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Rename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Mask"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SASInputDialog.OutputFieldColumn.TrimType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaString.trimTypeDesc),
        };

    colinf[3].setComboValuesSelectionListener(
        (tableItem, rowNr, colNr) -> {
          String[] comboValues = new String[] {};
          int type = ValueMetaFactory.getIdForValueMeta(tableItem.getText(colNr - 1));
          switch (type) {
            case IValueMeta.TYPE_DATE:
              comboValues = Const.getDateFormats();
              break;
            case IValueMeta.TYPE_INTEGER:
            case IValueMeta.TYPE_BIGNUMBER:
            case IValueMeta.TYPE_NUMBER:
              comboValues = Const.getNumberFormats();
              break;
            default:
              break;
          }
          return comboValues;
        });

    wFields =
        new TableView(variables, shell, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props);

    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(lastControl, margin * 2);
    fdFields.bottom = new FormAttachment(wOk, -margin * 2);
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wAccField.setText(Const.NVL(input.getAcceptingField(), ""));

    for (int i = 0; i < input.getOutputFields().size(); i++) {
      SasInputField field = input.getOutputFields().get(i);

      TableItem item = new TableItem(wFields.table, SWT.NONE);
      int colnr = 1;
      item.setText(colnr++, Const.NVL(field.getName(), ""));
      item.setText(colnr++, Const.NVL(field.getRename(), ""));
      item.setText(colnr++, ValueMetaFactory.getValueMetaName(field.getType()));
      item.setText(colnr++, Const.NVL(field.getConversionMask(), ""));
      item.setText(colnr++, field.getLength() >= 0 ? Integer.toString(field.getLength()) : "");
      item.setText(
          colnr++, field.getPrecision() >= 0 ? Integer.toString(field.getPrecision()) : "");
      item.setText(colnr++, Const.NVL(field.getDecimalSymbol(), ""));
      item.setText(colnr++, Const.NVL(field.getGroupingSymbol(), ""));
      item.setText(colnr++, Const.NVL(field.getTrimTypeDesc(), ""));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  public void getInfo(SasInputMeta meta) throws HopTransformException {
    // copy info to Meta class (input)
    meta.setAcceptingField(wAccField.getText());

    int nrNonEmptyFields = wFields.nrNonEmpty();
    meta.getOutputFields().clear();

    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      int colnr = 1;
      SasInputField field = new SasInputField();
      field.setName(item.getText(colnr++));
      field.setRename(item.getText(colnr++));
      if (Utils.isEmpty(field.getRename())) {
        field.setRename(field.getName());
      }
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(colnr++)));
      field.setConversionMask(item.getText(colnr++));
      field.setLength(Const.toInt(item.getText(colnr++), -1));
      field.setPrecision(Const.toInt(item.getText(colnr++), -1));
      field.setDecimalSymbol(item.getText(colnr++));
      field.setGroupingSymbol(item.getText(colnr++));
      field.setTrimType(ValueMetaString.getTrimTypeByDesc(item.getText(colnr++)));

      meta.getOutputFields().add(field);
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    try {
      transformName = wTransformName.getText(); // return value
      getInfo(input);
    } catch (HopTransformException e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(e.toString());
      mb.setText(BaseMessages.getString(PKG, "System.Warning"));
      mb.open();
    }
    dispose();
  }

  public void get() {
    try {

      // As the user for a file to use as a reference
      //
      String filename =
          BaseDialog.presentFileDialog(
              shell,
              new String[] {"*.sas7bdat;*.SAS7BDAT", "*.*"},
              new String[] {
                BaseMessages.getString(PKG, "SASInputDialog.FileType.SAS7BAT")
                    + ", "
                    + BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
                BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                BaseMessages.getString(PKG, "System.FileType.AllFiles")
              },
              true);
      if (filename != null) {
        String realFilename = variables.resolve(filename);
        try (InputStream inputStream = HopVfs.getInputStream(realFilename)) {
          SasFileReaderImpl sasFileReader = new SasFileReaderImpl(inputStream);
          SasFileProperties fileProperties = sasFileReader.getSasFileProperties();

          List<Column> columns = sasFileReader.getColumns();
          for (int c = 0; c < columns.size(); c++) {
            Column column = columns.get(c);
            ColumnFormat format = column.getFormat();

            int length = format.getWidth() == 0 ? -1 : format.getWidth();
            int precision = format.getPrecision() == 0 ? -1 : format.getWidth();

            TableItem item = new TableItem(wFields.table, SWT.NONE);
            int colNr = 1;
            item.setText(colNr++, Const.NVL(column.getName(), ""));
            item.setText(colNr++, "");
            item.setText(colNr++, SasUtil.getHopDataTypeDesc(column.getType()));
            item.setText(colNr++, "");
            item.setText(colNr++, length < 0 ? "" : Integer.toString(length));
            item.setText(colNr++, precision < 0 ? "" : Integer.toString(precision));
            item.setText(colNr++, "");
            item.setText(colNr++, "");
            item.setText(colNr++, "");
          }
        } catch (Exception e) {
          throw new HopException("Error reading from file: " + realFilename);
        }
      }

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error reading information from file", e);
    }
  }
}
