/*******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.standardizephonenumber;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.List;

public class StandardizePhoneNumberDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = StandardizePhoneNumberMeta.class; // for i18n
  private final StandardizePhoneNumberMeta input; // purposes
  private TableView wFields;

  public StandardizePhoneNumberDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (StandardizePhoneNumberMeta) in;
  }

  protected void getData() {

    // Fields
    List<StandardizePhoneField> standardizes = input.getFields();
    if (standardizes.size() > 0) {
      Table table = wFields.getTable();
      // table.removeAll();
      for (int i = 0; i < standardizes.size(); i++) {
        StandardizePhoneField standardize = standardizes.get(i);
        TableItem item = new TableItem(table, SWT.NONE);
        item.setText(1, StringUtils.stripToEmpty(standardize.getInputField()));
        item.setText(2, StringUtils.stripToEmpty(standardize.getOutputField()));
        item.setText(3, StringUtils.stripToEmpty(standardize.getCountryField()));
        item.setText(4, StringUtils.stripToEmpty(standardize.getDefaultCountry()));
        item.setText(5, StringUtils.stripToEmpty(standardize.getNumberFormat()));
        item.setText(6, StringUtils.stripToEmpty(standardize.getNumberTypeField()));
        item.setText(7, StringUtils.stripToEmpty(standardize.getIsValidNumberField()));
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setText(BaseMessages.getString(PKG, "StandardizePhoneNumberDialog.Shell.Title"));
    shell.setMinimumSize(650, 350);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    int margin = props.getMargin();

    // The buttons at the bottom of the dialog
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    Label hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    hSpacer.setLayoutData(
        new FormDataBuilder().left().right().bottom(wOk, -margin).height(2).result());

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    wlTransformName.setLayoutData(new FormDataBuilder().left().top().result());
    props.setLook(wlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    wTransformName.addListener(SWT.Modify, e -> input.setChanged());
    wTransformName.setLayoutData(
        new FormDataBuilder().left().top(wlTransformName, margin).right(100, 0).result());
    props.setLook(wTransformName);

    // Table with fields
    Label lblFields = new Label(shell, SWT.LEFT);
    lblFields.setText(BaseMessages.getString(PKG, "StandardizePhoneNumberDialog.Fields.Label"));
    lblFields.setLayoutData(new FormDataBuilder().top(wTransformName, margin).fullWidth().result());
    props.setLook(lblFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.InputField.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.OutputField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.CountryField.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.DefaultCountry.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              input.getSupportedCountries(),
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.NumberFormat.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              input.getSupportedFormats(),
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.NumberTypeField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberDialog.ColumnInfo.IsValidNumberField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              new String[] {""},
              false)
        };

    columns[0].setToolTip(
        BaseMessages.getString(PKG, "StandardizePhoneNumberDialog.ColumnInfo.InputField.Tooltip"));
    columns[1].setToolTip(
        BaseMessages.getString(PKG, "StandardizePhoneNumberDialog.ColumnInfo.OutputField.Tooltip"));
    columns[1].setUsingVariables(true);
    columns[2].setToolTip(
        BaseMessages.getString(
            PKG, "StandardizePhoneNumberDialog.ColumnInfo.CountryField.Tooltip"));
    columns[3].setToolTip(
        BaseMessages.getString(
            PKG, "StandardizePhoneNumberDialog.ColumnInfo.DefaultCountry.Tooltip"));
    columns[4].setToolTip(
        BaseMessages.getString(
            PKG, "StandardizePhoneNumberDialog.ColumnInfo.NumberFormat.Tooltip"));
    columns[5].setUsingVariables(true);
    columns[5].setToolTip(
        BaseMessages.getString(
            PKG, "StandardizePhoneNumberDialog.ColumnInfo.NumberTypeField.Tooltip"));
    columns[6].setUsingVariables(true);
    columns[6].setToolTip(
        BaseMessages.getString(
            PKG, "StandardizePhoneNumberDialog.ColumnInfo.IsValidNumberField.Tooltip"));

    wFields =
        new TableView(
            this.getVariables(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            e -> input.setChanged(),
            props);
    wFields.setLayoutData(
        new FormDataBuilder()
            .left()
            .fullWidth()
            .top(lblFields, margin)
            .bottom(hSpacer, -margin)
            .result());
    wFields.getTable().addListener(SWT.Resize, new ColumnsResizer(2, 20, 20, 10, 12, 12, 12, 8));

    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              final List<String> inputFields = new ArrayList<>();

              if (rowMeta != null) {

                for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
                  inputFields.add(valueMeta.getName());
                }

                // Sort by name
                String[] fieldNames = Const.sortStrings(inputFields.toArray(new String[0]));
                columns[0].setComboValues(fieldNames);
                columns[2].setComboValues(fieldNames);
              }

              // Display in red missing field names
              Display.getDefault()
                  .asyncExec(
                      new Runnable() {
                        public void run() {
                          if (!wFields.isDisposed()) {
                            for (int i = 0; i < wFields.table.getItemCount(); i++) {
                              TableItem item = wFields.table.getItem(i);

                              // Input field
                              if (!Utils.isEmpty(item.getText(1))) {
                                if (!inputFields.contains(item.getText(1))) {
                                  item.setBackground(GuiResource.getInstance().getColorRed());
                                }
                              }

                              // Country field
                              if (!Utils.isEmpty(item.getText(3))) {
                                if (!inputFields.contains(item.getText(3))) {
                                  item.setBackground(GuiResource.getInstance().getColorRed());
                                }
                              }
                            }
                          }
                        }
                      });

            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
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

    // Save transform name
    transformName = wTransformName.getText();

    List<StandardizePhoneField> standardizes = new ArrayList<>();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);

      StandardizePhoneField standardize = new StandardizePhoneField();
      standardize.setInputField(StringUtils.stripToNull(item.getText(1)));
      standardize.setOutputField(StringUtils.stripToNull(item.getText(2)));
      standardize.setCountryField(StringUtils.stripToNull(item.getText(3)));
      standardize.setDefaultCountry(StringUtils.stripToNull(item.getText(4)));
      standardize.setNumberFormat(StringUtils.stripToNull(item.getText(5)));
      standardize.setNumberTypeField(item.getText(6));
      standardize.setIsValidNumberField(StringUtils.stripToNull(item.getText(7)));
      standardizes.add(standardize);
    }
    input.setFields(standardizes);

    dispose();
  }
}
