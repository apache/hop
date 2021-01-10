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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Displays the meta-data on the Values in a row as well as the Transform origin of the Value.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class TransformFieldsDialog extends Dialog {
  private static final Class<?> PKG = TransformFieldsDialog.class; // For Translator

  private TableView wFields;

  private final IRowMeta input;

  private Shell shell;

  private final PropsUi props;

  private String transformName;

  private final IVariables variables;

  private String shellText;

  private String originText;

  private boolean showEditButton = true;

  public TransformFieldsDialog(
      Shell parent, IVariables variables, int style, String transformName, IRowMeta input) {
    super(parent, style);
    this.transformName = transformName;
    this.input = input;
    this.variables = variables;
    props = PropsUi.getInstance();

    shellText = BaseMessages.getString(PKG, "TransformFieldsDialog.Title");
    originText = BaseMessages.getString(PKG, "TransformFieldsDialog.Name.Label");
    showEditButton = true;
  }

  public Object open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImagePipeline());
    shell.setText(shellText);

    int margin = props.getMargin();

    // Buttons at the bottom
    //
    Button[] buttons;
    if (showEditButton) {
      Button wEdit = new Button(shell, SWT.PUSH);
      wEdit.setText(BaseMessages.getString(PKG, "TransformFieldsDialog.Buttons.EditOrigin"));
      wEdit.addListener(SWT.Selection, e -> edit());
      Button wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      wCancel.addListener(SWT.Selection, e -> cancel());

      buttons = new Button[] {wEdit, wCancel};
    } else {
      Button wClose = new Button(shell, SWT.PUSH);
      wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
      wClose.addListener(SWT.Selection, e -> cancel());
      buttons = new Button[] {wClose};
    }

    BaseTransformDialog.positionBottomButtons(shell, buttons, margin, null);

    // Filename line
    Label wlTransformName = new Label(shell, SWT.NONE);
    wlTransformName.setText(originText);
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    Text wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.READ_ONLY);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "TransformFieldsDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wlTransformName, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Fieldname"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Type"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.ConversionMask"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Origin"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.StorageType"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.TrimType"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TransformFieldsDialog.TableCol.Comments"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            true, // read-only
            null,
            props);
    wFields.optWidth(true);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(buttons[0], -margin * 2);
    wFields.setLayoutData(fdFields);

    SelectionAdapter lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            edit();
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

    wFields.table.addMouseListener(
        new MouseListener() {
          @Override
          public void mouseDoubleClick(MouseEvent arg0) {
            edit();
          }

          @Override
          public void mouseDown(MouseEvent arg0) {}

          @Override
          public void mouseUp(MouseEvent arg0) {}
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    int i;

    for (i = 0; i < input.size(); i++) {
      TableItem item = wFields.table.getItem(i);
      IValueMeta v = input.getValueMeta(i);
      int idx = 1;
      if (v.getName() != null) {
        item.setText(idx++, v.getName());
      }
      item.setText(idx++, v.getTypeDesc());
      item.setText(idx++, Const.NVL(v.getConversionMask(), ""));
      item.setText(idx++, v.getLength() < 0 ? "-" : "" + v.getLength());
      item.setText(idx++, v.getPrecision() < 0 ? "-" : "" + v.getPrecision());
      item.setText(idx++, Const.NVL(v.getOrigin(), ""));
      item.setText(idx++, ValueMetaBase.getStorageTypeCode(v.getStorageType()));
      item.setText(idx++, Const.NVL(v.getCurrencySymbol(), ""));
      item.setText(idx++, Const.NVL(v.getDecimalSymbol(), ""));
      item.setText(idx++, Const.NVL(v.getGroupingSymbol(), ""));
      item.setText(idx++, ValueMetaBase.getTrimTypeDesc(v.getTrimType()));
      item.setText(idx++, Const.NVL(v.getComments(), ""));
    }
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void edit() {
    int idx = wFields.table.getSelectionIndex();
    if (idx >= 0) {
      transformName = wFields.table.getItem(idx).getText(5);
      dispose();
    } else {
      transformName = null;
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "TransformFieldsDialog.OriginTransform.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "TransformFieldsDialog.OriginTransform.Message"));
      mb.open();
    }
  }

  public String getShellText() {
    return shellText;
  }

  public void setShellText(String shellText) {
    this.shellText = shellText;
  }

  public String getOriginText() {
    return originText;
  }

  public void setOriginText(String originText) {
    this.originText = originText;
  }

  public boolean isShowEditButton() {
    return showEditButton;
  }

  public void setShowEditButton(boolean showEditButton) {
    this.showEditButton = showEditButton;
  }
}
