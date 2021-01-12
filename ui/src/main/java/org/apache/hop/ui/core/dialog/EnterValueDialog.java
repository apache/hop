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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
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
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog to enter a Hop Value
 *
 * @author Matt
 * @since 01-11-2004
 */
public class EnterValueDialog extends Dialog {
  private static final Class<?> PKG = EnterValueDialog.class; // For Translator

  private Display display;

  /*
   * Type of Value: String, Number, Date, Boolean, Integer
   */
  private Label wlValueType;

  private CCombo wValueType;

  private FormData fdlValueType, fdValueType;

  private Label wlInputString;

  private Text wInputString;

  private FormData fdlInputString, fdInputString;

  private Label wlFormat;

  private CCombo wFormat;

  private FormData fdlFormat, fdFormat;

  private Label wlLength;

  private Text wLength;

  private FormData fdlLength, fdLength;

  private Label wlPrecision;

  private Text wPrecision;

  private FormData fdlPrecision, fdPrecision;
  private Button wOk, wCancel, wTest;
  private Listener lsOk, lsCancel, lsTest;

  private Shell shell;

  private SelectionAdapter lsDef;

  private PropsUi props;

  private ValueMetaAndData valueMetaAndData;

  private IValueMeta valueMeta;

  private Object valueData;

  private boolean modalDialog;

  public EnterValueDialog(Shell parent, int style, IValueMeta value, Object data) {
    super(parent, style);
    this.props = PropsUi.getInstance();
    this.valueMeta = value;
    this.valueData = data;
  }

  public ValueMetaAndData open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM
                | SWT.RESIZE
                | (modalDialog ? SWT.APPLICATION_MODAL | SWT.SHEET : SWT.NONE));
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "EnterValueDialog.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Type of value
    wlValueType = new Label(shell, SWT.RIGHT);
    wlValueType.setText(BaseMessages.getString(PKG, "EnterValueDialog.Type.Label"));
    props.setLook(wlValueType);
    fdlValueType = new FormData();
    fdlValueType.left = new FormAttachment(0, 0);
    fdlValueType.right = new FormAttachment(middle, -margin);
    fdlValueType.top = new FormAttachment(0, margin);
    wlValueType.setLayoutData(fdlValueType);
    wValueType = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    wValueType.setItems(ValueMetaFactory.getValueMetaNames());
    props.setLook(wValueType);
    fdValueType = new FormData();
    fdValueType.left = new FormAttachment(middle, 0);
    fdValueType.top = new FormAttachment(0, margin);
    fdValueType.right = new FormAttachment(100, -margin);
    wValueType.setLayoutData(fdValueType);
    wValueType.addModifyListener(arg0 -> setFormats());

    // Value line
    wlInputString = new Label(shell, SWT.RIGHT);
    wlInputString.setText(BaseMessages.getString(PKG, "EnterValueDialog.Value.Label"));
    props.setLook(wlInputString);
    fdlInputString = new FormData();
    fdlInputString.left = new FormAttachment(0, 0);
    fdlInputString.right = new FormAttachment(middle, -margin);
    fdlInputString.top = new FormAttachment(wValueType, margin);
    wlInputString.setLayoutData(fdlInputString);
    wInputString = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInputString);
    fdInputString = new FormData();
    fdInputString.left = new FormAttachment(middle, 0);
    fdInputString.top = new FormAttachment(wValueType, margin);
    fdInputString.right = new FormAttachment(100, -margin);
    wInputString.setLayoutData(fdInputString);

    // Format mask
    wlFormat = new Label(shell, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "EnterValueDialog.ConversionFormat.Label"));
    props.setLook(wlFormat);
    fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.right = new FormAttachment(middle, -margin);
    fdlFormat.top = new FormAttachment(wInputString, margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFormat);
    fdFormat = new FormData();
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.right = new FormAttachment(100, -margin);
    fdFormat.top = new FormAttachment(wInputString, margin);
    wFormat.setLayoutData(fdFormat);

    // Length line
    wlLength = new Label(shell, SWT.RIGHT);
    wlLength.setText(BaseMessages.getString(PKG, "EnterValueDialog.Length.Label"));
    props.setLook(wlLength);
    fdlLength = new FormData();
    fdlLength.left = new FormAttachment(0, 0);
    fdlLength.right = new FormAttachment(middle, -margin);
    fdlLength.top = new FormAttachment(wFormat, margin);
    wlLength.setLayoutData(fdlLength);
    wLength = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLength);
    fdLength = new FormData();
    fdLength.left = new FormAttachment(middle, 0);
    fdLength.right = new FormAttachment(100, -margin);
    fdLength.top = new FormAttachment(wFormat, margin);
    wLength.setLayoutData(fdLength);

    // Precision line
    wlPrecision = new Label(shell, SWT.RIGHT);
    wlPrecision.setText(BaseMessages.getString(PKG, "EnterValueDialog.Precision.Label"));
    props.setLook(wlPrecision);
    fdlPrecision = new FormData();
    fdlPrecision.left = new FormAttachment(0, 0);
    fdlPrecision.right = new FormAttachment(middle, -margin);
    fdlPrecision.top = new FormAttachment(wLength, margin);
    wlPrecision.setLayoutData(fdlPrecision);
    wPrecision = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrecision);
    fdPrecision = new FormData();
    fdPrecision.left = new FormAttachment(middle, 0);
    fdPrecision.right = new FormAttachment(100, -margin);
    fdPrecision.top = new FormAttachment(wLength, margin);
    wPrecision.setLayoutData(fdPrecision);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wTest = new Button(shell, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wTest, wCancel}, margin, wPrecision);

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();
    lsTest = e -> test();

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);
    wTest.addListener(SWT.Selection, lsTest);

    lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wInputString.addSelectionListener(lsDef);
    wLength.addSelectionListener(lsDef);
    wPrecision.addSelectionListener(lsDef);

    // If the user changes data type or if we type a text, we set the default mask for the type
    // We also set the list of possible masks in the wFormat
    //
    wInputString.addFocusListener(
        new FocusListener() {
          @Override
          public void focusGained(FocusEvent focusEvent) {}

          @Override
          public void focusLost(FocusEvent focusEvent) {
            setFormats();
          }
        });

    wValueType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            setFormats();
          }
        });

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return valueMetaAndData;
  }

  protected void setFormats() {
    // What is the selected type?
    //
    // The index must be set on the combobox after
    // calling setItems(), otherwise the zeroth element
    // is displayed, but the selectedIndex will be -1.

    int formatIndex = wFormat.getSelectionIndex();
    String formatString = formatIndex >= 0 ? wFormat.getItem(formatIndex) : "";
    int type = ValueMetaFactory.getIdForValueMeta(wValueType.getText());
    String string = wInputString.getText();

    // remove white spaces if not a string field
    if ((type != IValueMeta.TYPE_STRING) && (string.startsWith(" ") || string.endsWith(" "))) {
      string = Const.trim(string);
      wInputString.setText(string);
    }
    switch (type) {
      case IValueMeta.TYPE_INTEGER:
        wFormat.setItems(Const.getNumberFormats());
        int index =
            (!Utils.isEmpty(formatString)) ? wFormat.indexOf(formatString) : wFormat.indexOf("#");
        // ... then we have a custom format mask
        if ((!Utils.isEmpty(formatString)) && (index < 0)) {
          wFormat.add(formatString);
          index = wFormat.indexOf(formatString);
        }
        wFormat.select(index); // default
        break;
      case IValueMeta.TYPE_NUMBER:
        wFormat.setItems(Const.getNumberFormats());
        index =
            (!Utils.isEmpty(formatString)) ? wFormat.indexOf(formatString) : wFormat.indexOf("#.#");
        // ... then we have a custom format mask
        if ((!Utils.isEmpty(formatString)) && (index < 0)) {
          wFormat.add(formatString);
          index = wFormat.indexOf(formatString);
        }
        wFormat.select(index); // default
        break;
      case IValueMeta.TYPE_DATE:
        wFormat.setItems(Const.getDateFormats());
        index =
            (!Utils.isEmpty(formatString))
                ? wFormat.indexOf(formatString)
                : wFormat.indexOf("yyyy/MM/dd HH:mm:ss"); // default;
        // ... then we have a custom format mask
        if ((!Utils.isEmpty(formatString)) && (index < 0)) {
          wFormat.add(formatString);
          index = wFormat.indexOf(formatString);
        }
        wFormat.select(index); // default
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        wFormat.setItems(new String[] {});
        break;
      default:
        wFormat.setItems(new String[] {});
        break;
    }
  }

  public void dispose() {
    shell.dispose();
  }

  public void getData() {
    wValueType.setText(valueMeta.getTypeDesc());
    try {
      if (valueData != null) {
        String value = valueMeta.getString(valueData);
        if (value != null) {
          wInputString.setText(value);
        }
      }
    } catch (HopValueException e) {
      wInputString.setText(valueMeta.toString());
    }
    setFormats();

    int index = -1;
    // If there is a custom conversion mask set,
    // we need to add that mask to the combo box
    if (!Utils.isEmpty(valueMeta.getConversionMask())) {
      index = wFormat.indexOf(valueMeta.getConversionMask());
      if (index < 0) {
        wFormat.add(valueMeta.getConversionMask());
        index = wFormat.indexOf(valueMeta.getConversionMask());
      }
    }
    if (index >= 0) {
      wFormat.select(index);
    }

    wLength.setText(Integer.toString(valueMeta.getLength()));
    wPrecision.setText(Integer.toString(valueMeta.getPrecision()));

    setFormats();

    wInputString.setFocus();
    wInputString.selectAll();
  }

  private void cancel() {
    props.setScreen(new WindowProperty(shell));
    valueMeta = null;
    dispose();
  }

  private ValueMetaAndData getValue(String valuename) throws HopValueException {
    try {
      int valtype = ValueMetaFactory.getIdForValueMeta(wValueType.getText());
      ValueMetaAndData val = new ValueMetaAndData(valuename, wInputString.getText());

      IValueMeta valueMeta = ValueMetaFactory.cloneValueMeta(val.getValueMeta(), valtype);
      Object valueData = val.getValueData();

      int formatIndex = wFormat.getSelectionIndex();
      valueMeta.setConversionMask(
          formatIndex >= 0 ? wFormat.getItem(formatIndex) : wFormat.getText());
      valueMeta.setLength(Const.toInt(wLength.getText(), -1));
      valueMeta.setPrecision(Const.toInt(wPrecision.getText(), -1));
      val.setValueMeta(valueMeta);

      IValueMeta stringValueMeta = new ValueMetaString(valuename);
      stringValueMeta.setConversionMetadata(valueMeta);

      Object targetData = stringValueMeta.convertDataUsingConversionMetaData(valueData);
      val.setValueData(targetData);

      return val;
    } catch (Exception e) {
      throw new HopValueException(e);
    }
  }

  private void ok() {
    try {
      valueMetaAndData = getValue(valueMeta.getName()); // Keep the same name...
      dispose();
    } catch (HopValueException e) {
      new ErrorDialog(shell, "Error", "There was a conversion error: ", e);
    }
  }

  /** Test the entered value */
  public void test() {
    try {
      ValueMetaAndData v = getValue(valueMeta.getName());
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);

      StringBuilder result = new StringBuilder();
      result.append(Const.CR).append(Const.CR).append("    ").append(v.toString());
      result.append(Const.CR).append("    ").append(v.toStringMeta());

      mb.setMessage(
          BaseMessages.getString(PKG, "EnterValueDialog.TestResult.Message", result.toString()));
      mb.setText(BaseMessages.getString(PKG, "EnterValueDialog.TestResult.Title"));
      mb.open();
    } catch (HopValueException e) {
      new ErrorDialog(shell, "Error", "There was an error during data type conversion: ", e);
    }
  }

  /** @return the modalDialog */
  public boolean isModalDialog() {
    return modalDialog;
  }

  /** @param modalDialog the modalDialog to set */
  public void setModalDialog(boolean modalDialog) {
    this.modalDialog = modalDialog;
  }
}
