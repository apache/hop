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
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Shell;

import java.util.List;

/**
 * Displays an ArrayList of rows in a TableView and allows you to select one.
 *
 * @author Matt
 */
public class SelectRowDialog extends Dialog {
  private static final Class<?> PKG = SelectRowDialog.class; // For Translator

  private Label wlFields;
  private TableView wFields;
  private FormData fdFields;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private Shell shell;
  private List<RowMetaAndData> buffer;
  private PropsUi props;
  private String title;

  private RowMetaAndData selection;
  private IRowMeta rowMeta;

  private IVariables variables;

  /**
   * @param parent
   * @param style
   * @param buffer
   */
  public SelectRowDialog(
      Shell parent, IVariables variables, int style, List<RowMetaAndData> buffer) {
    super(parent, style);
    this.buffer = buffer;
    this.variables = variables;
    props = PropsUi.getInstance();

    selection = null;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public RowMetaAndData open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    if (title == null) {
      title = BaseMessages.getString(PKG, "SelectRowDialog.Title");
    }

    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImagePipeline());
    shell.setText(title);

    int margin = props.getMargin();

    // Simply exit and close in case we don't have anything to edit or show
    //
    if (buffer == null || buffer.size() == 0) {
      return null;
    }

    rowMeta = buffer.get(0).getRowMeta();

    int FieldsRows = buffer.size();

    ColumnInfo[] colinf = new ColumnInfo[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta v = rowMeta.getValueMeta(i);
      colinf[i] = new ColumnInfo(v.getName(), ColumnInfo.COLUMN_TYPE_TEXT, false);
      colinf[i].setToolTip(v.toStringMeta());
      colinf[i].setReadOnly(true);
    }

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            null,
            props);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    // Add listeners
    lsOk = e -> ok();
    wOk.addListener(SWT.Selection, lsOk);

    lsCancel = e -> close();
    wCancel.addListener(SWT.Selection, lsCancel);

    wFields.table.addListener(SWT.DefaultSelection, e -> ok());

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            close();
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
    return selection;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /** Copy information from the input buffer to the dialog fields. */
  private void getData() {
    for (int i = 0; i < buffer.size(); i++) {
      RowMetaAndData rowMetaAndData = buffer.get(i);
      IRowMeta rowMeta = rowMetaAndData.getRowMeta();
      Object[] rowData = rowMetaAndData.getData();

      for (int c = 0; c < rowMeta.size(); c++) {
        IValueMeta v = rowMeta.getValueMeta(c);
        String show;

        try {
          if (v.isNumeric()) {
            show = v.getString(rowData[c]);
          } else {
            show = v.getString(rowData[c]);
          }
        } catch (HopValueException e) {
          show = "<conversion error>";
        }
        if (show != null) {
          wFields.table.getItem(i).setText(c + 1, show);
        }
      }
    }
    wFields.optWidth(true);
  }

  private void close() {
    selection = null;
    dispose();
  }

  private void ok() {
    int[] idx = wFields.getSelectionIndices();
    if (idx.length > 0 && idx[0] < buffer.size()) {
      selection = buffer.get(idx[0]);
    }
    dispose();
  }
}
