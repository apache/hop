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

package org.apache.hop.neo4j.transforms.loginfo;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class GetLoggingInfoDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = GetLoggingInfo.class; // for i18n purposes, needed by Translator2!!

  private Label wlTransformname;
  private Text wTransformname;
  private FormData fdlTransformname, fdTransformname;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private GetLoggingInfoMeta input;

  private boolean isReceivingInput = false;

  public GetLoggingInfoDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (GetLoggingInfoMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod =
        new ModifyListener() {
          public void modifyText(ModifyEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "GetLoggingInfoDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // See if the transform receives input.
    //
    isReceivingInput = pipelineMeta.findNrPrevTransforms(transformMeta) > 0;

    // Transformname line
    wlTransformname = new Label(shell, SWT.RIGHT);
    wlTransformname.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformname);
    fdlTransformname = new FormData();
    fdlTransformname.left = new FormAttachment(0, 0);
    fdlTransformname.right = new FormAttachment(middle, -margin);
    fdlTransformname.top = new FormAttachment(0, margin);
    wlTransformname.setLayoutData(fdlTransformname);
    wTransformname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformname.setText(transformName);
    props.setLook(wTransformname);
    wTransformname.addModifyListener(lsMod);
    fdTransformname = new FormData();
    fdTransformname.left = new FormAttachment(middle, 0);
    fdTransformname.top = new FormAttachment(0, margin);
    fdTransformname.right = new FormAttachment(100, 0);
    wTransformname.setLayoutData(fdTransformname);

    wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "GetLoggingInfoDialog.Fields.Label"));
    props.setLook(wlFields);
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wTransformname, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsCols = 3;
    final int FieldsRows = input.getFieldName().length;

    final String[] functionDesc = new String[GetLoggingInfoTypes.values().length - 1];
    for (int i = 1; i < GetLoggingInfoTypes.values().length; i++) {
      functionDesc[i - 1] = GetLoggingInfoTypes.values()[i].lookupDescription();
    }

    ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetLoggingInfoDialog.NameColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetLoggingInfoDialog.TypeColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1].setSelectionAdapter(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            EnterSelectionDialog esd =
                new EnterSelectionDialog(
                    shell,
                    functionDesc,
                    BaseMessages.getString(PKG, "GetLoggingInfoDialog.SelectInfoType.DialogTitle"),
                    BaseMessages.getString(
                        PKG, "GetLoggingInfoDialog.SelectInfoType.DialogMessage"));
            String string = esd.open();
            if (string != null) {
              TableView tv = (TableView) e.widget;
              tv.setText(string, e.x, e.y);
            }
            input.setChanged();
          }
        });
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetLoggingInfoDialog.ArgumentColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2].setUsingVariables(true);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -50);
    wFields.setLayoutData(fdFields);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "GetLoggingInfoDialog.Button.PreviewRows"));
    wPreview.setEnabled(!isReceivingInput);
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, wFields);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview.addListener(SWT.Selection, e -> preview());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformname.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTransformname.setText(transformName);

    for (int i = 0; i < input.getFieldName().length; i++) {
      TableItem item = wFields.table.getItem(i);

      item.setText(1, Const.NVL(input.getFieldName()[i], ""));
      item.setText(2, Const.NVL(input.getFieldType()[i].lookupDescription(), ""));
      item.setText(3, Const.NVL(input.getFieldArgument()[i], ""));
    }

    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformname.selectAll();
    wTransformname.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformname.getText())) {
      return;
    }

    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetLoggingInfoDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "GetLoggingInfoDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  private void getInfo(GetLoggingInfoMeta in) throws HopException {

    transformName = wTransformname.getText(); // return value
    int count = wFields.nrNonEmpty();
    in.allocate(count);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < count; i++) {
      TableItem item = wFields.getNonEmpty(i);
      in.getFieldName()[i] = item.getText(1);
      in.getFieldType()[i] = GetLoggingInfoTypes.getTypeFromString(item.getText(2));
      in.getFieldArgument()[i] = item.getText(3);
    }
  }

  // Preview the data
  private void preview() {
    try {
      GetLoggingInfoMeta oneMeta = new GetLoggingInfoMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
            metadataProvider, oneMeta, wTransformname.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "GetLoggingInfoDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "GetLoggingInfoDialog.NumberRows.DialogMessage"));

      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
                shell,
                variables,
                previewMeta,
                new String[] {wTransformname.getText()},
                new int[] {previewSize});
        progressDialog.open();

        if (!progressDialog.isCancelled()) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
            EnterTextDialog etd =
                new EnterTextDialog(
                    shell,
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                    loggingText,
                    true);
            etd.setReadOnly();
            etd.open();
          }

          PreviewRowsDialog prd =
              new PreviewRowsDialog(
                  shell,
                  variables,
                  SWT.NONE,
                  wTransformname.getText(),
                  progressDialog.getPreviewRowsMeta(wTransformname.getText()),
                  progressDialog.getPreviewRows(wTransformname.getText()),
                  loggingText);
          prd.open();
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetLoggingInfoDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "GetLoggingInfoDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }
}
