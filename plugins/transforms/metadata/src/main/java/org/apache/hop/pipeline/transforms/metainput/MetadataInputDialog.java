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

package org.apache.hop.pipeline.transforms.metainput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class MetadataInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MetadataInputMeta.class; // For Translator

  private Text wTransformName;

  private Text wProvider;
  private Text wTypeKey;
  private Text wTypeName;
  private Text wTypeDescription;
  private Text wTypeClass;
  private Text wName;
  private Text wJson;
  private TableView wTypeFilters;

  private final MetadataInputMeta input;

  public MetadataInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (MetadataInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MetadataInput.Transform.Name"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // See if the transform receives input.
    //
    boolean isReceivingInput = pipelineMeta.findPreviousTransforms(transformMeta).size() > 0;

    // TransformName line
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // Provider
    //
    Label wlProvider = new Label(shell, SWT.RIGHT);
    wlProvider.setText(BaseMessages.getString(PKG, "MetadataInputDialog.ProviderField.Label"));
    props.setLook(wlProvider);
    FormData fdlProvider = new FormData();
    fdlProvider.left = new FormAttachment(0, 0);
    fdlProvider.right = new FormAttachment(middle, -margin);
    fdlProvider.top = new FormAttachment(lastControl, margin);
    wlProvider.setLayoutData(fdlProvider);
    wProvider = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wProvider);
    wProvider.addModifyListener(lsMod);
    FormData fdProvider = new FormData();
    fdProvider.left = new FormAttachment(middle, 0);
    fdProvider.top = new FormAttachment(lastControl, margin);
    fdProvider.right = new FormAttachment(100, 0);
    wProvider.setLayoutData(fdProvider);
    lastControl = wProvider;

    // TypeKey
    //
    Label wlTypeKey = new Label(shell, SWT.RIGHT);
    wlTypeKey.setText(BaseMessages.getString(PKG, "MetadataInputDialog.TypeKeyField.Label"));
    props.setLook(wlTypeKey);
    FormData fdlTypeKey = new FormData();
    fdlTypeKey.left = new FormAttachment(0, 0);
    fdlTypeKey.right = new FormAttachment(middle, -margin);
    fdlTypeKey.top = new FormAttachment(lastControl, margin);
    wlTypeKey.setLayoutData(fdlTypeKey);
    wTypeKey = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTypeKey);
    wTypeKey.addModifyListener(lsMod);
    FormData fdTypeKey = new FormData();
    fdTypeKey.left = new FormAttachment(middle, 0);
    fdTypeKey.top = new FormAttachment(lastControl, margin);
    fdTypeKey.right = new FormAttachment(100, 0);
    wTypeKey.setLayoutData(fdTypeKey);
    lastControl = wTypeKey;

    // TypeName
    //
    Label wlTypeName = new Label(shell, SWT.RIGHT);
    wlTypeName.setText(BaseMessages.getString(PKG, "MetadataInputDialog.TypeNameField.Label"));
    props.setLook(wlTypeName);
    FormData fdlTypeName = new FormData();
    fdlTypeName.left = new FormAttachment(0, 0);
    fdlTypeName.right = new FormAttachment(middle, -margin);
    fdlTypeName.top = new FormAttachment(lastControl, margin);
    wlTypeName.setLayoutData(fdlTypeName);
    wTypeName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTypeName);
    wTypeName.addModifyListener(lsMod);
    FormData fdTypeName = new FormData();
    fdTypeName.left = new FormAttachment(middle, 0);
    fdTypeName.top = new FormAttachment(lastControl, margin);
    fdTypeName.right = new FormAttachment(100, 0);
    wTypeName.setLayoutData(fdTypeName);
    lastControl = wTypeName;

    // TypeDescription
    //
    Label wlTypeDescription = new Label(shell, SWT.RIGHT);
    wlTypeDescription.setText(
        BaseMessages.getString(PKG, "MetadataInputDialog.TypeDescriptionField.Label"));
    props.setLook(wlTypeDescription);
    FormData fdlTypeDescription = new FormData();
    fdlTypeDescription.left = new FormAttachment(0, 0);
    fdlTypeDescription.right = new FormAttachment(middle, -margin);
    fdlTypeDescription.top = new FormAttachment(lastControl, margin);
    wlTypeDescription.setLayoutData(fdlTypeDescription);
    wTypeDescription = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTypeDescription);
    wTypeDescription.addModifyListener(lsMod);
    FormData fdTypeDescription = new FormData();
    fdTypeDescription.left = new FormAttachment(middle, 0);
    fdTypeDescription.top = new FormAttachment(lastControl, margin);
    fdTypeDescription.right = new FormAttachment(100, 0);
    wTypeDescription.setLayoutData(fdTypeDescription);
    lastControl = wTypeDescription;

    // TypeClass
    //
    Label wlTypeClass = new Label(shell, SWT.RIGHT);
    wlTypeClass.setText(BaseMessages.getString(PKG, "MetadataInputDialog.TypeClassField.Label"));
    props.setLook(wlTypeClass);
    FormData fdlTypeClass = new FormData();
    fdlTypeClass.left = new FormAttachment(0, 0);
    fdlTypeClass.right = new FormAttachment(middle, -margin);
    fdlTypeClass.top = new FormAttachment(lastControl, margin);
    wlTypeClass.setLayoutData(fdlTypeClass);
    wTypeClass = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTypeClass);
    wTypeClass.addModifyListener(lsMod);
    FormData fdTypeClass = new FormData();
    fdTypeClass.left = new FormAttachment(middle, 0);
    fdTypeClass.top = new FormAttachment(lastControl, margin);
    fdTypeClass.right = new FormAttachment(100, 0);
    wTypeClass.setLayoutData(fdTypeClass);
    lastControl = wTypeClass;

    // Name
    //
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "MetadataInputDialog.NameField.Label"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(lastControl, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(lastControl, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    lastControl = wName;

    // JSON
    //
    Label wlJson = new Label(shell, SWT.RIGHT);
    wlJson.setText(BaseMessages.getString(PKG, "MetadataInputDialog.JsonField.Label"));
    props.setLook(wlJson);
    FormData fdlJson = new FormData();
    fdlJson.left = new FormAttachment(0, 0);
    fdlJson.right = new FormAttachment(middle, -margin);
    fdlJson.top = new FormAttachment(lastControl, margin);
    wlJson.setLayoutData(fdlJson);
    wJson = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wJson);
    wJson.addModifyListener(lsMod);
    FormData fdJson = new FormData();
    fdJson.left = new FormAttachment(middle, 0);
    fdJson.top = new FormAttachment(lastControl, margin);
    fdJson.right = new FormAttachment(100, 0);
    wJson.setLayoutData(fdJson);
    lastControl = wJson;

    // Some buttons at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(this.shell, 8);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wPreview.setEnabled(!isReceivingInput);
    wPreview.addListener(13, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    Label wlTypeFilters = new Label(shell, SWT.NONE);
    wlTypeFilters.setText(BaseMessages.getString(PKG, "MetadataInputDialog.TypeFilters.Label"));
    props.setLook(wlTypeFilters);
    FormData fdlTypeFilters = new FormData();
    fdlTypeFilters.left = new FormAttachment(0, 0);
    fdlTypeFilters.top = new FormAttachment(lastControl, margin);
    wlTypeFilters.setLayoutData(fdlTypeFilters);

    final int fieldsRows = input.getTypeKeyFilters().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MetadataInputDialog.KeyColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              HopMetadataUtil.getHopMetadataKeys(metadataProvider)),
        };

    colinf[0].setToolTip(BaseMessages.getString(PKG, "MetadataInputDialog.KeyColumn.Tooltip"));
    colinf[0].setUsingVariables(true);

    wTypeFilters =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            lsMod,
            props);

    FormData fdTypeFilters = new FormData();
    fdTypeFilters.left = new FormAttachment(0, 0);
    fdTypeFilters.top = new FormAttachment(wlTypeFilters, margin);
    fdTypeFilters.right = new FormAttachment(100, 0);
    fdTypeFilters.bottom = new FormAttachment(wOk, -2 * margin);
    wTypeFilters.setLayoutData(fdTypeFilters);

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTransformName.setText(transformName);

    wProvider.setText(Const.NVL(input.getProviderFieldName(), ""));
    wTypeKey.setText(Const.NVL(input.getTypeKeyFieldName(), ""));
    wTypeName.setText(Const.NVL(input.getTypeNameFieldName(), ""));
    wTypeDescription.setText(Const.NVL(input.getTypeDescriptionFieldName(), ""));
    wTypeClass.setText(Const.NVL(input.getTypeClassFieldName(), ""));
    wName.setText(Const.NVL(input.getNameFieldName(), ""));
    wJson.setText(Const.NVL(input.getJsonFieldName(), ""));

    for (int i = 0; i < input.getTypeKeyFilters().size(); i++) {
      TableItem item = wTypeFilters.table.getItem(i);

      int index = 1;
      item.setText(index++, Const.NVL(input.getTypeKeyFilters().get(i), ""));
    }

    wTypeFilters.setRowNums();
    wTypeFilters.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(MetadataInputMeta input) throws HopException {

    transformName = wTransformName.getText(); // return value

    input.getTypeKeyFilters().clear();

    for (TableItem item : wTypeFilters.getNonEmptyItems()) {
      int index = 1;
      input.getTypeKeyFilters().add(item.getText(index));
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error saving transform information", e);
    }
    dispose();
  }

  // Preview the data
  private void preview() {
    try {
      // Create the Access input transform
      MetadataInputMeta oneMeta = new MetadataInputMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      // We always just want to preview a single output row
      //
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {1});
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
                wTransformName.getText(),
                progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                progressDialog.getPreviewRows(wTransformName.getText()),
                loggingText);
        prd.open();
      }

    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MetadataInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "MetadataInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }
}
