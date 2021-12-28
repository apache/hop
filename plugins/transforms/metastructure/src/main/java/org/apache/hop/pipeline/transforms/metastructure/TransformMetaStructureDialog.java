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

package org.apache.hop.pipeline.transforms.metastructure;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class TransformMetaStructureDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG =
      TransformMetaStructureMeta.class; // for i18n purposes, needed by Translator2!!

  private final TransformMetaStructureMeta input;

  private Button wOutputRowcount;
  private Button wIncludePosition;
  private Button wIncludeFieldname;
  private Button wIncludeType;
  private Button wIncludeComments;
  private Button wIncludeLength;
  private Button wIncludePrecision;
  private Button wIncludeOrigin;

  private TextVar wRowCountField;
  private TextVar wPositionField;
  private TextVar wFieldnameField;
  private TextVar wCommentsField;
  private TextVar wTypeField;
  private TextVar wLengthField;
  private TextVar wPrecisionField;
  private TextVar wOriginField;

  public TransformMetaStructureDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (TransformMetaStructureMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "TransformMetaStructureDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.TransformName.Label"));
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

    // Rowcout Output
    Label wlOutputRowcount = new Label(shell, SWT.RIGHT);
    wlOutputRowcount.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.outputRowcount.Label"));
    props.setLook(wlOutputRowcount);
    FormData fdlOutputRowcount = new FormData();
    fdlOutputRowcount.left = new FormAttachment(0, 0);
    fdlOutputRowcount.top = new FormAttachment(wTransformName, margin);
    fdlOutputRowcount.right = new FormAttachment(middle, -margin);
    wlOutputRowcount.setLayoutData(fdlOutputRowcount);
    wOutputRowcount = new Button(shell, SWT.CHECK);
    props.setLook(wOutputRowcount);
    FormData fdOutputRowcount = new FormData();
    fdOutputRowcount.left = new FormAttachment(middle, 0);
    fdOutputRowcount.top = new FormAttachment(wlOutputRowcount, 0, SWT.CENTER);
    fdOutputRowcount.right = new FormAttachment(100, 0);
    wOutputRowcount.setLayoutData(fdOutputRowcount);
    wOutputRowcount.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wOutputRowcount.getSelection()) {
              wRowCountField.setEnabled(true);
              // Default to field name by using translations
              wRowCountField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.RowcountName"));
            } else {
              wRowCountField.setText("");
              wRowCountField.setEnabled(false);
            }
          }
        });

    // Row count Field
    Label wlRowCountField = new Label(shell, SWT.RIGHT);
    wlRowCountField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.RowcountField.Label"));
    props.setLook(wlRowCountField);
    FormData fdlRowCountField = new FormData();
    fdlRowCountField.left = new FormAttachment(0, 0);
    fdlRowCountField.right = new FormAttachment(middle, -margin);
    fdlRowCountField.top = new FormAttachment(wlOutputRowcount, 2 * margin);
    wlRowCountField.setLayoutData(fdlRowCountField);

    wRowCountField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRowCountField);
    wRowCountField.addModifyListener(lsMod);
    FormData fdRowCountField = new FormData();
    fdRowCountField.left = new FormAttachment(middle, 0);
    fdRowCountField.top = new FormAttachment(wlRowCountField, 0, SWT.CENTER);
    fdRowCountField.right = new FormAttachment(100, -margin);
    wRowCountField.setLayoutData(fdRowCountField);
    wRowCountField.setEnabled(false);

    // Include position field
    Label wlIncludePosition = new Label(shell, SWT.RIGHT);
    wlIncludePosition.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includePosition.Label"));
    props.setLook(wlIncludePosition);
    FormData fdlIncludePosition = new FormData();
    fdlIncludePosition.left = new FormAttachment(0, 0);
    fdlIncludePosition.top = new FormAttachment(wlRowCountField, margin);
    fdlIncludePosition.right = new FormAttachment(middle, -margin);
    wlIncludePosition.setLayoutData(fdlIncludePosition);
    wIncludePosition = new Button(shell, SWT.CHECK);
    props.setLook(wIncludePosition);
    FormData fdIncludePosition = new FormData();
    fdIncludePosition.left = new FormAttachment(middle, 0);
    fdIncludePosition.top = new FormAttachment(wlIncludePosition, 0, SWT.CENTER);
    fdIncludePosition.right = new FormAttachment(100, 0);
    wIncludePosition.setLayoutData(fdIncludePosition);
    wIncludePosition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludePosition.getSelection()) {
              wPositionField.setEnabled(true);
              // Default to field name by using translations
              wPositionField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.PositionName"));
            } else {
              wPositionField.setText("");
              wPositionField.setEnabled(false);
            }
          }
        });

    // Position Field
    Label wlPositionField = new Label(shell, SWT.RIGHT);
    wlPositionField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.PositionField.Label"));
    props.setLook(wlPositionField);
    FormData fdlPositionField = new FormData();
    fdlPositionField.left = new FormAttachment(0, 0);
    fdlPositionField.right = new FormAttachment(middle, -margin);
    fdlPositionField.top = new FormAttachment(wlIncludePosition, 2 * margin);
    wlPositionField.setLayoutData(fdlPositionField);

    wPositionField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPositionField);
    wPositionField.addModifyListener(lsMod);
    FormData fdPositionField = new FormData();
    fdPositionField.left = new FormAttachment(middle, 0);
    fdPositionField.top = new FormAttachment(wlPositionField, 0, SWT.CENTER);
    fdPositionField.right = new FormAttachment(100, -margin);
    wPositionField.setLayoutData(fdPositionField);
    wPositionField.setEnabled(false);

    // Include fieldname field
    Label wlIncludeFieldname = new Label(shell, SWT.RIGHT);
    wlIncludeFieldname.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeFieldname.Label"));
    props.setLook(wlIncludeFieldname);
    FormData fdlIncludeFieldname = new FormData();
    fdlIncludeFieldname.left = new FormAttachment(0, 0);
    fdlIncludeFieldname.top = new FormAttachment(wlPositionField, margin);
    fdlIncludeFieldname.right = new FormAttachment(middle, -margin);
    wlIncludeFieldname.setLayoutData(fdlIncludeFieldname);
    wIncludeFieldname = new Button(shell, SWT.CHECK);
    props.setLook(wIncludeFieldname);
    FormData fdIncludeFieldname = new FormData();
    fdIncludeFieldname.left = new FormAttachment(middle, 0);
    fdIncludeFieldname.top = new FormAttachment(wlIncludeFieldname, 0, SWT.CENTER);
    fdIncludeFieldname.right = new FormAttachment(100, 0);
    wIncludeFieldname.setLayoutData(fdIncludeFieldname);

    wIncludeFieldname.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludeFieldname.getSelection()) {
              wFieldnameField.setEnabled(true);
              // Default to field name by using translations
              wFieldnameField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldName"));
            } else {
              wFieldnameField.setText("");
              wFieldnameField.setEnabled(false);
            }
          }
        });

    // Fieldname Field
    Label wlFieldnameField = new Label(shell, SWT.RIGHT);
    wlFieldnameField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldnameField.Label"));
    props.setLook(wlFieldnameField);
    FormData fdlFieldnameField = new FormData();
    fdlFieldnameField.left = new FormAttachment(0, 0);
    fdlFieldnameField.right = new FormAttachment(middle, -margin);
    fdlFieldnameField.top = new FormAttachment(wlIncludeFieldname, 2 * margin);
    wlFieldnameField.setLayoutData(fdlFieldnameField);

    wFieldnameField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFieldnameField);
    wFieldnameField.addModifyListener(lsMod);
    FormData fdFieldnameField = new FormData();
    fdFieldnameField.left = new FormAttachment(middle, 0);
    fdFieldnameField.top = new FormAttachment(wlFieldnameField, 0, SWT.CENTER);
    fdFieldnameField.right = new FormAttachment(100, -margin);
    wFieldnameField.setLayoutData(fdFieldnameField);
    wFieldnameField.setEnabled(false);

    // Include Comment field
    Label wlIncludeComment = new Label(shell, SWT.RIGHT);
    wlIncludeComment.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeComments.Label"));
    props.setLook(wlIncludeComment);
    FormData fdlIncludeComment = new FormData();
    fdlIncludeComment.left = new FormAttachment(0, 0);
    fdlIncludeComment.top = new FormAttachment(wlFieldnameField, margin);
    fdlIncludeComment.right = new FormAttachment(middle, -margin);
    wlIncludeComment.setLayoutData(fdlIncludeComment);
    wIncludeComments = new Button(shell, SWT.CHECK);
    props.setLook(wIncludeComments);
    FormData fdIncludeComment = new FormData();
    fdIncludeComment.left = new FormAttachment(middle, 0);
    fdIncludeComment.top = new FormAttachment(wlIncludeComment, 0, SWT.CENTER);
    fdIncludeComment.right = new FormAttachment(100, 0);
    wIncludeComments.setLayoutData(fdIncludeComment);

    wIncludeComments.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludeComments.getSelection()) {
              wCommentsField.setEnabled(true);
              // Default to field name by using translations
              wCommentsField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.CommentsName"));
            } else {
              wCommentsField.setText("");
              wCommentsField.setEnabled(false);
            }
          }
        });

    // Comments Field
    Label wlCommentsField = new Label(shell, SWT.RIGHT);
    wlCommentsField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.CommentsField.Label"));
    props.setLook(wlCommentsField);
    FormData fdlCommentsField = new FormData();
    fdlCommentsField.left = new FormAttachment(0, 0);
    fdlCommentsField.right = new FormAttachment(middle, -margin);
    fdlCommentsField.top = new FormAttachment(wlIncludeComment, 2 * margin);
    wlCommentsField.setLayoutData(fdlCommentsField);

    wCommentsField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCommentsField);
    wCommentsField.addModifyListener(lsMod);
    FormData fdCommentsField = new FormData();
    fdCommentsField.left = new FormAttachment(middle, 0);
    fdCommentsField.top = new FormAttachment(wlCommentsField, 0, SWT.CENTER);
    fdCommentsField.right = new FormAttachment(100, -margin);
    wCommentsField.setLayoutData(fdCommentsField);
    wCommentsField.setEnabled(true);

    // Include Type field
    Label wlIncludeType = new Label(shell, SWT.RIGHT);
    wlIncludeType.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeType.Label"));
    props.setLook(wlIncludeType);
    FormData fdlIncludeType = new FormData();
    fdlIncludeType.left = new FormAttachment(0, 0);
    fdlIncludeType.top = new FormAttachment(wlCommentsField, margin);
    fdlIncludeType.right = new FormAttachment(middle, -margin);
    wlIncludeType.setLayoutData(fdlIncludeType);
    wIncludeType = new Button(shell, SWT.CHECK);
    props.setLook(wIncludeType);
    FormData fdIncludeType = new FormData();
    fdIncludeType.left = new FormAttachment(middle, 0);
    fdIncludeType.top = new FormAttachment(wlIncludeType, 0, SWT.CENTER);
    fdIncludeType.right = new FormAttachment(100, 0);
    wIncludeType.setLayoutData(fdIncludeType);

    wIncludeType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludeType.getSelection()) {
              wTypeField.setEnabled(true);
              // Default to field name by using translations
              wTypeField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.TypeName"));
            } else {
              wTypeField.setText("");
              wTypeField.setEnabled(false);
            }
          }
        });

    // Type Field
    Label wlTypeField = new Label(shell, SWT.RIGHT);
    wlTypeField.setText(BaseMessages.getString(PKG, "TransformMetaStructureMeta.TypeField.Label"));
    props.setLook(wlTypeField);
    FormData fdlTypeField = new FormData();
    fdlTypeField.left = new FormAttachment(0, 0);
    fdlTypeField.right = new FormAttachment(middle, -margin);
    fdlTypeField.top = new FormAttachment(wlIncludeType, 2 * margin);
    wlTypeField.setLayoutData(fdlTypeField);

    wTypeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTypeField);
    wTypeField.addModifyListener(lsMod);
    FormData fdTypeField = new FormData();
    fdTypeField.left = new FormAttachment(middle, 0);
    fdTypeField.top = new FormAttachment(wlTypeField, 0, SWT.CENTER);
    fdTypeField.right = new FormAttachment(100, -margin);
    wTypeField.setLayoutData(fdTypeField);
    wTypeField.setEnabled(true);

    // Include Length field
    Label wlIncludeLength = new Label(shell, SWT.RIGHT);
    wlIncludeLength.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeLength.Label"));
    props.setLook(wlIncludeLength);
    FormData fdlIncludeLength = new FormData();
    fdlIncludeLength.left = new FormAttachment(0, 0);
    fdlIncludeLength.top = new FormAttachment(wlTypeField, margin);
    fdlIncludeLength.right = new FormAttachment(middle, -margin);
    wlIncludeLength.setLayoutData(fdlIncludeLength);
    wIncludeLength = new Button(shell, SWT.CHECK);
    props.setLook(wIncludeLength);
    FormData fdIncludeLength = new FormData();
    fdIncludeLength.left = new FormAttachment(middle, 0);
    fdIncludeLength.top = new FormAttachment(wlIncludeLength, 0, SWT.CENTER);
    fdIncludeLength.right = new FormAttachment(100, 0);
    wIncludeLength.setLayoutData(fdIncludeLength);

    wIncludeLength.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludeLength.getSelection()) {
              wLengthField.setEnabled(true);
              // Default to field name by using translations
              wLengthField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.LengthName"));
            } else {
              wLengthField.setText("");
              wLengthField.setEnabled(false);
            }
          }
        });

    // Length Field
    Label wlLengthField = new Label(shell, SWT.RIGHT);
    wlLengthField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.LengthField.Label"));
    props.setLook(wlLengthField);
    FormData fdlLengthField = new FormData();
    fdlLengthField.left = new FormAttachment(0, 0);
    fdlLengthField.right = new FormAttachment(middle, -margin);
    fdlLengthField.top = new FormAttachment(wlIncludeLength, 2 * margin);
    wlLengthField.setLayoutData(fdlLengthField);

    wLengthField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLengthField);
    wLengthField.addModifyListener(lsMod);
    FormData fdLengthField = new FormData();
    fdLengthField.left = new FormAttachment(middle, 0);
    fdLengthField.top = new FormAttachment(wlLengthField, 0, SWT.CENTER);
    fdLengthField.right = new FormAttachment(100, -margin);
    wLengthField.setLayoutData(fdLengthField);
    wLengthField.setEnabled(true);

    // Include Precision field
    Label wlIncludePrecision = new Label(shell, SWT.RIGHT);
    wlIncludePrecision.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includePrecision.Label"));
    props.setLook(wlIncludePrecision);
    FormData fdlIncludePrecision = new FormData();
    fdlIncludePrecision.left = new FormAttachment(0, 0);
    fdlIncludePrecision.top = new FormAttachment(wlLengthField, margin);
    fdlIncludePrecision.right = new FormAttachment(middle, -margin);
    wlIncludePrecision.setLayoutData(fdlIncludePrecision);
    wIncludePrecision = new Button(shell, SWT.CHECK);
    props.setLook(wIncludePrecision);
    FormData fdIncludePrecision = new FormData();
    fdIncludePrecision.left = new FormAttachment(middle, 0);
    fdIncludePrecision.top = new FormAttachment(wlIncludePrecision, 0, SWT.CENTER);
    fdIncludePrecision.right = new FormAttachment(100, 0);
    wIncludePrecision.setLayoutData(fdIncludePrecision);

    wIncludePrecision.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludePrecision.getSelection()) {
              wPrecisionField.setEnabled(true);
              // Default to field name by using translations
              wPrecisionField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionName"));
            } else {
              wIncludePrecision.setText("");
              wPrecisionField.setEnabled(false);
            }
          }
        });

    // Precision Field
    Label wlPrecisionField = new Label(shell, SWT.RIGHT);
    wlPrecisionField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionField.Label"));
    props.setLook(wlPrecisionField);
    FormData fdlPrecisionField = new FormData();
    fdlPrecisionField.left = new FormAttachment(0, 0);
    fdlPrecisionField.right = new FormAttachment(middle, -margin);
    fdlPrecisionField.top = new FormAttachment(wlIncludePrecision, 2 * margin);
    wlPrecisionField.setLayoutData(fdlPrecisionField);

    wPrecisionField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrecisionField);
    wPrecisionField.addModifyListener(lsMod);
    FormData fdPrecisionField = new FormData();
    fdPrecisionField.left = new FormAttachment(middle, 0);
    fdPrecisionField.top = new FormAttachment(wlPrecisionField, 0, SWT.CENTER);
    fdPrecisionField.right = new FormAttachment(100, -margin);
    wPrecisionField.setLayoutData(fdPrecisionField);
    wPrecisionField.setEnabled(true);

    // Include Origin field
    Label wlIncludeOrigin = new Label(shell, SWT.RIGHT);
    wlIncludeOrigin.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeOrigin.Label"));
    props.setLook(wlIncludeOrigin);
    FormData fdlIncludeOrigin = new FormData();
    fdlIncludeOrigin.left = new FormAttachment(0, 0);
    fdlIncludeOrigin.top = new FormAttachment(wlPrecisionField, margin);
    fdlIncludeOrigin.right = new FormAttachment(middle, -margin);
    wlIncludeOrigin.setLayoutData(fdlIncludeOrigin);
    wIncludeOrigin = new Button(shell, SWT.CHECK);
    props.setLook(wIncludeOrigin);
    FormData fdIncludeOrigin = new FormData();
    fdIncludeOrigin.left = new FormAttachment(middle, 0);
    fdIncludeOrigin.top = new FormAttachment(wlIncludeOrigin, 0, SWT.CENTER);
    fdIncludeOrigin.right = new FormAttachment(100, 0);
    wIncludeOrigin.setLayoutData(fdIncludeOrigin);

    wIncludeOrigin.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();

            if (wIncludeOrigin.getSelection()) {
              wOriginField.setEnabled(true);
              // Default to field name by using translations
              wOriginField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.OriginName"));
            } else {
              wOriginField.setEnabled(false);
            }
          }
        });

    // Origin Field
    Label wlOriginField = new Label(shell, SWT.RIGHT);
    wlOriginField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.OriginField.Label"));
    props.setLook(wlOriginField);
    FormData fdlOriginField = new FormData();
    fdlOriginField.left = new FormAttachment(0, 0);
    fdlOriginField.right = new FormAttachment(middle, -margin);
    fdlOriginField.top = new FormAttachment(wlIncludeOrigin, 2 * margin);
    wlOriginField.setLayoutData(fdlOriginField);

    wOriginField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOriginField);
    wOriginField.addModifyListener(lsMod);
    FormData fdOriginField = new FormData();
    fdOriginField.left = new FormAttachment(middle, 0);
    fdOriginField.top = new FormAttachment(wlOriginField, 0, SWT.CENTER);
    fdOriginField.right = new FormAttachment(100, -margin);
    wOriginField.setLayoutData(fdOriginField);
    wOriginField.setEnabled(true);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wOriginField);

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.isOutputRowcount()) {
      wRowCountField.setText(input.getRowcountField());
    }
    wRowCountField.setEnabled(input.isOutputRowcount());
    wOutputRowcount.setSelection(input.isOutputRowcount());

    if (input.isIncludePositionField()) {
      wPositionField.setText(input.getPositionFieldname());
    }

    wPositionField.setEnabled(input.isIncludePositionField());
    wIncludePosition.setSelection(input.isIncludePositionField());

    if (input.isIncludeFieldnameField()) {
      wFieldnameField.setText(input.getFieldFieldname());
    }

    wFieldnameField.setEnabled(input.isIncludeFieldnameField());
    wIncludeFieldname.setSelection(input.isIncludeFieldnameField());

    if (input.isIncludeCommentsField()) {
      wCommentsField.setText(input.getCommentsFieldname());
    }

    wCommentsField.setEnabled(input.isIncludeCommentsField());
    wIncludeComments.setSelection(input.isIncludeCommentsField());

    if (input.isIncludeTypeField()) {
      wTypeField.setText(input.getTypeFieldname());
    }

    wTypeField.setEnabled(input.isIncludeTypeField());
    wIncludeType.setSelection(input.isIncludeTypeField());

    if (input.isIncludeLengthField()) {
      wLengthField.setText(input.getLengthFieldname());
    }

    wLengthField.setEnabled(input.isIncludeLengthField());
    wIncludeLength.setSelection(input.isIncludeLengthField());

    if (input.isIncludePrecisionField()) {
      wPrecisionField.setText(input.getPrecisionFieldname());
    }

    wPrecisionField.setEnabled(input.isIncludePrecisionField());
    wIncludePrecision.setSelection(input.isIncludePrecisionField());

    if (input.isIncludeOriginField()) {
      wOriginField.setText(input.getOriginFieldname());
    }

    wOriginField.setEnabled(input.isIncludeOriginField());
    wIncludeOrigin.setSelection(input.isIncludeOriginField());

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void getInfo(TransformMetaStructureMeta tfoi) {
    tfoi.setOutputRowcount(wOutputRowcount.getSelection());
    tfoi.setRowcountField(wRowCountField.getText());
    tfoi.setIncludePositionField(wIncludePosition.getSelection());
    tfoi.setPositionFieldname(wPositionField.getText());
    tfoi.setIncludeFieldnameField(wIncludeFieldname.getSelection());
    tfoi.setFieldFieldname(wFieldnameField.getText());
    tfoi.setIncludeCommentsField(wIncludeComments.getSelection());
    tfoi.setCommentsFieldname(wCommentsField.getText());
    tfoi.setIncludeTypeField(wIncludeType.getSelection());
    tfoi.setTypeFieldname(wTypeField.getText());
    tfoi.setIncludePrecisionField(wIncludePrecision.getSelection());
    tfoi.setPrecisionFieldname(wPrecisionField.getText());
    tfoi.setIncludeLengthField(wIncludeLength.getSelection());
    tfoi.setLengthFieldname(wLengthField.getText());
    tfoi.setIncludeOriginField(wIncludeOrigin.getSelection());
    tfoi.setOriginFieldname(wOriginField.getText());
  }
}
