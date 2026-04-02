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

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class TransformMetaStructureDialog extends BaseTransformDialog {
  // for i18n purposes, needed by Translator2!!
  private static final Class<?> PKG = TransformMetaStructureMeta.class;

  private final TransformMetaStructureMeta input;

  private Button wOutputRowcount;
  private Button wIncludePosition;
  private Button wIncludeFieldName;
  private Button wIncludeType;
  private Button wIncludeComments;
  private Button wIncludeLength;
  private Button wIncludePrecision;
  private Button wIncludeMask;
  private Button wIncludeOrigin;

  // label, text
  private Label wlRowCountField;
  private TextVar wRowCountField;

  private Label wlPositionField;
  private TextVar wPositionField;

  private Label wlFieldNameField;
  private TextVar wFieldNameField;

  private Label wlCommentsField;
  private TextVar wCommentsField;

  private Label wlTypeField;
  private TextVar wTypeField;

  private Label wlLengthField;
  private TextVar wLengthField;

  private Label wlPrecisionField;
  private TextVar wPrecisionField;

  private Label wlMaskField;
  private TextVar wMaskField;

  private Label wlOriginField;
  private TextVar wOriginField;

  public TransformMetaStructureDialog(
      Shell parent,
      IVariables variables,
      TransformMetaStructureMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "TransformMetaStructureDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // Row count Output
    Label wlOutputRowcount = new Label(wContent, SWT.RIGHT);
    wlOutputRowcount.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.outputRowcount.Label"));
    PropsUi.setLook(wlOutputRowcount);
    FormData fdlOutputRowcount = new FormData();
    fdlOutputRowcount.left = new FormAttachment(0, 0);
    fdlOutputRowcount.top = new FormAttachment(0, margin);
    fdlOutputRowcount.right = new FormAttachment(middle, -margin);
    wlOutputRowcount.setLayoutData(fdlOutputRowcount);
    wOutputRowcount = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wOutputRowcount);
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
            toggleTextAndLabel(wOutputRowcount, wRowCountField, wlRowCountField, "RowcountName");
          }
        });

    // Row count Field
    wlRowCountField = new Label(wContent, SWT.RIGHT);
    wlRowCountField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.RowcountField.Label"));
    PropsUi.setLook(wlRowCountField);
    FormData fdlRowCountField = new FormData();
    fdlRowCountField.left = new FormAttachment(0, 0);
    fdlRowCountField.right = new FormAttachment(middle, -margin);
    fdlRowCountField.top = new FormAttachment(wlOutputRowcount, margin);
    wlRowCountField.setLayoutData(fdlRowCountField);

    wRowCountField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRowCountField);
    wRowCountField.addModifyListener(lsMod);
    FormData fdRowCountField = new FormData();
    fdRowCountField.left = new FormAttachment(middle, 0);
    fdRowCountField.top = new FormAttachment(wlRowCountField, 0, SWT.CENTER);
    fdRowCountField.right = new FormAttachment(100, -margin);
    wRowCountField.setLayoutData(fdRowCountField);
    wRowCountField.setEnabled(false);

    // Include position field
    Label wlIncludePosition = new Label(wContent, SWT.RIGHT);
    wlIncludePosition.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includePosition.Label"));
    PropsUi.setLook(wlIncludePosition);
    FormData fdlIncludePosition = new FormData();
    fdlIncludePosition.left = new FormAttachment(0, 0);
    fdlIncludePosition.top = new FormAttachment(wlRowCountField, margin);
    fdlIncludePosition.right = new FormAttachment(middle, -margin);
    wlIncludePosition.setLayoutData(fdlIncludePosition);
    wIncludePosition = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludePosition);
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
            toggleTextAndLabel(wIncludePosition, wPositionField, wlPositionField, "PositionName");
          }
        });

    // Position Field
    wlPositionField = new Label(wContent, SWT.RIGHT);
    wlPositionField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.PositionField.Label"));
    PropsUi.setLook(wlPositionField);
    FormData fdlPositionField = new FormData();
    fdlPositionField.left = new FormAttachment(0, 0);
    fdlPositionField.right = new FormAttachment(middle, -margin);
    fdlPositionField.top = new FormAttachment(wlIncludePosition, margin);
    wlPositionField.setLayoutData(fdlPositionField);

    wPositionField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPositionField);
    wPositionField.addModifyListener(lsMod);
    FormData fdPositionField = new FormData();
    fdPositionField.left = new FormAttachment(middle, 0);
    fdPositionField.top = new FormAttachment(wlPositionField, 0, SWT.CENTER);
    fdPositionField.right = new FormAttachment(100, -margin);
    wPositionField.setLayoutData(fdPositionField);
    wPositionField.setEnabled(false);

    // Include fieldName field
    Label wlIncludeFieldName = new Label(wContent, SWT.RIGHT);
    wlIncludeFieldName.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeFieldname.Label"));
    PropsUi.setLook(wlIncludeFieldName);
    FormData fdlIncludeFieldName = new FormData();
    fdlIncludeFieldName.left = new FormAttachment(0, 0);
    fdlIncludeFieldName.top = new FormAttachment(wlPositionField, margin);
    fdlIncludeFieldName.right = new FormAttachment(middle, -margin);
    wlIncludeFieldName.setLayoutData(fdlIncludeFieldName);
    wIncludeFieldName = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeFieldName);
    FormData fdIncludeFieldName = new FormData();
    fdIncludeFieldName.left = new FormAttachment(middle, 0);
    fdIncludeFieldName.top = new FormAttachment(wlIncludeFieldName, 0, SWT.CENTER);
    fdIncludeFieldName.right = new FormAttachment(100, 0);
    wIncludeFieldName.setLayoutData(fdIncludeFieldName);

    wIncludeFieldName.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            toggleTextAndLabel(wIncludeFieldName, wFieldNameField, wlFieldNameField, "FieldName");
          }
        });

    // Fieldname Field
    wlFieldNameField = new Label(wContent, SWT.RIGHT);
    wlFieldNameField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldnameField.Label"));
    PropsUi.setLook(wlFieldNameField);
    FormData fdlFieldNameField = new FormData();
    fdlFieldNameField.left = new FormAttachment(0, 0);
    fdlFieldNameField.right = new FormAttachment(middle, -margin);
    fdlFieldNameField.top = new FormAttachment(wlIncludeFieldName, margin);
    wlFieldNameField.setLayoutData(fdlFieldNameField);

    wFieldNameField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldNameField);
    wFieldNameField.addModifyListener(lsMod);
    FormData fdFieldNameField = new FormData();
    fdFieldNameField.left = new FormAttachment(middle, 0);
    fdFieldNameField.top = new FormAttachment(wlFieldNameField, 0, SWT.CENTER);
    fdFieldNameField.right = new FormAttachment(100, -margin);
    wFieldNameField.setLayoutData(fdFieldNameField);
    wFieldNameField.setEnabled(false);

    // Include Comment field
    Label wlIncludeComment = new Label(wContent, SWT.RIGHT);
    wlIncludeComment.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeComments.Label"));
    PropsUi.setLook(wlIncludeComment);
    FormData fdlIncludeComment = new FormData();
    fdlIncludeComment.left = new FormAttachment(0, 0);
    fdlIncludeComment.top = new FormAttachment(wlFieldNameField, margin);
    fdlIncludeComment.right = new FormAttachment(middle, -margin);
    wlIncludeComment.setLayoutData(fdlIncludeComment);
    wIncludeComments = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeComments);
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
            toggleTextAndLabel(wIncludeComments, wCommentsField, wlCommentsField, "CommentsName");
          }
        });

    // Comments Field
    wlCommentsField = new Label(wContent, SWT.RIGHT);
    wlCommentsField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.CommentsField.Label"));
    PropsUi.setLook(wlCommentsField);
    FormData fdlCommentsField = new FormData();
    fdlCommentsField.left = new FormAttachment(0, 0);
    fdlCommentsField.right = new FormAttachment(middle, -margin);
    fdlCommentsField.top = new FormAttachment(wlIncludeComment, margin);
    wlCommentsField.setLayoutData(fdlCommentsField);

    wCommentsField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommentsField);
    wCommentsField.addModifyListener(lsMod);
    FormData fdCommentsField = new FormData();
    fdCommentsField.left = new FormAttachment(middle, 0);
    fdCommentsField.top = new FormAttachment(wlCommentsField, 0, SWT.CENTER);
    fdCommentsField.right = new FormAttachment(100, -margin);
    wCommentsField.setLayoutData(fdCommentsField);
    wCommentsField.setEnabled(true);

    // Include Type field
    Label wlIncludeType = new Label(wContent, SWT.RIGHT);
    wlIncludeType.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeType.Label"));
    PropsUi.setLook(wlIncludeType);
    FormData fdlIncludeType = new FormData();
    fdlIncludeType.left = new FormAttachment(0, 0);
    fdlIncludeType.top = new FormAttachment(wlCommentsField, margin);
    fdlIncludeType.right = new FormAttachment(middle, -margin);
    wlIncludeType.setLayoutData(fdlIncludeType);
    wIncludeType = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeType);
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
            toggleTextAndLabel(wIncludeType, wTypeField, wlTypeField, "TypeName");
          }
        });

    // Type Field
    wlTypeField = new Label(wContent, SWT.RIGHT);
    wlTypeField.setText(BaseMessages.getString(PKG, "TransformMetaStructureMeta.TypeField.Label"));
    PropsUi.setLook(wlTypeField);
    FormData fdlTypeField = new FormData();
    fdlTypeField.left = new FormAttachment(0, 0);
    fdlTypeField.right = new FormAttachment(middle, -margin);
    fdlTypeField.top = new FormAttachment(wlIncludeType, margin);
    wlTypeField.setLayoutData(fdlTypeField);

    wTypeField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTypeField);
    wTypeField.addModifyListener(lsMod);
    FormData fdTypeField = new FormData();
    fdTypeField.left = new FormAttachment(middle, 0);
    fdTypeField.top = new FormAttachment(wlTypeField, 0, SWT.CENTER);
    fdTypeField.right = new FormAttachment(100, -margin);
    wTypeField.setLayoutData(fdTypeField);
    wTypeField.setEnabled(true);

    // Include Mask field
    Label wlIncludeMask = new Label(wContent, SWT.RIGHT);
    wlIncludeMask.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeMask.Label"));
    PropsUi.setLook(wlIncludeMask);
    FormData fdlIncludeMask = new FormData();
    fdlIncludeMask.left = new FormAttachment(0, 0);
    fdlIncludeMask.top = new FormAttachment(wlTypeField, margin);
    fdlIncludeMask.right = new FormAttachment(middle, -margin);
    wlIncludeMask.setLayoutData(fdlIncludeMask);
    wIncludeMask = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeMask);
    FormData fdIncludeMask = new FormData();
    fdIncludeMask.left = new FormAttachment(middle, 0);
    fdIncludeMask.top = new FormAttachment(wlIncludeMask, 0, SWT.CENTER);
    fdIncludeMask.right = new FormAttachment(100, 0);
    wIncludeMask.setLayoutData(fdIncludeMask);

    wIncludeMask.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            toggleTextAndLabel(wIncludeMask, wMaskField, wlMaskField, "MaskName");
          }
        });

    // Mask Field
    wlMaskField = new Label(wContent, SWT.RIGHT);
    wlMaskField.setText(BaseMessages.getString(PKG, "TransformMetaStructureMeta.MaskField.Label"));
    PropsUi.setLook(wlMaskField);
    FormData fdlMaskField = new FormData();
    fdlMaskField.left = new FormAttachment(0, 0);
    fdlMaskField.right = new FormAttachment(middle, -margin);
    fdlMaskField.top = new FormAttachment(wlIncludeMask, margin);
    wlMaskField.setLayoutData(fdlMaskField);

    wMaskField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaskField);
    wMaskField.addModifyListener(lsMod);
    FormData fdMaskField = new FormData();
    fdMaskField.left = new FormAttachment(middle, 0);
    fdMaskField.top = new FormAttachment(wlMaskField, 0, SWT.CENTER);
    fdMaskField.right = new FormAttachment(100, -margin);
    wMaskField.setLayoutData(fdMaskField);
    wMaskField.setEnabled(true);

    // Include Length field
    Label wlIncludeLength = new Label(wContent, SWT.RIGHT);
    wlIncludeLength.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeLength.Label"));
    PropsUi.setLook(wlIncludeLength);
    FormData fdlIncludeLength = new FormData();
    fdlIncludeLength.left = new FormAttachment(0, 0);
    fdlIncludeLength.top = new FormAttachment(wlMaskField, margin);
    fdlIncludeLength.right = new FormAttachment(middle, -margin);
    wlIncludeLength.setLayoutData(fdlIncludeLength);
    wIncludeLength = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeLength);
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
            toggleTextAndLabel(wIncludeLength, wLengthField, wlLengthField, "LengthName");
          }
        });

    // Length Field
    wlLengthField = new Label(wContent, SWT.RIGHT);
    wlLengthField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.LengthField.Label"));
    PropsUi.setLook(wlLengthField);
    FormData fdlLengthField = new FormData();
    fdlLengthField.left = new FormAttachment(0, 0);
    fdlLengthField.right = new FormAttachment(middle, -margin);
    fdlLengthField.top = new FormAttachment(wlIncludeLength, margin);
    wlLengthField.setLayoutData(fdlLengthField);

    wLengthField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLengthField);
    wLengthField.addModifyListener(lsMod);
    FormData fdLengthField = new FormData();
    fdLengthField.left = new FormAttachment(middle, 0);
    fdLengthField.top = new FormAttachment(wlLengthField, 0, SWT.CENTER);
    fdLengthField.right = new FormAttachment(100, -margin);
    wLengthField.setLayoutData(fdLengthField);
    wLengthField.setEnabled(true);

    // Include Precision field
    Label wlIncludePrecision = new Label(wContent, SWT.RIGHT);
    wlIncludePrecision.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includePrecision.Label"));
    PropsUi.setLook(wlIncludePrecision);
    FormData fdlIncludePrecision = new FormData();
    fdlIncludePrecision.left = new FormAttachment(0, 0);
    fdlIncludePrecision.top = new FormAttachment(wlLengthField, margin);
    fdlIncludePrecision.right = new FormAttachment(middle, -margin);
    wlIncludePrecision.setLayoutData(fdlIncludePrecision);
    wIncludePrecision = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludePrecision);
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
            toggleTextAndLabel(
                wIncludePrecision, wPrecisionField, wlPrecisionField, "PrecisionName");
          }
        });

    // Precision Field
    wlPrecisionField = new Label(wContent, SWT.RIGHT);
    wlPrecisionField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionField.Label"));
    PropsUi.setLook(wlPrecisionField);
    FormData fdlPrecisionField = new FormData();
    fdlPrecisionField.left = new FormAttachment(0, 0);
    fdlPrecisionField.right = new FormAttachment(middle, -margin);
    fdlPrecisionField.top = new FormAttachment(wlIncludePrecision, margin);
    wlPrecisionField.setLayoutData(fdlPrecisionField);

    wPrecisionField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrecisionField);
    wPrecisionField.addModifyListener(lsMod);
    FormData fdPrecisionField = new FormData();
    fdPrecisionField.left = new FormAttachment(middle, 0);
    fdPrecisionField.top = new FormAttachment(wlPrecisionField, 0, SWT.CENTER);
    fdPrecisionField.right = new FormAttachment(100, -margin);
    wPrecisionField.setLayoutData(fdPrecisionField);
    wPrecisionField.setEnabled(true);

    // Include Origin field
    Label wlIncludeOrigin = new Label(wContent, SWT.RIGHT);
    wlIncludeOrigin.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeOrigin.Label"));
    PropsUi.setLook(wlIncludeOrigin);
    FormData fdlIncludeOrigin = new FormData();
    fdlIncludeOrigin.left = new FormAttachment(0, 0);
    fdlIncludeOrigin.top = new FormAttachment(wlPrecisionField, margin);
    fdlIncludeOrigin.right = new FormAttachment(middle, -margin);
    wlIncludeOrigin.setLayoutData(fdlIncludeOrigin);
    wIncludeOrigin = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeOrigin);
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
            toggleTextAndLabel(wIncludeOrigin, wOriginField, wlOriginField, "OriginName");
          }
        });

    // Origin Field
    wlOriginField = new Label(wContent, SWT.RIGHT);
    wlOriginField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.OriginField.Label"));
    PropsUi.setLook(wlOriginField);
    FormData fdlOriginField = new FormData();
    fdlOriginField.left = new FormAttachment(0, 0);
    fdlOriginField.right = new FormAttachment(middle, -margin);
    fdlOriginField.top = new FormAttachment(wlIncludeOrigin, margin);
    wlOriginField.setLayoutData(fdlOriginField);

    wOriginField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOriginField);
    wOriginField.addModifyListener(lsMod);
    FormData fdOriginField = new FormData();
    fdOriginField.left = new FormAttachment(middle, 0);
    fdOriginField.top = new FormAttachment(wlOriginField, 0, SWT.CENTER);
    fdOriginField.right = new FormAttachment(100, -margin);
    wOriginField.setLayoutData(fdOriginField);
    wOriginField.setEnabled(true);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.isOutputRowcount()) {
      wRowCountField.setText(input.getRowcountField());
    }

    wlRowCountField.setEnabled(input.isOutputRowcount());
    wRowCountField.setEnabled(input.isOutputRowcount());
    wOutputRowcount.setSelection(input.isOutputRowcount());

    if (input.isIncludePositionField()) {
      wPositionField.setText(input.getPositionFieldName());
    }

    wlPositionField.setEnabled(input.isIncludePositionField());
    wPositionField.setEnabled(input.isIncludePositionField());
    wIncludePosition.setSelection(input.isIncludePositionField());

    if (input.isIncludeFieldNameField()) {
      wFieldNameField.setText(input.getFieldFieldName());
    }

    wlFieldNameField.setEnabled(input.isIncludeFieldNameField());
    wFieldNameField.setEnabled(input.isIncludeFieldNameField());
    wIncludeFieldName.setSelection(input.isIncludeFieldNameField());

    if (input.isIncludeCommentsField()) {
      wCommentsField.setText(input.getCommentsFieldName());
    }

    wlCommentsField.setEnabled(input.isIncludeCommentsField());
    wCommentsField.setEnabled(input.isIncludeCommentsField());
    wIncludeComments.setSelection(input.isIncludeCommentsField());

    if (input.isIncludeTypeField()) {
      wTypeField.setText(input.getTypeFieldName());
    }

    if (input.isIncludeMaskField()) {
      wMaskField.setText(input.getMaskFieldName());
    }

    wlMaskField.setEnabled(input.isIncludeMaskField());
    wMaskField.setEnabled(input.isIncludeMaskField());
    wIncludeMask.setSelection(input.isIncludeMaskField());

    wlTypeField.setEnabled(input.isIncludeTypeField());
    wTypeField.setEnabled(input.isIncludeTypeField());
    wIncludeType.setSelection(input.isIncludeTypeField());

    if (input.isIncludeLengthField()) {
      wLengthField.setText(input.getLengthFieldName());
    }

    wlLengthField.setEnabled(input.isIncludeLengthField());
    wLengthField.setEnabled(input.isIncludeLengthField());
    wIncludeLength.setSelection(input.isIncludeLengthField());

    if (input.isIncludePrecisionField()) {
      wPrecisionField.setText(input.getPrecisionFieldName());
    }

    wlPrecisionField.setEnabled(input.isIncludePrecisionField());
    wPrecisionField.setEnabled(input.isIncludePrecisionField());
    wIncludePrecision.setSelection(input.isIncludePrecisionField());

    if (input.isIncludeOriginField()) {
      wOriginField.setText(input.getOriginFieldName());
    }

    wlOriginField.setEnabled(input.isIncludeOriginField());
    wOriginField.setEnabled(input.isIncludeOriginField());
    wIncludeOrigin.setSelection(input.isIncludeOriginField());
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

    // return value
    transformName = wTransformName.getText();

    getInfo(input);
    dispose();
  }

  private void getInfo(TransformMetaStructureMeta meta) {
    meta.setOutputRowcount(wOutputRowcount.getSelection());
    meta.setRowcountField(wRowCountField.getText());
    meta.setIncludePositionField(wIncludePosition.getSelection());
    meta.setPositionFieldName(wPositionField.getText());
    meta.setIncludeFieldNameField(wIncludeFieldName.getSelection());
    meta.setFieldFieldName(wFieldNameField.getText());
    meta.setIncludeCommentsField(wIncludeComments.getSelection());
    meta.setCommentsFieldName(wCommentsField.getText());
    meta.setIncludeTypeField(wIncludeType.getSelection());
    meta.setTypeFieldName(wTypeField.getText());
    meta.setIncludeMaskField(wIncludeMask.getSelection());
    meta.setMaskFieldName(wMaskField.getText());
    meta.setIncludePrecisionField(wIncludePrecision.getSelection());
    meta.setPrecisionFieldName(wPrecisionField.getText());
    meta.setIncludeLengthField(wIncludeLength.getSelection());
    meta.setLengthFieldName(wLengthField.getText());
    meta.setIncludeOriginField(wIncludeOrigin.getSelection());
    meta.setOriginFieldName(wOriginField.getText());
  }

  /**
   * Updates the enabled state of the given TextVar and Label based on the checkbox selection.
   *
   * @param check the checkbox controlling the state
   * @param text the text field to update
   * @param label the label associated with the text field
   * @param message the message key used to retrieve the default value
   */
  private void toggleTextAndLabel(Button check, TextVar text, Label label, String message) {
    if (check.getSelection()) {
      text.setEnabled(true);
      label.setEnabled(true);
      // Default to field name by using translations
      text.setText(BaseMessages.getString(PKG, "TransformMetaStructureMeta." + message));
    } else {
      text.setText("");
      text.setEnabled(false);
      label.setEnabled(false);
    }
  }
}
