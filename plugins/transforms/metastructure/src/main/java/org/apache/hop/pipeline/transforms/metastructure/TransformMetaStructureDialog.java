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
  private Button wIncludeMask;
  private Button wIncludeOrigin;

  private TextVar wRowCountField;
  private TextVar wPositionField;
  private TextVar wFieldnameField;
  private TextVar wCommentsField;
  private TextVar wTypeField;
  private TextVar wLengthField;
  private TextVar wPrecisionField;
  private TextVar wMaskField;
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

    // Rowcout Output
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
    Label wlRowCountField = new Label(wContent, SWT.RIGHT);
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
    Label wlPositionField = new Label(wContent, SWT.RIGHT);
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

    // Include fieldname field
    Label wlIncludeFieldname = new Label(wContent, SWT.RIGHT);
    wlIncludeFieldname.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeFieldname.Label"));
    PropsUi.setLook(wlIncludeFieldname);
    FormData fdlIncludeFieldname = new FormData();
    fdlIncludeFieldname.left = new FormAttachment(0, 0);
    fdlIncludeFieldname.top = new FormAttachment(wlPositionField, margin);
    fdlIncludeFieldname.right = new FormAttachment(middle, -margin);
    wlIncludeFieldname.setLayoutData(fdlIncludeFieldname);
    wIncludeFieldname = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wIncludeFieldname);
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
    Label wlFieldnameField = new Label(wContent, SWT.RIGHT);
    wlFieldnameField.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldnameField.Label"));
    PropsUi.setLook(wlFieldnameField);
    FormData fdlFieldnameField = new FormData();
    fdlFieldnameField.left = new FormAttachment(0, 0);
    fdlFieldnameField.right = new FormAttachment(middle, -margin);
    fdlFieldnameField.top = new FormAttachment(wlIncludeFieldname, margin);
    wlFieldnameField.setLayoutData(fdlFieldnameField);

    wFieldnameField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldnameField);
    wFieldnameField.addModifyListener(lsMod);
    FormData fdFieldnameField = new FormData();
    fdFieldnameField.left = new FormAttachment(middle, 0);
    fdFieldnameField.top = new FormAttachment(wlFieldnameField, 0, SWT.CENTER);
    fdFieldnameField.right = new FormAttachment(100, -margin);
    wFieldnameField.setLayoutData(fdFieldnameField);
    wFieldnameField.setEnabled(false);

    // Include Comment field
    Label wlIncludeComment = new Label(wContent, SWT.RIGHT);
    wlIncludeComment.setText(
        BaseMessages.getString(PKG, "TransformMetaStructureDialog.includeComments.Label"));
    PropsUi.setLook(wlIncludeComment);
    FormData fdlIncludeComment = new FormData();
    fdlIncludeComment.left = new FormAttachment(0, 0);
    fdlIncludeComment.top = new FormAttachment(wlFieldnameField, margin);
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
    Label wlCommentsField = new Label(wContent, SWT.RIGHT);
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
    Label wlTypeField = new Label(wContent, SWT.RIGHT);
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

            if (wIncludeMask.getSelection()) {
              wMaskField.setEnabled(true);
              // Default to field name by using translations
              wMaskField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.MaskName"));
            } else {
              wMaskField.setEnabled(false);
            }
          }
        });

    // Mask Field
    Label wlMaskField = new Label(wContent, SWT.RIGHT);
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
    Label wlLengthField = new Label(wContent, SWT.RIGHT);
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

            if (wIncludePrecision.getSelection()) {
              wPrecisionField.setEnabled(true);
              // Default to field name by using translations
              wPrecisionField.setText(
                  BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionName"));
            } else {
              wPrecisionField.setText("");
              wPrecisionField.setEnabled(false);
            }
          }
        });

    // Precision Field
    Label wlPrecisionField = new Label(wContent, SWT.RIGHT);
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
    Label wlOriginField = new Label(wContent, SWT.RIGHT);
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

    if (input.isIncludeMaskField()) {
      wMaskField.setText(input.getMaskFieldname());
    }

    wMaskField.setEnabled(input.isIncludeMaskField());
    wIncludeMask.setSelection(input.isIncludeMaskField());

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
    tfoi.setIncludeMaskField(wIncludeMask.getSelection());
    tfoi.setMaskFieldname(wMaskField.getText());
    tfoi.setIncludePrecisionField(wIncludePrecision.getSelection());
    tfoi.setPrecisionFieldname(wPrecisionField.getText());
    tfoi.setIncludeLengthField(wIncludeLength.getSelection());
    tfoi.setLengthFieldname(wLengthField.getText());
    tfoi.setIncludeOriginField(wIncludeOrigin.getSelection());
    tfoi.setOriginFieldname(wOriginField.getText());
  }
}
