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
 *
 */

package org.apache.hop.pipeline.transforms.validator;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ValidatorDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ValidatorMeta.class;
  private final ValidatorMeta input;
  private List wValidationsList;
  private IRowMeta inputFields;

  private Button wValidateAll;

  private Validation selectedField;

  private Label wlDescription;
  private Text wDescription;

  private Label wlFieldName;
  private CCombo wFieldName;

  private Button wNullAllowed;

  private Button wOnlyNullAllowed;

  private Button wOnlyNumeric;

  private final java.util.List<Validation> selectionList;
  private TextVar wMaxLength;
  private TextVar wMinLength;
  private Group wgData;
  private Group wgType;
  private Button wDataTypeVerified;
  private CCombo wDataType;
  private TextVar wConversionMask;
  private TextVar wDecimalSymbol;
  private TextVar wGroupingSymbol;
  private TextVar wMaxValue;
  private TextVar wMinValue;
  private Label wlAllowedValues;
  private List wAllowedValues;
  private Button wSourceValues;
  private Label wlSourceTransform;
  private CCombo wSourceTransform;
  private Label wlSourceField;
  private CCombo wSourceField;

  private Button wbAddAllowed;
  private Button wbRemoveAllowed;

  private TextVar wStartStringExpected;
  private TextVar wEndStringExpected;
  private TextVar wStartStringDisallowed;
  private TextVar wEndStringDisallowed;
  private TextVar wRegExpExpected;
  private TextVar wRegExpDisallowed;
  private Label wlErrorCode;
  private TextVar wErrorCode;
  private Label wlErrorDescription;
  private TextVar wErrorDescription;
  private Button wConcatErrors;
  private TextVar wConcatSeparator;

  public ValidatorDialog(
      Shell parent, IVariables variables, ValidatorMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;

    // Just to make sure everything is nicely in sync...
    //
    java.util.List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    for (int i = 0; i < infoStreams.size(); i++) {
      input.getValidations().get(i).setSourcingTransformName(infoStreams.get(i).getTransformName());
    }

    selectedField = null;
    selectionList = new ArrayList<>();

    // Copy the data from the input into the map...
    //
    for (Validation field : input.getValidations()) {
      selectionList.add(field.clone());
    }
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ValidatorDialog.Transform.Name"));

    buildButtonBar()
        .ok(e -> ok())
        .custom(
            BaseMessages.getString(PKG, "ValidatorDialog.NewButton.Label"), e -> newValidation())
        .custom(
            BaseMessages.getString(PKG, "ValidatorDialog.ClearButton.Label"),
            e -> clearValidation())
        .cancel(e -> cancel())
        .build();

    // List of fields to the left...
    //
    Label wlFieldList = new Label(shell, SWT.LEFT);
    wlFieldList.setText(BaseMessages.getString(PKG, "ValidatorDialog.FieldList.Label"));
    PropsUi.setLook(wlFieldList);
    FormData fdlFieldList = new FormData();
    fdlFieldList.left = new FormAttachment(0, margin);
    fdlFieldList.right = new FormAttachment(middle, -margin);
    fdlFieldList.top = new FormAttachment(wSpacer, margin);
    wlFieldList.setLayoutData(fdlFieldList);
    wValidationsList =
        new List(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);

    PropsUi.setLook(wValidationsList);
    wValidationsList.addListener(
        SWT.Selection, e -> showSelectedValidatorField(wValidationsList.getSelection()[0]));

    FormData fdFieldList = new FormData();
    fdFieldList.left = new FormAttachment(0, margin);
    fdFieldList.top = new FormAttachment(wlFieldList, margin);
    fdFieldList.right = new FormAttachment(middle / 2, -margin);
    fdFieldList.bottom = new FormAttachment(wOk, -margin);
    wValidationsList.setLayoutData(fdFieldList);

    // General: an option to allow ALL the options to be checked.
    //
    wValidateAll = new Button(shell, SWT.CHECK);
    wValidateAll.setText(BaseMessages.getString(PKG, "ValidatorDialog.ValidateAll.Label"));
    PropsUi.setLook(wValidateAll);
    FormData fdValidateAll = new FormData();
    fdValidateAll.left = new FormAttachment(middle, 0);
    fdValidateAll.right = new FormAttachment(100, 0);
    fdValidateAll.top = new FormAttachment(wSpacer, margin);
    wValidateAll.setLayoutData(fdValidateAll);
    wValidateAll.addListener(SWT.Selection, e -> setFlags());

    // General: When validating all options, still output a single row, errors concatenated
    //
    wConcatErrors = new Button(shell, SWT.CHECK);
    wConcatErrors.setText(BaseMessages.getString(PKG, "ValidatorDialog.ConcatErrors.Label"));
    PropsUi.setLook(wConcatErrors);
    FormData fdConcatErrors = new FormData();
    fdConcatErrors.left = new FormAttachment(middle, 0);
    fdConcatErrors.top = new FormAttachment(wValidateAll, margin);
    wConcatErrors.setLayoutData(fdConcatErrors);
    wConcatErrors.addListener(SWT.Selection, e -> setFlags());

    // The separator
    //
    wConcatSeparator = new TextVar(variables, shell, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wConcatSeparator);
    FormData fdConcatSeparator = new FormData();
    fdConcatSeparator.left = new FormAttachment(wConcatErrors, margin);
    fdConcatSeparator.right = new FormAttachment(100, 0);
    fdConcatSeparator.top = new FormAttachment(wValidateAll, margin);
    wConcatSeparator.setLayoutData(fdConcatSeparator);

    // Create a scrolled composite on the right side...
    //
    ScrolledComposite wSComp = new ScrolledComposite(shell, SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wSComp);
    wSComp.setLayout(new FillLayout());
    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(middle / 2, margin);
    fdComp.top = new FormAttachment(wConcatSeparator, margin);
    fdComp.right = new FormAttachment(100, -margin);
    fdComp.bottom = new FormAttachment(wOk, -margin);
    // Limit viewport size so the dialog opens at a reasonable size; content scrolls inside.
    fdComp.width = 550;
    fdComp.height = 450;
    wSComp.setLayoutData(fdComp);

    Composite wComp = new Composite(wSComp, SWT.BORDER);
    PropsUi.setLook(wComp);
    FormLayout compLayout = new FormLayout();
    compLayout.marginWidth = 3;
    compLayout.marginHeight = 3;
    wComp.setLayout(compLayout);

    // Description (list key)
    //
    wlDescription = new Label(wComp, SWT.RIGHT);
    wlDescription.setText(BaseMessages.getString(PKG, "ValidatorDialog.Description.Label"));
    PropsUi.setLook(wlDescription);
    FormData fdlDescription = new FormData();
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    fdlDescription.top = new FormAttachment(0, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.left = new FormAttachment(middle, margin);
    fdDescription.right = new FormAttachment(100, 0);
    fdDescription.top = new FormAttachment(0, 0);
    wDescription.setLayoutData(fdDescription);
    wDescription.addListener(
        SWT.Modify,
        event -> {
          // See if there is a selected Validation
          //
          if (wValidationsList != null
              && wValidationsList.getItemCount() > 0
              && wValidationsList.getSelection().length == 1) {
            int index = wValidationsList.getSelectionIndex();
            String description = wValidationsList.getItem(index);
            Validation validation = Validation.findValidation(selectionList, description);
            if (validation != null) {
              String newDescription = wDescription.getText();
              validation.setName(newDescription);
              wValidationsList.setItem(index, newDescription);
              wValidationsList.select(index);
            }
          }
        });

    // The name of the field to validate
    //
    wlFieldName = new Label(wComp, SWT.RIGHT);
    wlFieldName.setText(BaseMessages.getString(PKG, "ValidatorDialog.FieldName.Label"));
    PropsUi.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment(0, 0);
    fdlFieldName.right = new FormAttachment(middle, -margin);
    fdlFieldName.top = new FormAttachment(wDescription, margin);
    wlFieldName.setLayoutData(fdlFieldName);
    wFieldName = new CCombo(wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldName);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(middle, margin);
    fdFieldName.right = new FormAttachment(100, 0);
    fdFieldName.top = new FormAttachment(wDescription, margin);
    wFieldName.setLayoutData(fdFieldName);

    // Consider: grab field list in thread in the background...
    //
    try {
      inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      wFieldName.setItems(inputFields.getFieldNames());
    } catch (HopTransformException ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "ValidatorDialog.Exception.CantGetFieldsFromPreviousTransforms.Title"),
          BaseMessages.getString(
              PKG, "ValidatorDialog.Exception.CantGetFieldsFromPreviousTransforms.Message"),
          ex);
    }

    // ErrorCode
    //
    wlErrorCode = new Label(wComp, SWT.RIGHT);
    wlErrorCode.setText(BaseMessages.getString(PKG, "ValidatorDialog.ErrorCode.Label"));
    PropsUi.setLook(wlErrorCode);
    FormData fdlErrorCode = new FormData();
    fdlErrorCode.left = new FormAttachment(0, 0);
    fdlErrorCode.right = new FormAttachment(middle, -margin);
    fdlErrorCode.top = new FormAttachment(wFieldName, margin);
    wlErrorCode.setLayoutData(fdlErrorCode);
    wErrorCode = new TextVar(variables, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorCode);
    FormData fdErrorCode = new FormData();
    fdErrorCode.left = new FormAttachment(middle, margin);
    fdErrorCode.right = new FormAttachment(100, 0);
    fdErrorCode.top = new FormAttachment(wFieldName, margin);
    wErrorCode.setLayoutData(fdErrorCode);
    addSpacesWarning(wErrorCode);

    // ErrorDescription
    //
    wlErrorDescription = new Label(wComp, SWT.RIGHT);
    wlErrorDescription.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.ErrorDescription.Label"));
    PropsUi.setLook(wlErrorDescription);
    FormData fdlErrorDescription = new FormData();
    fdlErrorDescription.left = new FormAttachment(0, 0);
    fdlErrorDescription.right = new FormAttachment(middle, -margin);
    fdlErrorDescription.top = new FormAttachment(wErrorCode, margin);
    wlErrorDescription.setLayoutData(fdlErrorDescription);
    wErrorDescription = new TextVar(variables, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorDescription);
    FormData fdErrorDescription = new FormData();
    fdErrorDescription.left = new FormAttachment(middle, margin);
    fdErrorDescription.right = new FormAttachment(100, 0);
    fdErrorDescription.top = new FormAttachment(wErrorCode, margin);
    wErrorDescription.setLayoutData(fdErrorDescription);
    addSpacesWarning(wErrorDescription);

    // Data type validations & constants masks...
    //
    wgType = new Group(wComp, SWT.NONE);
    PropsUi.setLook(wgType);
    wgType.setText(BaseMessages.getString(PKG, "ValidatorDialog.TypeGroup.Label"));
    FormLayout typeGroupLayout = new FormLayout();
    typeGroupLayout.marginHeight = Const.FORM_MARGIN;
    typeGroupLayout.marginWidth = Const.FORM_MARGIN;
    wgType.setLayout(typeGroupLayout);
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(0, 0);
    fdType.right = new FormAttachment(100, 0);
    fdType.top = new FormAttachment(wErrorDescription, margin);
    wgType.setLayoutData(fdType);

    // Check for data type correctness?
    //
    Label wlDataTypeVerified = new Label(wgType, SWT.RIGHT);
    wlDataTypeVerified.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.DataTypeVerified.Label"));
    PropsUi.setLook(wlDataTypeVerified);
    FormData fdlDataTypeVerified = new FormData();
    fdlDataTypeVerified.left = new FormAttachment(0, 0);
    fdlDataTypeVerified.right = new FormAttachment(middle, -margin);
    fdlDataTypeVerified.top = new FormAttachment(0, 0);
    wlDataTypeVerified.setLayoutData(fdlDataTypeVerified);
    wDataTypeVerified = new Button(wgType, SWT.CHECK);
    PropsUi.setLook(wDataTypeVerified);
    FormData fdDataTypeVerified = new FormData();
    fdDataTypeVerified.left = new FormAttachment(middle, margin);
    fdDataTypeVerified.right = new FormAttachment(100, 0);
    fdDataTypeVerified.top = new FormAttachment(wlDataTypeVerified, 0, SWT.CENTER);
    wDataTypeVerified.setLayoutData(fdDataTypeVerified);

    // Data type
    //
    Label wlDataType = new Label(wgType, SWT.RIGHT);
    wlDataType.setText(BaseMessages.getString(PKG, "ValidatorDialog.DataType.Label"));
    PropsUi.setLook(wlDataType);
    FormData fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment(0, 0);
    fdlDataType.right = new FormAttachment(middle, -margin);
    fdlDataType.top = new FormAttachment(wlDataTypeVerified, margin);
    wlDataType.setLayoutData(fdlDataType);
    wDataType = new CCombo(wgType, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDataType.setItems(ValueMetaFactory.getValueMetaNames());
    PropsUi.setLook(wDataType);
    FormData fdDataType = new FormData();
    fdDataType.left = new FormAttachment(middle, margin);
    fdDataType.right = new FormAttachment(100, 0);
    fdDataType.top = new FormAttachment(wlDataType, 0, SWT.CENTER);
    wDataType.setLayoutData(fdDataType);

    // Conversion mask
    //
    Label wlConversionMask = new Label(wgType, SWT.RIGHT);
    wlConversionMask.setText(BaseMessages.getString(PKG, "ValidatorDialog.ConversionMask.Label"));
    PropsUi.setLook(wlConversionMask);
    FormData fdlConversionMask = new FormData();
    fdlConversionMask.left = new FormAttachment(0, 0);
    fdlConversionMask.right = new FormAttachment(middle, -margin);
    fdlConversionMask.top = new FormAttachment(wlDataType, margin);
    wlConversionMask.setLayoutData(fdlConversionMask);
    wConversionMask = new TextVar(variables, wgType, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wConversionMask);
    FormData fdConversionMask = new FormData();
    fdConversionMask.left = new FormAttachment(middle, margin);
    fdConversionMask.right = new FormAttachment(100, 0);
    fdConversionMask.top = new FormAttachment(wDataType, margin);
    wConversionMask.setLayoutData(fdConversionMask);
    addSpacesWarning(wConversionMask);

    // Decimal Symbol
    //
    Label wlDecimalSymbol = new Label(wgType, SWT.RIGHT);
    wlDecimalSymbol.setText(BaseMessages.getString(PKG, "ValidatorDialog.DecimalSymbol.Label"));
    PropsUi.setLook(wlDecimalSymbol);
    FormData fdlDecimalSymbol = new FormData();
    fdlDecimalSymbol.left = new FormAttachment(0, 0);
    fdlDecimalSymbol.right = new FormAttachment(middle, -margin);
    fdlDecimalSymbol.top = new FormAttachment(wConversionMask, margin);
    wlDecimalSymbol.setLayoutData(fdlDecimalSymbol);
    wDecimalSymbol = new TextVar(variables, wgType, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDecimalSymbol);
    FormData fdDecimalSymbol = new FormData();
    fdDecimalSymbol.left = new FormAttachment(middle, margin);
    fdDecimalSymbol.right = new FormAttachment(100, 0);
    fdDecimalSymbol.top = new FormAttachment(wConversionMask, margin);
    wDecimalSymbol.setLayoutData(fdDecimalSymbol);
    addSpacesWarning(wDecimalSymbol);

    // Grouping Symbol
    //
    Label wlGroupingSymbol = new Label(wgType, SWT.RIGHT);
    wlGroupingSymbol.setText(BaseMessages.getString(PKG, "ValidatorDialog.GroupingSymbol.Label"));
    PropsUi.setLook(wlGroupingSymbol);
    FormData fdlGroupingSymbol = new FormData();
    fdlGroupingSymbol.left = new FormAttachment(0, 0);
    fdlGroupingSymbol.right = new FormAttachment(middle, -margin);
    fdlGroupingSymbol.top = new FormAttachment(wDecimalSymbol, margin);
    wlGroupingSymbol.setLayoutData(fdlGroupingSymbol);
    wGroupingSymbol = new TextVar(variables, wgType, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wGroupingSymbol);
    FormData fdGroupingSymbol = new FormData();
    fdGroupingSymbol.left = new FormAttachment(middle, margin);
    fdGroupingSymbol.right = new FormAttachment(100, 0);
    fdGroupingSymbol.top = new FormAttachment(wDecimalSymbol, margin);
    wGroupingSymbol.setLayoutData(fdGroupingSymbol);
    addSpacesWarning(wGroupingSymbol);

    // /////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // The data group...
    //
    //
    wgData = new Group(wComp, SWT.NONE);
    PropsUi.setLook(wgData);
    wgData.setText(BaseMessages.getString(PKG, "ValidatorDialog.DataGroup.Label"));
    wgData.setLayout(props.createFormLayout());
    FormData fdData = new FormData();
    fdData.left = new FormAttachment(0, 0);
    fdData.right = new FormAttachment(100, 0);
    fdData.top = new FormAttachment(wgType, margin);
    wgData.setLayoutData(fdData);

    // Check for null?
    //
    Label wlNullAllowed = new Label(wgData, SWT.RIGHT);
    wlNullAllowed.setText(BaseMessages.getString(PKG, "ValidatorDialog.NullAllowed.Label"));
    PropsUi.setLook(wlNullAllowed);
    FormData fdlNullAllowed = new FormData();
    fdlNullAllowed.left = new FormAttachment(0, 0);
    fdlNullAllowed.right = new FormAttachment(middle, -margin);
    fdlNullAllowed.top = new FormAttachment(0, 0);
    wlNullAllowed.setLayoutData(fdlNullAllowed);
    wNullAllowed = new Button(wgData, SWT.CHECK);
    PropsUi.setLook(wNullAllowed);
    FormData fdNullAllowed = new FormData();
    fdNullAllowed.left = new FormAttachment(middle, margin);
    fdNullAllowed.right = new FormAttachment(100, 0);
    fdNullAllowed.top = new FormAttachment(wlNullAllowed, 0, SWT.CENTER);
    wNullAllowed.setLayoutData(fdNullAllowed);

    // Only null allowed?
    //
    Label wlOnlyNullAllowed = new Label(wgData, SWT.RIGHT);
    wlOnlyNullAllowed.setText(BaseMessages.getString(PKG, "ValidatorDialog.OnlyNullAllowed.Label"));
    PropsUi.setLook(wlOnlyNullAllowed);
    FormData fdlOnlyNullAllowed = new FormData();
    fdlOnlyNullAllowed.left = new FormAttachment(0, 0);
    fdlOnlyNullAllowed.right = new FormAttachment(middle, -margin);
    fdlOnlyNullAllowed.top = new FormAttachment(wlNullAllowed, margin);
    wlOnlyNullAllowed.setLayoutData(fdlOnlyNullAllowed);
    wOnlyNullAllowed = new Button(wgData, SWT.CHECK);
    PropsUi.setLook(wOnlyNullAllowed);
    FormData fdOnlyNullAllowed = new FormData();
    fdOnlyNullAllowed.left = new FormAttachment(middle, margin);
    fdOnlyNullAllowed.right = new FormAttachment(100, 0);
    fdOnlyNullAllowed.top = new FormAttachment(wlOnlyNullAllowed, 0, SWT.CENTER);
    wOnlyNullAllowed.setLayoutData(fdOnlyNullAllowed);

    // Only numeric allowed?
    //
    Label wlOnlyNumeric = new Label(wgData, SWT.RIGHT);
    wlOnlyNumeric.setText(BaseMessages.getString(PKG, "ValidatorDialog.OnlyNumeric.Label"));
    PropsUi.setLook(wlOnlyNumeric);
    FormData fdlOnlyNumeric = new FormData();
    fdlOnlyNumeric.left = new FormAttachment(0, 0);
    fdlOnlyNumeric.right = new FormAttachment(middle, -margin);
    fdlOnlyNumeric.top = new FormAttachment(wlOnlyNullAllowed, margin);
    wlOnlyNumeric.setLayoutData(fdlOnlyNumeric);
    wOnlyNumeric = new Button(wgData, SWT.CHECK);
    PropsUi.setLook(wOnlyNumeric);
    FormData fdOnlyNumeric = new FormData();
    fdOnlyNumeric.left = new FormAttachment(middle, margin);
    fdOnlyNumeric.right = new FormAttachment(100, 0);
    fdOnlyNumeric.top = new FormAttachment(wlOnlyNumeric, 0, SWT.CENTER);
    wOnlyNumeric.setLayoutData(fdOnlyNumeric);

    // Maximum length
    //
    Label wlMaxLength = new Label(wgData, SWT.RIGHT);
    wlMaxLength.setText(BaseMessages.getString(PKG, "ValidatorDialog.MaxLength.Label"));
    PropsUi.setLook(wlMaxLength);
    FormData fdlMaxLength = new FormData();
    fdlMaxLength.left = new FormAttachment(0, 0);
    fdlMaxLength.right = new FormAttachment(middle, -margin);
    fdlMaxLength.top = new FormAttachment(wlOnlyNumeric, margin);
    wlMaxLength.setLayoutData(fdlMaxLength);
    wMaxLength = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxLength);
    FormData fdMaxLength = new FormData();
    fdMaxLength.left = new FormAttachment(middle, margin);
    fdMaxLength.right = new FormAttachment(100, 0);
    fdMaxLength.top = new FormAttachment(wlMaxLength, 0, SWT.CENTER);
    wMaxLength.setLayoutData(fdMaxLength);
    addSpacesWarning(wMaxLength);

    // Minimum length
    //
    Label wlMinLength = new Label(wgData, SWT.RIGHT);
    wlMinLength.setText(BaseMessages.getString(PKG, "ValidatorDialog.MinLength.Label"));
    PropsUi.setLook(wlMinLength);
    FormData fdlMinLength = new FormData();
    fdlMinLength.left = new FormAttachment(0, 0);
    fdlMinLength.right = new FormAttachment(middle, -margin);
    fdlMinLength.top = new FormAttachment(wMaxLength, margin);
    wlMinLength.setLayoutData(fdlMinLength);
    wMinLength = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinLength);
    FormData fdMinLength = new FormData();
    fdMinLength.left = new FormAttachment(middle, margin);
    fdMinLength.right = new FormAttachment(100, 0);
    fdMinLength.top = new FormAttachment(wMaxLength, margin);
    wMinLength.setLayoutData(fdMinLength);
    addSpacesWarning(wMinLength);

    // Maximum value
    //
    Label wlMaxValue = new Label(wgData, SWT.RIGHT);
    wlMaxValue.setText(BaseMessages.getString(PKG, "ValidatorDialog.MaxValue.Label"));
    PropsUi.setLook(wlMaxValue);
    FormData fdlMaxValue = new FormData();
    fdlMaxValue.left = new FormAttachment(0, 0);
    fdlMaxValue.right = new FormAttachment(middle, -margin);
    fdlMaxValue.top = new FormAttachment(wMinLength, margin);
    wlMaxValue.setLayoutData(fdlMaxValue);
    wMaxValue = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxValue);
    FormData fdMaxValue = new FormData();
    fdMaxValue.left = new FormAttachment(middle, margin);
    fdMaxValue.right = new FormAttachment(100, 0);
    fdMaxValue.top = new FormAttachment(wMinLength, margin);
    wMaxValue.setLayoutData(fdMaxValue);
    addSpacesWarning(wMaxValue);

    // Minimum value
    //
    Label wlMinValue = new Label(wgData, SWT.RIGHT);
    wlMinValue.setText(BaseMessages.getString(PKG, "ValidatorDialog.MinValue.Label"));
    PropsUi.setLook(wlMinValue);
    FormData fdlMinValue = new FormData();
    fdlMinValue.left = new FormAttachment(0, 0);
    fdlMinValue.right = new FormAttachment(middle, -margin);
    fdlMinValue.top = new FormAttachment(wMaxValue, margin);
    wlMinValue.setLayoutData(fdlMinValue);
    wMinValue = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinValue);
    FormData fdMinValue = new FormData();
    fdMinValue.left = new FormAttachment(middle, margin);
    fdMinValue.right = new FormAttachment(100, 0);
    fdMinValue.top = new FormAttachment(wMaxValue, margin);
    wMinValue.setLayoutData(fdMinValue);
    addSpacesWarning(wMinValue);

    // Expected start string
    //
    Label wlStartStringExpected = new Label(wgData, SWT.RIGHT);
    wlStartStringExpected.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.StartStringExpected.Label"));
    PropsUi.setLook(wlStartStringExpected);
    FormData fdlStartStringExpected = new FormData();
    fdlStartStringExpected.left = new FormAttachment(0, 0);
    fdlStartStringExpected.right = new FormAttachment(middle, -margin);
    fdlStartStringExpected.top = new FormAttachment(wMinValue, margin);
    wlStartStringExpected.setLayoutData(fdlStartStringExpected);
    wStartStringExpected = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStartStringExpected);
    FormData fdStartStringExpected = new FormData();
    fdStartStringExpected.left = new FormAttachment(middle, margin);
    fdStartStringExpected.right = new FormAttachment(100, 0);
    fdStartStringExpected.top = new FormAttachment(wMinValue, margin);
    wStartStringExpected.setLayoutData(fdStartStringExpected);
    addSpacesWarning(wStartStringExpected);

    // Expected End string
    //
    Label wlEndStringExpected = new Label(wgData, SWT.RIGHT);
    wlEndStringExpected.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.EndStringExpected.Label"));
    PropsUi.setLook(wlEndStringExpected);
    FormData fdlEndStringExpected = new FormData();
    fdlEndStringExpected.left = new FormAttachment(0, 0);
    fdlEndStringExpected.right = new FormAttachment(middle, -margin);
    fdlEndStringExpected.top = new FormAttachment(wStartStringExpected, margin);
    wlEndStringExpected.setLayoutData(fdlEndStringExpected);
    wEndStringExpected = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEndStringExpected);
    FormData fdEndStringExpected = new FormData();
    fdEndStringExpected.left = new FormAttachment(middle, margin);
    fdEndStringExpected.right = new FormAttachment(100, 0);
    fdEndStringExpected.top = new FormAttachment(wStartStringExpected, margin);
    wEndStringExpected.setLayoutData(fdEndStringExpected);
    addSpacesWarning(wEndStringExpected);

    // Disallowed start string
    //
    Label wlStartStringDisallowed = new Label(wgData, SWT.RIGHT);
    wlStartStringDisallowed.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.StartStringDisallowed.Label"));
    PropsUi.setLook(wlStartStringDisallowed);
    FormData fdlStartStringDisallowed = new FormData();
    fdlStartStringDisallowed.left = new FormAttachment(0, 0);
    fdlStartStringDisallowed.right = new FormAttachment(middle, -margin);
    fdlStartStringDisallowed.top = new FormAttachment(wEndStringExpected, margin);
    wlStartStringDisallowed.setLayoutData(fdlStartStringDisallowed);
    wStartStringDisallowed = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStartStringDisallowed);
    FormData fdStartStringDisallowed = new FormData();
    fdStartStringDisallowed.left = new FormAttachment(middle, margin);
    fdStartStringDisallowed.right = new FormAttachment(100, 0);
    fdStartStringDisallowed.top = new FormAttachment(wEndStringExpected, margin);
    wStartStringDisallowed.setLayoutData(fdStartStringDisallowed);
    addSpacesWarning(wStartStringDisallowed);

    // Disallowed End string
    //
    Label wlEndStringDisallowed = new Label(wgData, SWT.RIGHT);
    wlEndStringDisallowed.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.EndStringDisallowed.Label"));
    PropsUi.setLook(wlEndStringDisallowed);
    FormData fdlEndStringDisallowed = new FormData();
    fdlEndStringDisallowed.left = new FormAttachment(0, 0);
    fdlEndStringDisallowed.right = new FormAttachment(middle, -margin);
    fdlEndStringDisallowed.top = new FormAttachment(wStartStringDisallowed, margin);
    wlEndStringDisallowed.setLayoutData(fdlEndStringDisallowed);
    wEndStringDisallowed = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEndStringDisallowed);
    FormData fdEndStringDisallowed = new FormData();
    fdEndStringDisallowed.left = new FormAttachment(middle, margin);
    fdEndStringDisallowed.right = new FormAttachment(100, 0);
    fdEndStringDisallowed.top = new FormAttachment(wStartStringDisallowed, margin);
    wEndStringDisallowed.setLayoutData(fdEndStringDisallowed);
    addSpacesWarning(wEndStringDisallowed);

    // Expected regular expression
    //
    Label wlRegExpExpected = new Label(wgData, SWT.RIGHT);
    wlRegExpExpected.setText(BaseMessages.getString(PKG, "ValidatorDialog.RegExpExpected.Label"));
    PropsUi.setLook(wlRegExpExpected);
    FormData fdlRegExpExpected = new FormData();
    fdlRegExpExpected.left = new FormAttachment(0, 0);
    fdlRegExpExpected.right = new FormAttachment(middle, -margin);
    fdlRegExpExpected.top = new FormAttachment(wEndStringDisallowed, margin);
    wlRegExpExpected.setLayoutData(fdlRegExpExpected);
    wRegExpExpected = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRegExpExpected);
    FormData fdRegExpExpected = new FormData();
    fdRegExpExpected.left = new FormAttachment(middle, margin);
    fdRegExpExpected.right = new FormAttachment(100, 0);
    fdRegExpExpected.top = new FormAttachment(wEndStringDisallowed, margin);
    wRegExpExpected.setLayoutData(fdRegExpExpected);
    addSpacesWarning(wRegExpExpected);

    // Disallowed regular expression
    //
    Label wlRegExpDisallowed = new Label(wgData, SWT.RIGHT);
    wlRegExpDisallowed.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.RegExpDisallowed.Label"));
    PropsUi.setLook(wlRegExpDisallowed);
    FormData fdlRegExpDisallowed = new FormData();
    fdlRegExpDisallowed.left = new FormAttachment(0, 0);
    fdlRegExpDisallowed.right = new FormAttachment(middle, -margin);
    fdlRegExpDisallowed.top = new FormAttachment(wRegExpExpected, margin);
    wlRegExpDisallowed.setLayoutData(fdlRegExpDisallowed);
    wRegExpDisallowed = new TextVar(variables, wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRegExpDisallowed);
    FormData fdRegExpDisallowed = new FormData();
    fdRegExpDisallowed.left = new FormAttachment(middle, margin);
    fdRegExpDisallowed.right = new FormAttachment(100, 0);
    fdRegExpDisallowed.top = new FormAttachment(wRegExpExpected, margin);
    wRegExpDisallowed.setLayoutData(fdRegExpDisallowed);
    addSpacesWarning(wRegExpDisallowed);

    // Allowed values: a list box.
    //
    // Add an entry
    //
    wbAddAllowed = new Button(wgData, SWT.PUSH);
    wbAddAllowed.setText(BaseMessages.getString(PKG, "ValidatorDialog.ButtonAddAllowed.Label"));
    FormData fdbAddAllowed = new FormData();
    fdbAddAllowed.right = new FormAttachment(100, 0);
    fdbAddAllowed.top = new FormAttachment(wRegExpDisallowed, margin);
    wbAddAllowed.setLayoutData(fdbAddAllowed);
    wbAddAllowed.addListener(SWT.Selection, e -> addAllowedValue());

    // Remove an entry
    //
    wbRemoveAllowed = new Button(wgData, SWT.PUSH);
    wbRemoveAllowed.setText(
        BaseMessages.getString(PKG, "ValidatorDialog.ButtonRemoveAllowed.Label"));
    FormData fdbRemoveAllowed = new FormData();
    fdbRemoveAllowed.right = new FormAttachment(100, 0);
    fdbRemoveAllowed.top = new FormAttachment(wbAddAllowed, margin);
    wbRemoveAllowed.setLayoutData(fdbRemoveAllowed);
    wbRemoveAllowed.addListener(SWT.Selection, e -> removeAllowedValue());

    wlAllowedValues = new Label(wgData, SWT.RIGHT);
    wlAllowedValues.setText(BaseMessages.getString(PKG, "ValidatorDialog.AllowedValues.Label"));
    PropsUi.setLook(wlAllowedValues);
    FormData fdlAllowedValues = new FormData();
    fdlAllowedValues.left = new FormAttachment(0, 0);
    fdlAllowedValues.right = new FormAttachment(middle, -margin);
    fdlAllowedValues.top = new FormAttachment(wRegExpDisallowed, margin);
    wlAllowedValues.setLayoutData(fdlAllowedValues);
    wAllowedValues =
        new List(wgData, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wAllowedValues);
    FormData fdAllowedValues = new FormData();
    fdAllowedValues.left = new FormAttachment(middle, margin);
    fdAllowedValues.right = new FormAttachment(wbRemoveAllowed, -20);
    fdAllowedValues.top = new FormAttachment(wRegExpDisallowed, margin);
    fdAllowedValues.bottom =
        new FormAttachment(wRegExpDisallowed, (int) (props.getZoomFactor() * 150));
    wAllowedValues.setLayoutData(fdAllowedValues);

    // Source allowed values from another transform?
    //
    Label wlSourceValues = new Label(wgData, SWT.RIGHT);
    wlSourceValues.setText(BaseMessages.getString(PKG, "ValidatorDialog.SourceValues.Label"));
    PropsUi.setLook(wlSourceValues);
    FormData fdlSourceValues = new FormData();
    fdlSourceValues.left = new FormAttachment(0, 0);
    fdlSourceValues.right = new FormAttachment(middle, -margin);
    fdlSourceValues.top = new FormAttachment(wAllowedValues, margin);
    wlSourceValues.setLayoutData(fdlSourceValues);
    wSourceValues = new Button(wgData, SWT.CHECK);
    PropsUi.setLook(wSourceValues);
    FormData fdSourceValues = new FormData();
    fdSourceValues.left = new FormAttachment(middle, margin);
    fdSourceValues.right = new FormAttachment(100, 0);
    fdSourceValues.top = new FormAttachment(wlSourceValues, 0, SWT.CENTER);
    wSourceValues.setLayoutData(fdSourceValues);
    wSourceValues.addListener(SWT.Selection, e -> enableFields());

    // Source allowed values : source transform
    //
    wlSourceTransform = new Label(wgData, SWT.RIGHT);
    wlSourceTransform.setText(BaseMessages.getString(PKG, "ValidatorDialog.SourceTransform.Label"));
    PropsUi.setLook(wlSourceTransform);
    FormData fdlSourceTransform = new FormData();
    fdlSourceTransform.left = new FormAttachment(0, margin);
    fdlSourceTransform.right = new FormAttachment(middle, -margin);
    fdlSourceTransform.top = new FormAttachment(wlSourceValues, margin);
    wlSourceTransform.setLayoutData(fdlSourceTransform);
    wSourceTransform = new CCombo(wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSourceTransform);
    FormData fdSourceTransform = new FormData();
    fdSourceTransform.left = new FormAttachment(middle, margin);
    fdSourceTransform.right = new FormAttachment(100, 0);
    fdSourceTransform.top = new FormAttachment(wSourceValues, margin);
    wSourceTransform.setLayoutData(fdSourceTransform);
    wSourceTransform.addListener(SWT.FocusIn, e -> getTransforms());
    wSourceTransform.addListener(SWT.Selection, e -> getTransforms());

    // Source allowed values : source field
    //
    wlSourceField = new Label(wgData, SWT.RIGHT);
    wlSourceField.setText(BaseMessages.getString(PKG, "ValidatorDialog.SourceField.Label"));
    PropsUi.setLook(wlSourceField);
    FormData fdlSourceField = new FormData();
    fdlSourceField.left = new FormAttachment(0, margin);
    fdlSourceField.right = new FormAttachment(middle, -margin);
    fdlSourceField.top = new FormAttachment(wSourceTransform, margin);
    wlSourceField.setLayoutData(fdlSourceField);
    wSourceField = new CCombo(wgData, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSourceField);
    FormData fdSourceField = new FormData();
    fdSourceField.left = new FormAttachment(middle, margin);
    fdSourceField.right = new FormAttachment(100, 0);
    fdSourceField.top = new FormAttachment(wSourceTransform, margin);
    wSourceField.setLayoutData(fdSourceField);
    wSourceField.addListener(SWT.FocusIn, e -> getFields());
    wSourceField.addListener(SWT.Selection, e -> getFields());

    wComp.layout();
    wComp.pack();
    Rectangle bounds = wComp.getBounds();

    wSComp.setContent(wComp);
    wSComp.setExpandHorizontal(true);
    wSComp.setExpandVertical(true);
    // Use full content size so scrollbars appear and the entire form is reachable.
    wSComp.setMinWidth(bounds.width);
    wSComp.setMinHeight(bounds.height);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void showSelectedValidatorField(String selection) {
    // Someone hit a field...
    //
    saveChanges();

    Validation field = Validation.findValidation(selectionList, selection);
    if (field == null) {
      field = new Validation(selection);
      IValueMeta valueMeta = inputFields.searchValueMeta(selection);
      if (valueMeta != null) {
        // Set the default data type
        //
        field.setDataType(valueMeta.getTypeDesc());
      }
    }

    selectedField = field;

    getValidatorFieldData(selectedField);

    enableFields();
  }

  private void saveChanges() {
    if (selectedField != null) {
      // First grab the info from the dialog...
      //
      selectedField.setFieldName(wFieldName.getText());

      selectedField.setErrorCode(wErrorCode.getText());
      selectedField.setErrorDescription(wErrorDescription.getText());

      selectedField.setDataTypeVerified(wDataTypeVerified.getSelection());
      selectedField.setDataType(wDataType.getText());
      selectedField.setConversionMask(wConversionMask.getText());
      selectedField.setDecimalSymbol(wDecimalSymbol.getText());
      selectedField.setGroupingSymbol(wGroupingSymbol.getText());

      selectedField.setNullAllowed(wNullAllowed.getSelection());
      selectedField.setOnlyNullAllowed(wOnlyNullAllowed.getSelection());
      selectedField.setOnlyNumericAllowed(wOnlyNumeric.getSelection());

      selectedField.setMaximumLength(wMaxLength.getText());
      selectedField.setMinimumLength(wMinLength.getText());
      selectedField.setMaximumValue(wMaxValue.getText());
      selectedField.setMinimumValue(wMinValue.getText());

      selectedField.setStartString(wStartStringExpected.getText());
      selectedField.setEndString(wEndStringExpected.getText());
      selectedField.setStartStringNotAllowed(wStartStringDisallowed.getText());
      selectedField.setEndStringNotAllowed(wEndStringDisallowed.getText());

      selectedField.setRegularExpression(wRegExpExpected.getText());
      selectedField.setRegularExpressionNotAllowed(wRegExpDisallowed.getText());

      selectedField.setAllowedValues(new ArrayList<>(Arrays.asList(wAllowedValues.getItems())));

      selectedField.setSourcingValues(wSourceValues.getSelection());
      selectedField.setSourcingField(wSourceField.getText());
      selectedField.setSourcingTransformName(wSourceTransform.getText());
      selectedField.setSourcingTransform(pipelineMeta.findTransform(wSourceTransform.getText()));

      // Save the old info in the map
      //
      // selectionList.add(selectedField);
    }
  }

  protected void setFlags() {
    wConcatErrors.setEnabled(wValidateAll.getSelection());
    wConcatSeparator.setEnabled(wConcatErrors.getSelection());
  }

  private void addSpacesWarning(TextVar text) {
    Text widget = text.getTextWidget();
    widget.addListener(
        SWT.Modify,
        e -> {
          boolean showWarning = false;
          String message = null;

          // Only spaces
          //
          if (spacesValidation(text.getText())) {
            showWarning = true;
            message = BaseMessages.getString(PKG, "System.Warning.OnlySpaces");
          } else if (trailingSpacesValidation(text.getText())) {
            showWarning = true;
            message = BaseMessages.getString(PKG, "System.Warning.TrailingSpaces");
          }

          // Red/White color if there's an issue.
          //
          if (showWarning) {
            widget.setBackground(GuiResource.getInstance().getColorRed());
            widget.setForeground(GuiResource.getInstance().getColorWhite());
            widget.setToolTipText(message);
          } else {
            // Reset to the defaults
            PropsUi.setLook(widget);
            widget.setToolTipText("");
          }
        });
  }

  public boolean spacesValidation(String text) {
    return text != null && Const.onlySpaces(text) && StringUtils.isNotEmpty(text);
  }

  public boolean trailingSpacesValidation(String text) {
    return text != null && text.endsWith(" ");
  }

  /** Remove the selected entries from the allowed entries */
  protected void removeAllowedValue() {
    String[] selection = wAllowedValues.getSelection();
    for (String string : selection) {
      wAllowedValues.remove(string);
    }
  }

  /** Add one entry to the list of allowed values... */
  protected void addAllowedValue() {
    EnterStringDialog dialog =
        new EnterStringDialog(
            shell,
            "",
            BaseMessages.getString(PKG, "ValidatorDialog.Dialog.AddAllowedValue.Title"),
            BaseMessages.getString(PKG, "ValidatorDialog.Dialog.AddAllowedValue.Message"),
            true,
            variables);
    String value = dialog.open();
    if (StringUtils.isNotEmpty(value)) {
      wAllowedValues.add(value);
    }
  }

  private void getValidatorFieldData(Validation field) {

    wDescription.setText(Const.NVL(field.getName(), ""));
    wFieldName.setText(Const.NVL(field.getFieldName(), ""));

    wErrorCode.setText(Const.NVL(field.getErrorCode(), ""));
    wErrorDescription.setText(Const.NVL(field.getErrorDescription(), ""));

    wDataTypeVerified.setSelection(field.isDataTypeVerified());
    wDataType.setText(Const.NVL(field.getDataType(), ""));
    wConversionMask.setText(Const.NVL(field.getConversionMask(), ""));
    wDecimalSymbol.setText(Const.NVL(field.getDecimalSymbol(), ""));
    wGroupingSymbol.setText(Const.NVL(field.getGroupingSymbol(), ""));

    wNullAllowed.setSelection(field.isNullAllowed());
    wOnlyNullAllowed.setSelection(field.isOnlyNullAllowed());
    wOnlyNumeric.setSelection(field.isOnlyNumericAllowed());
    wMaxLength.setText(Const.NVL(field.getMaximumLength(), ""));
    wMinLength.setText(Const.NVL(field.getMinimumLength(), ""));
    wMaxValue.setText(Const.NVL(field.getMaximumValue(), ""));
    wMinValue.setText(Const.NVL(field.getMinimumValue(), ""));
    wStartStringExpected.setText(Const.NVL(field.getStartString(), ""));
    wEndStringExpected.setText(Const.NVL(field.getEndString(), ""));
    wStartStringDisallowed.setText(Const.NVL(field.getStartStringNotAllowed(), ""));
    wEndStringDisallowed.setText(Const.NVL(field.getEndStringNotAllowed(), ""));
    wRegExpExpected.setText(Const.NVL(field.getRegularExpression(), ""));
    wRegExpDisallowed.setText(Const.NVL(field.getRegularExpressionNotAllowed(), ""));

    wAllowedValues.removeAll();
    if (field.getAllowedValues() != null) {
      for (String allowedValue : field.getAllowedValues()) {
        wAllowedValues.add(Const.NVL(allowedValue, ""));
      }
    }

    wSourceValues.setSelection(field.isSourcingValues());
    wSourceTransform.setText(Const.NVL(field.getSourcingTransformName(), ""));
    wSourceField.setText(Const.NVL(field.getSourcingField(), ""));
  }

  private void enableFields() {
    boolean visible = selectedField != null;

    wgType.setVisible(visible);
    wgData.setVisible(visible);

    wlFieldName.setVisible(visible);
    wFieldName.setVisible(visible);
    wlDescription.setVisible(visible);
    wDescription.setVisible(visible);
    wlErrorCode.setVisible(visible);
    wErrorCode.setVisible(visible);
    wlErrorDescription.setVisible(visible);
    wErrorDescription.setVisible(visible);

    wlSourceTransform.setEnabled(wSourceValues.getSelection());
    wSourceTransform.setEnabled(wSourceValues.getSelection());
    wlSourceField.setEnabled(wSourceValues.getSelection());
    wSourceField.setEnabled(wSourceValues.getSelection());
    wlAllowedValues.setEnabled(!wSourceValues.getSelection());
    wAllowedValues.setEnabled(!wSourceValues.getSelection());
    wbAddAllowed.setEnabled(!wSourceValues.getSelection());
    wbRemoveAllowed.setEnabled(!wSourceValues.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // Populate the list of validations...
    //
    refreshValidationsList();
    enableFields();

    wValidateAll.setSelection(input.isValidatingAll());
    wConcatErrors.setSelection(input.isConcatenatingErrors());
    wConcatSeparator.setText(Const.NVL(input.getConcatenationSeparator(), ""));

    // Select the first available field...
    //
    if (!input.getValidations().isEmpty()) {
      Validation validatorField = input.getValidations().get(0);
      String description = validatorField.getName();
      int index = wValidationsList.indexOf(description);
      if (index >= 0) {
        wValidationsList.select(index);
        showSelectedValidatorField(description);
      }
    }

    setFlags();
  }

  private void refreshValidationsList() {
    wValidationsList.removeAll();
    for (Validation validation : selectionList) {
      wValidationsList.add(validation.getName());
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    saveChanges();
    input.setChanged();
    input.setValidatingAll(wValidateAll.getSelection());
    input.setConcatenatingErrors(wConcatErrors.getSelection());
    input.setConcatenationSeparator(wConcatSeparator.getText());

    input.setValidations(selectionList);

    input.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    dispose();
  }

  private void getTransforms() {
    Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
    shell.setCursor(busy);

    String fieldTransform = wSourceTransform.getText();

    wSourceTransform.removeAll();
    wSourceTransform.setItems(pipelineMeta.getPrevTransformNames(transformMeta));

    wSourceTransform.setText(fieldTransform);

    shell.setCursor(null);
    busy.dispose();
  }

  private void getFields() {
    Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
    shell.setCursor(busy);

    try {
      String sourceTransformName = wSourceTransform.getText();
      if (StringUtils.isNotEmpty(sourceTransformName)) {
        String fieldName = wSourceField.getText();
        IRowMeta r = pipelineMeta.getTransformFields(variables, sourceTransformName);
        if (r != null) {
          wSourceField.setItems(r.getFieldNames());
        }
        wSourceField.setText(fieldName);
      }
      shell.setCursor(null);
      busy.dispose();
    } catch (HopException ke) {
      shell.setCursor(null);
      busy.dispose();
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ValidatorDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "ValidatorDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /** Create a new validation rule page. */
  private void newValidation() {
    EnterStringDialog enterStringDialog =
        new EnterStringDialog(
            shell,
            "",
            BaseMessages.getString(PKG, "ValidatorDialog.EnterValidationRuleName.Title"),
            BaseMessages.getString(PKG, "ValidatorDialog.EnterValidationRuleName.Message"));
    String description = enterStringDialog.open();
    if (description != null) {
      if (Validation.findValidation(selectionList, description) != null) {
        MessageBox messageBox = new MessageBox(shell, SWT.ICON_ERROR);
        messageBox.setText(
            BaseMessages.getString(PKG, "ValidatorDialog.ValidationRuleNameAlreadyExists.Title"));
        messageBox.setMessage(
            BaseMessages.getString(PKG, "ValidatorDialog.ValidationRuleNameAlreadyExists.Message"));
        messageBox.open();
        return;
      }
      saveChanges();
      Validation validation = new Validation();
      validation.setName(description);
      selectionList.add(validation);
      selectedField = validation;
      refreshValidationsList();
      wValidationsList.select(selectionList.size() - 1);
      getValidatorFieldData(validation);

      showSelectedValidatorField(description);
    }
  }

  /** Clear the validation rules for a certain field. */
  private void clearValidation() {

    int index = wValidationsList.getSelectionIndex();
    if (index >= 0) {
      selectionList.remove(index);
      selectedField = null;
      wValidationsList.remove(index);
      enableFields();

      if (!selectionList.isEmpty()) {
        wValidationsList.select(selectionList.size() - 1);
      }
    }
  }
}
