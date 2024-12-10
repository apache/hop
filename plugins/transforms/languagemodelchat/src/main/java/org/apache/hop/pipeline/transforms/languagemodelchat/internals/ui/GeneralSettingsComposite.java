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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.modelTypeDescriptions;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.typeFromDescription;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.FormDataBuilder.Builder.buildFormData;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;
import static org.apache.hop.ui.core.PropsUi.setLook;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.CHECK;
import static org.eclipse.swt.SWT.CURSOR_WAIT;
import static org.eclipse.swt.SWT.LEFT;
import static org.eclipse.swt.SWT.NONE;
import static org.eclipse.swt.SWT.READ_ONLY;
import static org.eclipse.swt.SWT.RIGHT;
import static org.eclipse.swt.SWT.SINGLE;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.FormDataBuilder.Builder;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

public class GeneralSettingsComposite implements IDialogComposite {

  private final Composite composite;
  private final LanguageModelChatMeta meta;
  private final Label inputFieldLabel;
  private final CCombo inputFieldInput;
  private final Label outputFieldNamePrefixLabel;
  private final TextVar outputFieldNamePrefixInput;
  private final Label identifierLabel;
  private final TextVar identifierInput;
  private final Button inputChatJsonInput;
  private final Button outputChatJsonInput;
  private final Button mockInput;
  private final Label mockOutputValueLabel;
  private final TextVar mockOutputValueInput;
  private final Label parallelismLabel;
  private final TextVar parallelismInput;
  private final Label modelTypeLabel;
  private final CCombo modelTypeInput;
  private final Control control;
  private final CompositeParameters parameters;

  private boolean gotPreviousFields = false;

  public GeneralSettingsComposite(CompositeParameters parameters) {
    this.parameters = parameters;
    this.meta = (LanguageModelChatMeta) parameters.meta();
    this.composite = new Composite(parameters.parent(), NONE);

    int middle = parameters.middlePct();
    int margin = parameters.margin();

    Builder layout = buildFormData().margin(margin);

    // Input Type JSON Checkbox
    inputChatJsonInput = new Button(parameters.parent(), CHECK);
    setLook(inputChatJsonInput);
    layout.control(composite);
    inputChatJsonInput.setText(i18n("LanguageModelChatDialog.InputChatJson.Label"));
    inputChatJsonInput.setToolTipText(i18n("LanguageModelChatDialog.InputChatJson.Tooltip"));
    inputChatJsonInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Output Type JSON Checkbox
    outputChatJsonInput = new Button(parameters.parent(), CHECK);
    setLook(outputChatJsonInput);
    layout.control(inputChatJsonInput);
    outputChatJsonInput.setText(i18n("LanguageModelChatDialog.OutputChatJson.Label"));
    outputChatJsonInput.setToolTipText(i18n("LanguageModelChatDialog.OutputChatJson.Tooltip"));
    outputChatJsonInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Input field
    inputFieldLabel = new Label(parameters.parent(), RIGHT);
    inputFieldLabel.setText(i18n("LanguageModelChatDialog.InputFieldName.Label"));
    inputFieldLabel.setToolTipText(i18n("LanguageModelChatDialog.InputFieldName.Tooltip"));
    setLook(inputFieldLabel);
    layout.control(outputChatJsonInput);
    inputFieldLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    inputFieldInput = new CCombo(parameters.parent(), BORDER | READ_ONLY);
    setLook(inputFieldInput);
    inputFieldInput.addModifyListener(e -> meta.setChanged());
    inputFieldInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());
    inputFieldInput.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(parameters.shell().getDisplay(), CURSOR_WAIT);
            parameters.shell().setCursor(busy);
            get();
            parameters.shell().setCursor(null);
            busy.dispose();
          }
        });

    // Output Field prefix
    outputFieldNamePrefixLabel = new Label(parameters.parent(), RIGHT);
    outputFieldNamePrefixInput =
        new TextVar(parameters.variables(), parameters.parent(), SINGLE | LEFT | BORDER);
    setLook(outputFieldNamePrefixLabel);
    setLook(outputFieldNamePrefixInput);
    layout.control(inputFieldInput);
    outputFieldNamePrefixLabel.setText(
        i18n("LanguageModelChatDialog.OutputFieldNamePrefixName.Label"));
    outputFieldNamePrefixLabel.setToolTipText(
        i18n("LanguageModelChatDialog.OutputFieldNamePrefixName.Tooltip"));
    outputFieldNamePrefixLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    outputFieldNamePrefixInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Identifier
    identifierLabel = new Label(parameters.parent(), RIGHT);
    identifierInput =
        new TextVar(parameters.variables(), parameters.parent(), SINGLE | LEFT | BORDER);
    setLook(identifierLabel);
    setLook(identifierInput);
    layout.control(outputFieldNamePrefixInput);
    identifierLabel.setText(i18n("LanguageModelChatDialog.Identifier.Label"));
    identifierLabel.setToolTipText(i18n("LanguageModelChatDialog.Identifier.Tooltip"));
    identifierLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    identifierInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Parallelism
    parallelismLabel = new Label(parameters.parent(), RIGHT);
    parallelismInput =
        new TextVar(parameters.variables(), parameters.parent(), SINGLE | LEFT | BORDER);
    setLook(parallelismLabel);
    setLook(parallelismInput);
    layout.control(identifierInput);
    parallelismLabel.setText(i18n("LanguageModelChatDialog.Parallelism.Label"));
    parallelismLabel.setToolTipText(i18n("LanguageModelChatDialog.Parallelism.Tooltip"));
    parallelismLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    parallelismInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Mock Checkbox
    mockInput = new Button(parameters.parent(), CHECK);
    setLook(mockInput);
    layout.control(parallelismInput);
    mockInput.setText(i18n("LanguageModelChatDialog.Mock.Label"));
    mockInput.setToolTipText(i18n("LanguageModelChatDialog.Mock.Tooltip"));
    mockInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Mock output value
    mockOutputValueLabel = new Label(parameters.parent(), RIGHT);
    mockOutputValueInput =
        new TextVar(parameters.variables(), parameters.parent(), SINGLE | LEFT | BORDER);
    setLook(mockOutputValueLabel);
    setLook(mockOutputValueInput);
    layout.control(mockInput);
    mockOutputValueLabel.setText(i18n("LanguageModelChatDialog.MockOutputValue.Label"));
    mockOutputValueLabel.setToolTipText(i18n("LanguageModelChatDialog.MockOutputValue.Tooltip"));
    mockOutputValueLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    mockOutputValueInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    // Model Type
    modelTypeLabel = new Label(parameters.parent(), RIGHT);
    modelTypeInput = new CCombo(parameters.parent(), BORDER | READ_ONLY);
    setLook(modelTypeLabel);
    setLook(modelTypeInput);
    layout.control(mockOutputValueInput);
    modelTypeLabel.setText(i18n("LanguageModelChatDialog.ModelType.Label"));
    modelTypeLabel.setToolTipText(i18n("LanguageModelChatDialog.ModelType.Tooltip"));
    modelTypeLabel.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    modelTypeInput.setItems(modelTypeDescriptions());
    modelTypeInput.addModifyListener(e -> meta.setChanged());
    modelTypeInput.setLayoutData(layout.left(middle, 0).right(100, 0).build());
    modelTypeInput.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            String modelType = typeFromDescription(modelTypeInput.getText()).code();
            ((LanguageModelChatMeta) parameters.meta()).setModelType(modelType);
            parameters.getPopulateInputsAdapter().populateInputs();
          }
        });

    control = modelTypeInput;
  }

  @Override
  public void updateLayouts() {
    composite().layout(true, true);
  }

  @Override
  public boolean validateInputs() {

    if (isBlank(inputFieldInput.getText())) {
      return false;
    }

    if (isBlank(modelTypeInput.getText())) {
      return false;
    }

    String parallelism = trim(parallelismInput.getText());
    if (!(isBlank(parallelism) || isCreatable(parallelism))) {
      return false;
    }

    // TODO Implement

    return true;
  }

  @Override
  public Control control() {
    return control;
  }

  @Override
  public void updateTransformMeta() {
    if (!validateInputs()) {
      return;
    }
    meta.setInputField(inputFieldInput.getText());
    meta.setInputChatJson(inputChatJsonInput.getSelection());
    meta.setOutputChatJson(outputChatJsonInput.getSelection());
    meta.setMock(mockInput.getSelection());
    meta.setMockOutputValue(mockOutputValueInput.getText());
    meta.setOutputFieldNamePrefix(outputFieldNamePrefixInput.getText());
    meta.setIdentifierValue(identifierInput.getText());
    meta.setModelType(typeFromDescription(modelTypeInput.getText()).code());
    meta.setParallelism(toInt(trim(parallelismInput.getText()), 1));
  }

  @Override
  public void populateInputs() {
    inputFieldInput.setText(meta.getInputField() != null ? meta.getInputField() : "");
    inputChatJsonInput.setSelection(meta.isInputChatJson());
    outputChatJsonInput.setSelection(meta.isOutputChatJson());
    mockInput.setSelection(meta.isMock());
    mockOutputValueInput.setText(
        meta.getMockOutputValue() != null ? meta.getMockOutputValue() : "");
    outputFieldNamePrefixInput.setText(
        meta.getOutputFieldNamePrefix() != null ? meta.getOutputFieldNamePrefix() : "");
    identifierInput.setText(meta.getIdentifierValue() != null ? meta.getIdentifierValue() : "");

    String modelType = meta.getModelType() != null ? meta.getModelType() : "";
    modelTypeInput.setText(ModelType.valueOf(modelType).description());
    parallelismInput.setText(String.valueOf(meta.getParallelism()));
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String inputField = null;
        if (inputFieldInput.getText() != null) {
          inputField = inputFieldInput.getText();
        }
        inputFieldInput.removeAll();

        IRowMeta r =
            parameters
                .pipelineMeta()
                .getPrevTransformFields(parameters.variables(), parameters.transformName());
        if (r != null) {
          inputFieldInput.setItems(r.getFieldNames());
        }
        if (inputField != null) {
          inputFieldInput.setText(inputField);
        }
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            parameters.shell(),
            i18n("LanguageModelChatDialog.FailedToGetFields.DialogTitle"),
            i18n("LanguageModelChatDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }

  @Override
  public Composite composite() {
    return composite;
  }

  public Composite getComposite() {
    return composite;
  }

  public Label getInputFieldLabel() {
    return inputFieldLabel;
  }

  public CCombo getInputFieldInput() {
    return inputFieldInput;
  }

  public Label getOutputFieldNamePrefixLabel() {
    return outputFieldNamePrefixLabel;
  }

  public TextVar getOutputFieldNamePrefixInput() {
    return outputFieldNamePrefixInput;
  }

  public Label getIdentifierLabel() {
    return identifierLabel;
  }

  public TextVar getIdentifierInput() {
    return identifierInput;
  }

  public Button getInputChatJsonInput() {
    return inputChatJsonInput;
  }

  public Button getOutputChatJsonInput() {
    return outputChatJsonInput;
  }

  public Button getMockInput() {
    return mockInput;
  }

  public Label getMockOutputValueLabel() {
    return mockOutputValueLabel;
  }

  public TextVar getMockOutputValueInput() {
    return mockOutputValueInput;
  }

  public Label getParallelismLabel() {
    return parallelismLabel;
  }

  public TextVar getParallelismInput() {
    return parallelismInput;
  }

  public Label getModelTypeLabel() {
    return modelTypeLabel;
  }

  public CCombo getModelTypeInput() {
    return modelTypeInput;
  }
}
