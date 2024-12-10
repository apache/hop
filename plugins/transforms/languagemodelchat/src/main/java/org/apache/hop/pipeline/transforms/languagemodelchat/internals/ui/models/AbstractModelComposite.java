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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;
import static org.apache.commons.lang3.math.NumberUtils.toDouble;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.FormDataBuilder.Builder.buildFormData;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;
import static org.apache.hop.ui.core.PropsUi.setLook;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.CHECK;
import static org.eclipse.swt.SWT.LEFT;
import static org.eclipse.swt.SWT.NONE;
import static org.eclipse.swt.SWT.READ_ONLY;
import static org.eclipse.swt.SWT.RIGHT;
import static org.eclipse.swt.SWT.SINGLE;

import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.FormDataBuilder;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

public abstract class AbstractModelComposite implements IModelComposite {

  protected final Composite composite;
  protected final LanguageModelChatMeta meta;

  protected Control control;
  protected final CompositeParameters parameters;
  protected final FormDataBuilder.Builder layout;
  protected final ModelType modelType;

  protected AbstractModelComposite(ModelType modelType, CompositeParameters parameters) {
    this.parameters = parameters;
    this.modelType = modelType;
    this.meta = (LanguageModelChatMeta) parameters.meta();
    this.composite = new Composite(parameters.parent(), NONE);
    this.composite.setVisible(false); // Disable composites by default, let the dialog control that.

    composite.setLayout(new FormLayout());
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(fd);
    setLook(composite);

    control = composite;
    layout = buildFormData().margin(parameters.margin());
  }

  protected Label createLabel() {
    return new Label(composite, RIGHT);
  }

  protected TextVar createTextVar() {
    return new TextVar(parameters.variables(), composite, SINGLE | LEFT | BORDER);
  }

  protected Button createButton() {
    return new Button(composite, CHECK);
  }

  protected CCombo createComboBox() {
    return new CCombo(composite, BORDER | READ_ONLY);
  }

  protected void prepare(String i18n, Label label, Control input) {
    notNull(input, "An input control must be provided");
    int middle = parameters.middlePct();
    int margin = parameters.margin();
    String i18nLabelKey =
        i18n("LanguageModelChatDialog." + modelType.name() + "." + i18n + ".Label");
    String i18nTooltipKey =
        i18n("LanguageModelChatDialog." + modelType.name() + "." + i18n + ".Tooltip");

    setLook(input);
    layout.control(control);
    input.setLayoutData(layout.left(middle, 0).right(100, 0).build());

    if (label == null) {
      isTrue(input instanceof Button, "Only buttons don't need a label");
      ((Button) input).setText(i18nLabelKey);
      input.setToolTipText(i18nTooltipKey);
    } else {
      label.setText(i18nLabelKey);
      label.setToolTipText(i18nTooltipKey);
      label.setLayoutData(layout.left(0, 0).right(middle, -margin).build());
    }

    control = input;
  }

  @Override
  public void updateLayouts() {
    composite().layout(true, true);
  }

  @Override
  public Composite composite() {
    return composite;
  }

  @Override
  public Control control() {
    return control;
  }

  public ModelType modelType() {
    return modelType;
  }

  @Override
  public boolean isSelectedModelType() {
    return modelType.code().equals(meta.getModelType());
  }

  @Override
  public boolean validateInputs() {
    // If not selected modelType, always validate
    return !isSelectedModelType();
  }

  String trimStringToNull(String input) {
    return trimToNull(input);
  }

  Double trimDoubleToNull(String input) {
    String v = trimToNull(input);
    return isBlank(v) || !isCreatable(v) ? null : toDouble(v);
  }

  Integer trimIntegerToNull(String input) {
    String v = trimToNull(input);
    return isBlank(v) || !isCreatable(v) ? null : toInt(v);
  }
}
