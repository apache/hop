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

import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;
import static org.apache.hop.ui.core.PropsUi.setLook;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.CENTER;
import static org.eclipse.swt.SWT.HORIZONTAL;
import static org.eclipse.swt.SWT.LEFT;
import static org.eclipse.swt.SWT.RIGHT;
import static org.eclipse.swt.SWT.SEPARATOR;
import static org.eclipse.swt.SWT.SINGLE;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class TransformNameComposite implements IDialogComposite {
  private final Label label;
  private final Text inputField;
  private final ITransformMeta meta;
  private final Control control;
  private final CompositeParameters parameters;

  public TransformNameComposite(CompositeParameters parameters) {
    this.parameters = parameters;
    this.meta = parameters.meta();

    // Transform Name label
    this.label = new Label(parameters.shell(), RIGHT);
    this.inputField = new Text(parameters.shell(), SINGLE | LEFT | BORDER);

    setLook(label);
    setLook(inputField);

    label.setText(i18n("System.Label.TransformName"));
    label.setToolTipText(i18n("System.TransformName.Tooltip"));
    inputField.setText(parameters.transformName());

    inputField.addModifyListener(e -> meta.setChanged());

    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(parameters.middlePct(), -parameters.margin());
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(label, parameters.margin());
    fdTransformName.top = new FormAttachment(label, 0, CENTER);
    fdTransformName.right = new FormAttachment(100, 0);

    // Draw line separator
    Label separator = new Label(parameters.shell(), HORIZONTAL | SEPARATOR);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.top = new FormAttachment(inputField, parameters.margin());
    fdSeparator.right = new FormAttachment(100, 0);
    separator.setLayoutData(fdSeparator);

    label.setLayoutData(fdlTransformName);
    inputField.setLayoutData(fdTransformName);

    this.control = inputField;
  }

  public Label getLabel() {
    return label;
  }

  public Text getInputField() {
    return inputField;
  }

  @Override
  public boolean validateInputs() {
    return true; // TODO implement
  }

  @Override
  public Control control() {
    return control;
  }

  @Override
  public void populateInputs() {

    inputField.selectAll();
    inputField.setFocus();
  }

  @Override
  public boolean ok() {
    if (StringUtils.isBlank(inputField.getText())) {
      return false;
    }

    parameters.dialog().setTransformName(inputField.getText());
    return true;
  }
}
