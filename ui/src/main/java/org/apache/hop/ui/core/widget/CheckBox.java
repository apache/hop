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

package org.apache.hop.ui.core.widget;

import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.WidgetUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

public class CheckBox extends Composite {
  private Button button;
  private Label label;

  public CheckBox(Composite composite) {
    super(composite, SWT.NONE);
    WidgetUtils.setFormLayout(this, 0);

    button = new Button(this, SWT.CHECK);
    button.setLayoutData(new FormDataBuilder().left().result());

    label = new Label(this, SWT.LEFT);
    label.setLayoutData(new FormDataBuilder().left(button, ConstUi.SMALL_MARGIN).result());
  }

  public void setText(String text) {
    label.setText(text);
  }

  public Button getButton() {
    return button;
  }

  public Label getLabel() {
    return label;
  }
}
