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

package org.apache.hop.pipeline.transforms.jms.shared;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

/** Widget plumbing shared by the JMS consumer and producer dialogs. */
public final class JmsDialogHelper {

  private JmsDialogHelper() {}

  /** Names of the JMS connections in the metadata, for a connection combo. */
  public static String[] listConnectionNames(IHopMetadataProvider metadataProvider) {
    try {
      List<String> names = metadataProvider.getSerializer(JmsConnection.class).listObjectNames();
      return names.toArray(new String[0]);
    } catch (HopException e) {
      // A dialog must still open when the metadata cannot be read; the combo is simply empty.
      return new String[0];
    }
  }

  public static CCombo labeledCombo(
      Composite shell,
      Class<?> pkg,
      IVariables variables,
      int middle,
      int margin,
      Control last,
      String labelKey,
      boolean readOnly) {
    Label label = label(shell, pkg, middle, margin, last, labelKey);
    CCombo combo = new CCombo(shell, SWT.BORDER | (readOnly ? SWT.READ_ONLY : SWT.NONE));
    PropsUi.setLook(combo);
    combo.setLayoutData(rightOf(label, middle));
    applyTooltip(pkg, labelKey, label, combo);
    return combo;
  }

  public static TextVar labeledText(
      Composite shell,
      Class<?> pkg,
      IVariables variables,
      int middle,
      int margin,
      Control last,
      String labelKey) {
    Label label = label(shell, pkg, middle, margin, last, labelKey);
    TextVar text = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setLayoutData(rightOf(label, middle));
    applyTooltip(pkg, labelKey, label, text);
    return text;
  }

  public static Button labeledCheckbox(
      Composite shell, Class<?> pkg, int middle, int margin, Control last, String labelKey) {
    Label label = label(shell, pkg, middle, margin, last, labelKey);
    Button button = new Button(shell, SWT.CHECK);
    PropsUi.setLook(button);
    button.setLayoutData(rightOf(label, middle));
    applyTooltip(pkg, labelKey, label, button);
    return button;
  }

  private static Label label(
      Composite shell, Class<?> pkg, int middle, int margin, Control last, String labelKey) {
    Label label = new Label(shell, SWT.RIGHT);
    label.setText(BaseMessages.getString(pkg, labelKey));
    PropsUi.setLook(label);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(last, margin);
    fdLabel.right = new FormAttachment(middle, -margin);
    label.setLayoutData(fdLabel);
    return label;
  }

  private static FormData rightOf(Label label, int middle) {
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    return fd;
  }

  private static void applyTooltip(Class<?> pkg, String labelKey, Label label, Control control) {
    String tooltip = BaseMessages.getString(pkg, labelKey + ".Tooltip");
    if (tooltip != null && !tooltip.startsWith("!")) {
      label.setToolTipText(tooltip);
      control.setToolTipText(tooltip);
    }
  }
}
