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

package org.apache.hop.annotation;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Shell;

/** Edits the text and placement of one annotation. */
public class AnnotationDialog extends Dialog {
  private static final Class<?> PKG = Annotation.class;

  private final IVariables variables;
  private final Annotation annotation;
  private Shell shell;
  private GuiCompositeWidgets widgets;
  private boolean accepted;

  public AnnotationDialog(Shell parent, IVariables variables, Annotation annotation) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.annotation = annotation;
  }

  /**
   * Opens the dialog.
   *
   * @return true when the user confirms the edit
   */
  public boolean open() {
    Shell parent = getParent();
    shell =
        new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL);
    shell.setText(BaseMessages.getString(PKG, "AnnotationDialog.Title"));
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImage("ui/images/note.svg"));
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(layout);

    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString("System.Button.OK"));
    ok.addListener(SWT.Selection, event -> accept());

    Button cancel = new Button(shell, SWT.PUSH);
    cancel.setText(BaseMessages.getString("System.Button.Cancel"));
    cancel.addListener(SWT.Selection, event -> discard());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {ok, cancel}, PropsUi.getMargin(), null);
    shell.setDefaultButton(ok);

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, null, ok, Annotation.GUI_PLUGIN_ELEMENT_PARENT_ID, annotation);

    BaseDialog.defaultShellHandling(shell, event -> accept(), event -> discard(), false);
    return accepted;
  }

  private void accept() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    widgets.getWidgetsContents(annotation, Annotation.GUI_PLUGIN_ELEMENT_PARENT_ID);
    accepted = true;
    shell.dispose();
  }

  private void discard() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    accepted = false;
    shell.dispose();
  }
}
