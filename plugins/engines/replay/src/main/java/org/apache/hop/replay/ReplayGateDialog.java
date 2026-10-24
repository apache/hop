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

package org.apache.hop.replay;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Shell;

public class ReplayGateDialog extends Dialog {
  private static final Class<?> PKG = ReplayGateDialog.class;

  private final ReplayGate input;
  private final ReplayGate gate;
  private final IVariables variables;
  private final String title;

  private Shell shell;
  private GuiCompositeWidgets widgets;
  private boolean ok;

  public ReplayGateDialog(Shell parent, IVariables variables, ReplayGate gate, String title) {
    super(parent, SWT.NONE);
    this.input = gate;
    this.gate = new ReplayGate(gate);
    this.variables = variables;
    this.title = title;
    this.ok = false;
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageServer());
    shell.setText(title);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    // Buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Scrolled composite with all widgets from metadata
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, null, wOk, ReplayGate.GUI_PLUGIN_ELEMENT_PARENT_ID, gate);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return ok;
  }

  private void ok() {
    if (widgets != null) {
      widgets.getWidgetsContents(gate, ReplayGate.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
    input.setEnabled(gate.isEnabled());
    input.setSpoolDirectory(gate.getSpoolDirectory());
    input.setCompression(gate.getCompression());
    input.setRowLimit(gate.getRowLimit());
    input.setDescription(gate.getDescription());

    ok = true;
    dispose();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
