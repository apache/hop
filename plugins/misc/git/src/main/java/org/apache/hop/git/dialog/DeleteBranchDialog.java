/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.dialog;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.util.List;

public class DeleteBranchDialog extends Dialog {

  private static final Class<?> PKG = HopPerspectivePlugin.class; // For Translator

  private CCombo comboBranch;
  private String selectedBranch;
  private boolean isForce;
  private List<String> branches;

  public DeleteBranchDialog(Shell parentShell) {
    super(parentShell);
  }

  @Override
  protected Control createDialogArea(Composite parent) {
    Composite comp = (Composite) super.createDialogArea(parent);

    GridLayout layout = (GridLayout) comp.getLayout();
    layout.numColumns = 2;

    Label branchLabel = new Label(comp, SWT.RIGHT);
    branchLabel.setText(BaseMessages.getString(PKG, "Git.Dialog.Branch.Delete.Message"));
    comboBranch = new CCombo(comp, SWT.DROP_DOWN);
    comboBranch.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    comboBranch.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            selectedBranch = ((CCombo) e.getSource()).getText();
          }
        });
    branches.forEach(branch -> comboBranch.add(branch));

    Label forceLabel = new Label(comp, SWT.RIGHT);
    forceLabel.setText(BaseMessages.getString(PKG, "Git.Dialog.Force"));
    Button forceButton = new Button(comp, SWT.CHECK);
    forceButton.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    forceButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            isForce = ((Button) e.getSource()).getSelection();
          }
        });
    forceButton.setSelection(false);

    return comp;
  }

  public void setBranches(List<String> branches) {
    this.branches = branches;
  }

  public String getSelectedBranch() {
    return selectedBranch;
  }

  public boolean isForce() {
    return isForce;
  }
}
