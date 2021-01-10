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
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.List;

public class MergeBranchDialog extends Dialog {

  private static final Class<?> PKG = HopPerspectivePlugin.class; // For Translator
  private static final List<String> listMergeStrategy = new ArrayList<>();

  static {
    listMergeStrategy.add(MergeStrategy.RECURSIVE.getName());
    listMergeStrategy.add(MergeStrategy.OURS.getName());
    listMergeStrategy.add(MergeStrategy.THEIRS.getName());
  }

  private CCombo comboBranch;
  private String selectedBranch;
  private List<String> branches;
  private CCombo comboMergeStrategy;
  private String selectedMergeStrategy;

  public MergeBranchDialog(Shell parentShell) {
    super(parentShell);
  }

  @Override
  protected Control createDialogArea(Composite parent) {
    Composite comp = (Composite) super.createDialogArea(parent);

    GridLayout layout = (GridLayout) comp.getLayout();
    layout.numColumns = 2;

    Label branchLabel = new Label(comp, SWT.RIGHT);
    branchLabel.setText(BaseMessages.getString(PKG, "Git.Dialog.Branch.Merge.Message"));
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

    Label strategyLabel = new Label(comp, SWT.RIGHT);
    strategyLabel.setText(BaseMessages.getString(PKG, "Git.Dialog.Branch.Merge.MergeStrategy"));
    comboMergeStrategy = new CCombo(comp, SWT.DROP_DOWN);
    comboMergeStrategy.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    listMergeStrategy.forEach(mergeStrategy -> comboMergeStrategy.add(mergeStrategy));
    comboMergeStrategy.select(0);
    selectedMergeStrategy = listMergeStrategy.get(0);
    comboMergeStrategy.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            selectedMergeStrategy = ((CCombo) e.getSource()).getText();
          }
        });

    return comp;
  }

  public void setBranches(List<String> branches) {
    this.branches = branches;
  }

  public String getSelectedBranch() {
    return selectedBranch;
  }

  public String getSelectedMergeStrategy() {
    return selectedMergeStrategy;
  }
}
