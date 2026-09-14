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

package org.apache.hop.lint;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Asks what to accept on a single transform or action, and why.
 *
 * <p>The reason is required rather than optional. A finding that simply vanished would leave the
 * next person with the same question the reporter of this feature had — is this checked and fine,
 * or did somebody switch the check off? — so the answer is written down where it can be reviewed.
 *
 * <p>The answer goes to the project's hop-lint.yml, never into the pipeline or workflow: those
 * files are opened by people who do not have this plugin installed, and lint bookkeeping in them
 * would be at best noise and at worst something another tool trips over.
 */
public class LintSuppressDialog extends Dialog {

  private static final Class<?> PKG = LintSuppressDialog.class; // for i18n purposes

  /** What the user chose: which rules to accept on this element, and why. */
  public record Suppression(Set<String> ruleIds, String reason) {}

  private final Shell parent;
  private final String elementName;
  private final List<LintResult> findings;

  private Shell shell;
  private Button allRulesButton;
  private Button listedRulesButton;
  private Text reasonText;
  private Suppression suppression;

  /**
   * @param elementName the transform or action the findings sit on
   * @param findings what is currently reported on it
   */
  public LintSuppressDialog(Shell parent, String elementName, List<LintResult> findings) {
    super(parent, SWT.NONE);
    this.parent = parent;
    this.elementName = elementName;
    this.findings = findings == null ? List.of() : findings;
  }

  /** Returns what to accept, or null when the user cancelled. */
  public Suppression open() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setText(BaseMessages.getString(PKG, "LintSuppressDialog.Shell.Title"));
    shell.setLayout(new FormLayout());

    createContents();

    shell.setSize(560, 420);
    shell.setLocation(
        parent.getLocation().x + (parent.getSize().x - 560) / 2,
        parent.getLocation().y + (parent.getSize().y - 420) / 2);
    shell.open();

    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return suppression;
  }

  private void createContents() {
    int margin = 10;

    Label header = new Label(shell, SWT.LEFT);
    header.setText(BaseMessages.getString(PKG, "LintSuppressDialog.Label.Element", elementName));
    FormData headerData = new FormData();
    headerData.left = new FormAttachment(0, margin);
    headerData.right = new FormAttachment(100, -margin);
    headerData.top = new FormAttachment(0, margin);
    header.setLayoutData(headerData);

    Label findingsLabel = new Label(shell, SWT.LEFT);
    findingsLabel.setText(BaseMessages.getString(PKG, "LintSuppressDialog.Label.Findings"));
    FormData findingsLabelData = new FormData();
    findingsLabelData.left = new FormAttachment(0, margin);
    findingsLabelData.top = new FormAttachment(header, margin);
    findingsLabel.setLayoutData(findingsLabelData);

    Text findingsText =
        new Text(shell, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP | SWT.READ_ONLY);
    findingsText.setText(describeFindings());
    FormData findingsData = new FormData();
    findingsData.left = new FormAttachment(0, margin);
    findingsData.right = new FormAttachment(100, -margin);
    findingsData.top = new FormAttachment(findingsLabel, margin / 2);
    findingsData.height = 90;
    findingsText.setLayoutData(findingsData);

    allRulesButton = new Button(shell, SWT.RADIO);
    allRulesButton.setText(BaseMessages.getString(PKG, "LintSuppressDialog.Scope.AllRules"));
    allRulesButton.setToolTipText(
        BaseMessages.getString(PKG, "LintSuppressDialog.Scope.AllRules.ToolTip"));
    FormData allRulesData = new FormData();
    allRulesData.left = new FormAttachment(0, margin);
    allRulesData.right = new FormAttachment(100, -margin);
    allRulesData.top = new FormAttachment(findingsText, margin);
    allRulesButton.setLayoutData(allRulesData);

    listedRulesButton = new Button(shell, SWT.RADIO);
    listedRulesButton.setText(
        BaseMessages.getString(PKG, "LintSuppressDialog.Scope.ListedRules", ruleList()));
    listedRulesButton.setToolTipText(
        BaseMessages.getString(PKG, "LintSuppressDialog.Scope.ListedRules.ToolTip"));
    listedRulesButton.setEnabled(!ruleIdsOfFindings().isEmpty());
    FormData listedRulesData = new FormData();
    listedRulesData.left = new FormAttachment(0, margin);
    listedRulesData.right = new FormAttachment(100, -margin);
    listedRulesData.top = new FormAttachment(allRulesButton, margin / 2);
    listedRulesButton.setLayoutData(listedRulesData);

    // Accepting everything on the element is the metadata injection case, which is the reason
    // this dialog exists; naming the rules is the careful option for anyone who wants it.
    allRulesButton.setSelection(true);

    Label reasonLabel = new Label(shell, SWT.LEFT);
    reasonLabel.setText(BaseMessages.getString(PKG, "LintSuppressDialog.Label.Reason"));
    FormData reasonLabelData = new FormData();
    reasonLabelData.left = new FormAttachment(0, margin);
    reasonLabelData.top = new FormAttachment(listedRulesButton, margin);
    reasonLabel.setLayoutData(reasonLabelData);

    Button okButton = new Button(shell, SWT.PUSH);
    okButton.setText(BaseMessages.getString("System.Button.OK"));
    Button cancelButton = new Button(shell, SWT.PUSH);
    cancelButton.setText(BaseMessages.getString("System.Button.Cancel"));

    FormData cancelData = new FormData();
    cancelData.right = new FormAttachment(100, -margin);
    cancelData.bottom = new FormAttachment(100, -margin);
    cancelButton.setLayoutData(cancelData);

    FormData okData = new FormData();
    okData.right = new FormAttachment(cancelButton, -margin);
    okData.bottom = new FormAttachment(100, -margin);
    okButton.setLayoutData(okData);

    reasonText = new Text(shell, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.WRAP);
    FormData reasonData = new FormData();
    reasonData.left = new FormAttachment(0, margin);
    reasonData.right = new FormAttachment(100, -margin);
    reasonData.top = new FormAttachment(reasonLabel, margin / 2);
    reasonData.bottom = new FormAttachment(okButton, -margin);
    reasonText.setLayoutData(reasonData);

    okButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            ok();
          }
        });
    cancelButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            shell.dispose();
          }
        });

    shell.setDefaultButton(okButton);
    reasonText.setFocus();
  }

  private void ok() {
    String reason = reasonText.getText().trim();
    if (Utils.isEmpty(reason)) {
      MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "LintSuppressDialog.ReasonRequired.Title"));
      box.setMessage(BaseMessages.getString(PKG, "LintSuppressDialog.ReasonRequired.Message"));
      box.open();
      reasonText.setFocus();
      return;
    }

    Set<String> ruleIds =
        allRulesButton.getSelection() ? Set.of(LintPolicy.ALL_RULES) : ruleIdsOfFindings();
    if (ruleIds.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.ICON_WARNING | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "LintSuppressDialog.NothingToName.Title"));
      box.setMessage(BaseMessages.getString(PKG, "LintSuppressDialog.NothingToName.Message"));
      box.open();
      return;
    }

    suppression = new Suppression(ruleIds, reason);
    shell.dispose();
  }

  private Set<String> ruleIdsOfFindings() {
    Set<String> ruleIds = new LinkedHashSet<>();
    for (LintResult finding : findings) {
      if (!Utils.isEmpty(finding.getRuleId())) {
        ruleIds.add(finding.getRuleId());
      }
    }
    return ruleIds;
  }

  private String ruleList() {
    Set<String> ruleIds = ruleIdsOfFindings();
    return ruleIds.isEmpty() ? "-" : String.join(", ", ruleIds);
  }

  private String describeFindings() {
    if (findings.isEmpty()) {
      return BaseMessages.getString(PKG, "LintSuppressDialog.NoFindings");
    }
    StringBuilder text = new StringBuilder();
    for (LintResult finding : findings) {
      text.append("[")
          .append(finding.getSeverity())
          .append("] ")
          .append(finding.getRuleId())
          .append(": ")
          .append(finding.getMessage())
          .append(Const.CR);
    }
    return text.toString();
  }
}
