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
package org.apache.hop.lint;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lint.registry.ProjectLintYamlExporter;
import org.apache.hop.lint.registry.RulePackIds;
import org.apache.hop.lint.registry.RulePackOwner;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;

/** Dialog for viewing pack rules and managing project-local lint rules. */
public class RuleManagerDialog extends Dialog {

  private static final Class<?> PKG = RuleManagerDialog.class; // for i18n purposes

  /** Fixed width of the button column, so extra dialog width goes to the table. */
  private static final int BUTTON_PANEL_WIDTH = 200;

  private static final ILogChannel log = LogChannel.GENERAL;

  private Shell shell;
  private Shell parent;
  private List<CustomLintRule> rules;
  private Table rulesTable;
  private Button editButton;
  private Button deleteButton;
  private Button toggleButton;

  /** The column the table is sorted on, or -1 for the order the rules were loaded in. */
  private int sortColumn = -1;

  private boolean sortAscending = true;

  /** The ids the packs define, looked up once rather than on every selection. */
  private Set<String> packRuleIds;

  public RuleManagerDialog(Shell parent, List<CustomLintRule> rules) {
    super(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    this.parent = parent;
    this.rules = rules;
    setText("Lint Rules Manager");
  }

  public void open() {
    createShell();
    createContents();
    populateTable();
    updateButtonState();

    // Wide enough that every column, Severity and Enabled included, is visible without scrolling
    // at the default size. The table scrolls horizontally if the user makes it narrower.
    shell.setSize(1220, 620);
    shell.setLocation(
        parent.getLocation().x + (parent.getSize().x - 1220) / 2,
        parent.getLocation().y + (parent.getSize().y - 620) / 2);
    shell.open();

    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private void createShell() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setText(getText());
    shell.setLayout(new FormLayout());
  }

  private void createContents() {
    int margin = 10;

    Label titleLabel = new Label(shell, SWT.NONE);
    titleLabel.setText(
        BaseMessages.getString(
            PKG, "RuleManagerDialog.Title.EffectiveRules", String.valueOf(rules.size())));
    titleLabel.setFont(
        new org.eclipse.swt.graphics.Font(shell.getDisplay(), "Arial", 12, SWT.BOLD));

    FormData titleData = new FormData();
    titleData.left = new FormAttachment(0, margin);
    titleData.top = new FormAttachment(0, margin);
    titleLabel.setLayoutData(titleData);

    Label hintLabel = new Label(shell, SWT.WRAP);
    hintLabel.setText(
        "Editing a pack rule writes the change to your project's hop-lint.yml; the pack itself is "
            + "never modified. Pack rules cannot be deleted, only switched off with Toggle Enabled. "
            + "Add and delete apply to project rules only.");
    FormData hintData = new FormData();
    hintData.left = new FormAttachment(0, margin);
    hintData.right = new FormAttachment(100, -margin);
    hintData.top = new FormAttachment(titleLabel, margin);
    hintLabel.setLayoutData(hintData);

    // H_SCROLL so the Severity and Enabled columns stay reachable when the dialog is narrower than
    // the sum of the column widths, rather than being clipped away with no way to get at them.
    rulesTable =
        new Table(
            shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    rulesTable.setHeaderVisible(true);
    rulesTable.setLinesVisible(true);
    rulesTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateButtonState();
          }
        });

    // Findings, hop-lint.yml and the CLI all name a rule by its id, and two rules can share the
    // start of a long name, so the id comes first.
    TableColumn idColumn = new TableColumn(rulesTable, SWT.LEFT);
    idColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.RuleId"));
    idColumn.setWidth(110);

    TableColumn nameColumn = new TableColumn(rulesTable, SWT.LEFT);
    nameColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.RuleName"));
    nameColumn.setWidth(260);

    TableColumn sourceColumn = new TableColumn(rulesTable, SWT.LEFT);
    sourceColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Source"));
    sourceColumn.setWidth(90);

    TableColumn targetColumn = new TableColumn(rulesTable, SWT.LEFT);
    targetColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Target"));
    targetColumn.setWidth(110);

    TableColumn fieldColumn = new TableColumn(rulesTable, SWT.LEFT);
    fieldColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Field"));
    fieldColumn.setWidth(130);

    TableColumn conditionColumn = new TableColumn(rulesTable, SWT.LEFT);
    conditionColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Condition"));
    conditionColumn.setWidth(150);

    TableColumn valueColumn = new TableColumn(rulesTable, SWT.LEFT);
    valueColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Value"));
    valueColumn.setWidth(100);

    TableColumn severityColumn = new TableColumn(rulesTable, SWT.CENTER);
    severityColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Severity"));
    severityColumn.setWidth(80);

    TableColumn enabledColumn = new TableColumn(rulesTable, SWT.CENTER);
    enabledColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Enabled"));
    enabledColumn.setWidth(80);

    TableColumn tagsColumn = new TableColumn(rulesTable, SWT.LEFT);
    tagsColumn.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Column.Tags"));
    tagsColumn.setWidth(200);

    TableColumn[] columns = rulesTable.getColumns();
    for (int i = 0; i < columns.length; i++) {
      int index = i;
      columns[i].addListener(SWT.Selection, e -> sortOn(index));
    }

    FormData tableData = new FormData();
    tableData.left = new FormAttachment(0, margin);
    tableData.right = new FormAttachment(100, -(BUTTON_PANEL_WIDTH + 2 * margin));
    tableData.top = new FormAttachment(hintLabel, margin);
    tableData.bottom = new FormAttachment(100, -60);
    rulesTable.setLayoutData(tableData);

    Composite buttonPanel = new Composite(shell, SWT.NONE);
    buttonPanel.setLayout(new FormLayout());

    FormData buttonPanelData = new FormData();
    // A fixed column, so widening the dialog gives the extra room to the table rather than to the
    // buttons.
    buttonPanelData.left = new FormAttachment(100, -(BUTTON_PANEL_WIDTH + margin));
    buttonPanelData.right = new FormAttachment(100, -margin);
    buttonPanelData.top = new FormAttachment(hintLabel, margin);
    buttonPanelData.bottom = new FormAttachment(100, -60);
    buttonPanel.setLayoutData(buttonPanelData);

    Button addButton = new Button(buttonPanel, SWT.PUSH);
    addButton.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Button.AddProjectRule"));
    addButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            addRule();
          }
        });

    FormData addData = new FormData();
    addData.left = new FormAttachment(0, margin);
    addData.right = new FormAttachment(100, -margin);
    addData.top = new FormAttachment(0, 0);
    addData.height = 30;
    addButton.setLayoutData(addData);

    editButton = new Button(buttonPanel, SWT.PUSH);
    editButton.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Button.EditRule"));
    editButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            editRule();
          }
        });

    FormData editData = new FormData();
    editData.left = new FormAttachment(0, margin);
    editData.right = new FormAttachment(100, -margin);
    editData.top = new FormAttachment(addButton, 10);
    editData.height = 30;
    editButton.setLayoutData(editData);

    deleteButton = new Button(buttonPanel, SWT.PUSH);
    deleteButton.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Button.DeleteRule"));
    deleteButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            deleteRule();
          }
        });

    FormData deleteData = new FormData();
    deleteData.left = new FormAttachment(0, margin);
    deleteData.right = new FormAttachment(100, -margin);
    deleteData.top = new FormAttachment(editButton, 10);
    deleteData.height = 30;
    deleteButton.setLayoutData(deleteData);

    toggleButton = new Button(buttonPanel, SWT.PUSH);
    toggleButton.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Button.ToggleEnabled"));
    toggleButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            toggleRule();
          }
        });

    FormData toggleData = new FormData();
    toggleData.left = new FormAttachment(0, margin);
    toggleData.right = new FormAttachment(100, -margin);
    toggleData.top = new FormAttachment(deleteButton, 20);
    toggleData.height = 30;
    toggleButton.setLayoutData(toggleData);

    Button closeButton = new Button(shell, SWT.PUSH);
    closeButton.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Button.Close"));
    closeButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            shell.dispose();
          }
        });

    FormData closeData = new FormData();
    closeData.right = new FormAttachment(100, -margin);
    closeData.bottom = new FormAttachment(100, -margin);
    closeData.width = 80;
    closeData.height = 30;
    closeButton.setLayoutData(closeData);

    shell.setDefaultButton(closeButton);
  }

  private void populateTable() {
    CustomLintRule selected = rulesTable.getSelectionIndex() < 0 ? null : getSelectedRule();
    rulesTable.removeAll();

    List<CustomLintRule> shown = new ArrayList<>(rules);
    if (sortColumn >= 0) {
      Comparator<CustomLintRule> order =
          Comparator.comparing(
              rule -> rowText(rule)[sortColumn].toLowerCase(Locale.ROOT),
              Comparator.naturalOrder());
      shown.sort(sortAscending ? order : order.reversed());
    }
    for (CustomLintRule rule : shown) {
      TableItem item = new TableItem(rulesTable, SWT.NONE);
      item.setText(rowText(rule));
      item.setData(rule);
    }
    if (selected != null) {
      select(selected);
    }
  }

  private static String[] rowText(CustomLintRule rule) {
    String[] row = new String[10];
    row[0] = rule.getTarget() != null ? rule.generateRuleId() : nullToEmpty(rule.getId());
    row[1] = nullToEmpty(rule.getName());
    row[2] = rule.getPackOwner() != null ? rule.getPackOwner().getDisplayName() : "";
    row[3] = rule.getTarget() != null ? rule.getTarget().getDisplayName() : "";
    // A composed rule checks several things, and showing only its first clause made it
    // indistinguishable from a rule that checks one. Say so in the columns that would otherwise be
    // a half-truth.
    if (rule.isComposed()) {
      row[4] = rule.getClauses().size() + " fields (" + rule.getCombinator().getYamlKey() + ")";
      // A condition says what is required; the rule reports when the conditions are not met.
      row[5] = rule.getCombinator() == RuleCombinator.ALL_OF ? "All not met" : "Any not met";
      row[6] = "";
    } else {
      row[4] = nullToEmpty(rule.getTargetField());
      row[5] = rule.getCondition() != null ? rule.getCondition().getDisplayName() : "";
      row[6] = nullToEmpty(rule.getConditionValue());
    }
    row[7] = rule.getSeverity() != null ? rule.getSeverity() : "WARNING";
    row[8] = rule.isEnabled() ? "✓" : "✗";
    row[9] = String.join(", ", LintRuleDetails.flatTags(rule.getTags()));
    return row;
  }

  /** Sort on a column, or turn the order round when it is already the one sorted on. */
  private void sortOn(int column) {
    sortAscending = column != sortColumn || !sortAscending;
    sortColumn = column;
    rulesTable.setSortColumn(rulesTable.getColumn(column));
    rulesTable.setSortDirection(sortAscending ? SWT.UP : SWT.DOWN);
    populateTable();
  }

  private void select(CustomLintRule rule) {
    int index = findTableIndex(rule);
    if (index >= 0) {
      rulesTable.setSelection(index);
      rulesTable.showSelection();
    }
    updateButtonState();
  }

  private CustomLintRule getSelectedRule() {
    int selectionIndex = rulesTable.getSelectionIndex();
    if (selectionIndex < 0) {
      return null;
    }
    return (CustomLintRule) rulesTable.getItem(selectionIndex).getData();
  }

  private void updateButtonState() {
    CustomLintRule selected = getSelectedRule();
    boolean hasSelection = selected != null;
    boolean projectEditable = hasSelection && selected.isProjectEditable();

    // A pack rule can be edited (the change lands in hop-lint.yml) but not deleted: it belongs to
    // the pack. Switching it off is what Toggle Enabled is for.
    editButton.setEnabled(hasSelection);
    deleteButton.setEnabled(projectEditable);
    toggleButton.setEnabled(hasSelection);
    // A project rule with a pack rule's id replaces it. Removing it brings the pack rule back,
    // which "Delete" did not say.
    deleteButton.setText(
        BaseMessages.getString(
            PKG,
            projectEditable && shadowsPackRule(selected)
                ? "RuleManagerDialog.Button.RestorePackRule"
                : "RuleManagerDialog.Button.DeleteRule"));
  }

  /**
   * Compared exactly, as the rule registry does: a project rule {@code trans-002} sits beside the
   * pack's TRANS-002 rather than replacing it.
   */
  private boolean shadowsPackRule(CustomLintRule rule) {
    if (packRuleIds == null) {
      packRuleIds = ProjectLintYamlExporter.packRuleIds();
    }
    return packRuleIds.contains(rule.generateRuleId());
  }

  /** Every rule id but this rule's own, for the editor to check a new id against. */
  private List<String> idsOtherThan(CustomLintRule rule) {
    List<String> ids = new ArrayList<>();
    for (CustomLintRule other : rules) {
      if (other != rule) {
        ids.add(other.generateRuleId());
      }
    }
    return ids;
  }

  /** PROJECT-001, or the first number after it that no rule uses yet. */
  private String suggestRuleId() {
    Set<String> taken = new TreeSet<>();
    for (CustomLintRule rule : rules) {
      taken.add(rule.generateRuleId().toUpperCase(Locale.ROOT));
    }
    for (int i = 1; ; i++) {
      String id = String.format("PROJECT-%03d", i);
      if (!taken.contains(id)) {
        return id;
      }
    }
  }

  /** Put the saved version of a rule where the old one was. */
  private void replaceInList(CustomLintRule old, CustomLintRule replacement) {
    for (int i = 0; i < rules.size(); i++) {
      if (rules.get(i) == old) {
        rules.set(i, replacement);
        return;
      }
    }
    rules.add(replacement);
  }

  private void addRule() {
    CustomLintRule draft = new CustomLintRule();
    draft.setId(suggestRuleId());
    RuleBuilderDialog dialog = new RuleBuilderDialog(shell, draft);
    dialog.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Title.Add"));
    dialog.setTakenIds(idsOtherThan(draft));
    CustomLintRule newRule = dialog.open();
    if (newRule == null) {
      return;
    }
    newRule.setPackId(RulePackIds.PROJECT);
    newRule.setPackOwner(RulePackOwner.PROJECT);
    // Listed only once it is saved: a rule which failed to save went on showing as if added.
    if (saveRule(newRule, null)) {
      rules.add(newRule);
      populateTable();
      select(newRule);
      log.logBasic("Added project rule: " + newRule.getName());
    }
  }

  private void editRule() {
    CustomLintRule rule = getSelectedRule();
    if (rule == null) {
      showMessage("No Selection", "Please select a rule to edit.");
      return;
    }
    // Pack rules are editable too. The pack file itself is never touched: the change is written to
    // the project's hop-lint.yml, as an override when it only tunes the rule and as a full rule
    // definition when it redefines it. That keeps the pack upgradeable and the decision with the
    // project.
    //
    // The editor works on a copy, so a change that fails to save leaves the rule as it was.
    String previousId = rule.generateRuleId();
    RuleBuilderDialog dialog = new RuleBuilderDialog(shell, rule.copy());
    dialog.setTakenIds(idsOtherThan(rule));
    CustomLintRule editedRule = dialog.open();
    if (editedRule == null) {
      return;
    }
    boolean redefinesPackRule =
        !rule.isProjectEditable() && ProjectLintYamlExporter.redefinesPackRule(editedRule);
    if (redefinesPackRule && !confirmRedefine(editedRule)) {
      return;
    }
    if (saveRule(editedRule, previousId)) {
      if (redefinesPackRule) {
        // Written out in full, it is the project's rule now, as it will be when next loaded.
        editedRule.setPackId(RulePackIds.PROJECT);
        editedRule.setPackOwner(RulePackOwner.PROJECT);
      }
      replaceInList(rule, editedRule);
      populateTable();
      select(editedRule);
      log.logBasic("Edited rule: " + editedRule.getName());
    }
  }

  private boolean confirmRedefine(CustomLintRule rule) {
    MessageBox confirmBox = new MessageBox(shell, SWT.ICON_WARNING | SWT.YES | SWT.NO);
    confirmBox.setText(
        BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.ConfirmRedefine.Title"));
    confirmBox.setMessage(
        BaseMessages.getString(
            PKG, "RuleManagerDialog.Dialog.ConfirmRedefine.Message", rule.generateRuleId()));
    return confirmBox.open() == SWT.YES;
  }

  private void deleteRule() {
    CustomLintRule rule = getSelectedRule();
    if (rule == null) {
      showMessage("No Selection", "Please select a rule to delete.");
      return;
    }
    if (!rule.isProjectEditable()) {
      showPackRuleMessage("delete");
      return;
    }

    String ruleId = rule.generateRuleId();
    CustomLintRule packRule = ProjectLintYamlExporter.packRule(ruleId);
    MessageBox confirmBox = new MessageBox(shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    if (packRule != null) {
      confirmBox.setText(
          BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.ConfirmRestore.Title"));
      confirmBox.setMessage(
          BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.ConfirmRestore.Message", ruleId));
    } else {
      confirmBox.setText(
          BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.ConfirmDelete.Title"));
      confirmBox.setMessage(
          BaseMessages.getString(
              PKG, "RuleManagerDialog.Dialog.ConfirmDelete.Message", rule.getName()));
    }
    if (confirmBox.open() != SWT.YES) {
      return;
    }

    try {
      LinterConfigPlugin.getInstance().removeProjectRule(ruleId);
    } catch (Exception e) {
      showSaveError(rule, e);
      return;
    }
    if (packRule != null) {
      replaceInList(rule, packRule);
      populateTable();
      select(packRule);
      log.logBasic("Restored pack rule: " + ruleId);
    } else {
      rules.remove(rule);
      populateTable();
      updateButtonState();
      log.logBasic("Deleted project rule: " + rule.getName());
    }
  }

  private void toggleRule() {
    CustomLintRule rule = getSelectedRule();
    if (rule == null) {
      showMessage("No Selection", "Please select a rule to toggle.");
      return;
    }

    CustomLintRule toggled = rule.copy();
    toggled.setEnabled(!rule.isEnabled());
    if (saveRule(toggled, null)) {
      replaceInList(rule, toggled);
      populateTable();
      select(toggled);
      log.logBasic(
          "Toggled rule: "
              + toggled.getName()
              + " to "
              + (toggled.isEnabled() ? "enabled" : "disabled"));
    }
  }

  private int findTableIndex(CustomLintRule rule) {
    for (int i = 0; i < rulesTable.getItemCount(); i++) {
      if (rulesTable.getItem(i).getData() == rule) {
        return i;
      }
    }
    return -1;
  }

  private void showPackRuleMessage(String action) {
    MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
    messageBox.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.PackRule.Title"));
    messageBox.setMessage(
        "Cannot "
            + action
            + " rules from the "
            + getSelectedRule().getPackOwner().getDisplayName()
            + " pack.\n\n"
            + "Use Toggle Enabled to disable this rule in your project's hop-lint.yml, "
            + "or Add Project Rule to define a project-specific rule.");
    messageBox.open();
  }

  private void showMessage(String title, String message) {
    MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
    messageBox.setText(title);
    messageBox.setMessage(message);
    messageBox.open();
  }

  /**
   * Write the one rule that changed. The rest of hop-lint.yml — other rules, exclusions,
   * suppressions and comments — stays as it is.
   */
  private boolean saveRule(CustomLintRule rule, String previousId) {
    try {
      LinterConfigPlugin.getInstance().saveProjectRule(rule, previousId);
      return true;
    } catch (Exception e) {
      showSaveError(rule, e);
      return false;
    }
  }

  /** A save that failed used to be logged only, so the change looked saved and was not. */
  private void showSaveError(CustomLintRule rule, Exception e) {
    log.logError("Error saving lint rule " + rule.generateRuleId() + ": " + e.getMessage(), e);
    MessageBox messageBox = new MessageBox(shell, SWT.ICON_ERROR | SWT.OK);
    messageBox.setText(BaseMessages.getString(PKG, "RuleManagerDialog.Dialog.SaveFailed.Title"));
    messageBox.setMessage(
        BaseMessages.getString(
                PKG, "RuleManagerDialog.Dialog.SaveFailed.Message", rule.generateRuleId())
            + "\n\n"
            + e.getMessage());
    messageBox.open();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
