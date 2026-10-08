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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Dialog for creating and editing custom lint rules */
public class RuleBuilderDialog extends Dialog {

  private static final Class<?> PKG = RuleBuilderDialog.class; // for i18n purposes

  private Shell shell;
  private Shell parent;
  private CustomLintRule rule;
  private boolean ok = false;

  // UI components
  private Text nameText;
  private Text descriptionText;
  private Combo targetCombo;
  private Combo fieldCombo;
  private Combo conditionCombo;
  private Text valueText;
  private Label valueLabel;
  private Combo severityCombo;
  private Button enabledCheck;
  private Combo combinatorCombo;
  private Table clauseTable;
  private Button addClauseButton;
  private Button removeClauseButton;
  private Text helpUriText;
  private TableView tagsTable;

  /**
   * The clauses being edited. Always at least one; a rule which checks one thing has exactly one.
   */
  private final List<RuleClause> editingClauses = new ArrayList<>();

  /** What the condition combo offers, in the order it shows them. */
  private List<RuleCondition> conditionChoices = new ArrayList<>();

  /** Guards the widget listeners while the widgets are being loaded from a clause. */
  private boolean loadingClause = false;

  public RuleBuilderDialog(Shell parent, CustomLintRule rule) {
    super(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    this.parent = parent;
    this.rule = rule != null ? rule : new CustomLintRule();
    setText(rule != null ? "Edit Lint Rule" : "Add Lint Rule");
  }

  public CustomLintRule open() {
    createShell();
    createContents();
    populateControls();

    shell.pack();
    // Roomy enough for the clause table, the fields and the tags table, but never taller than the
    // screen: on a laptop display an 820 pixel dialog put OK and Cancel out of reach. Below the
    // minimum the fixed rows would run into the button bar, so the dialog does not shrink past it.
    Rectangle screen = parent.getMonitor().getClientArea();
    int height = Math.min(820, screen.height);
    shell.setMinimumSize(560, Math.min(700, height));
    shell.setSize(700, height);
    shell.setLocation(
        Math.max(screen.x, parent.getLocation().x + (parent.getSize().x - 700) / 2),
        Math.max(screen.y, parent.getLocation().y + (parent.getSize().y - height) / 2));
    shell.open();

    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return ok ? rule : null;
  }

  private void createShell() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setText(getText());
    shell.setLayout(new FormLayout());
  }

  private void createContents() {
    int margin = 10;
    int labelWidth = 120;

    // Rule Name
    Label nameLabel = new Label(shell, SWT.RIGHT);
    nameLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.RuleName"));
    FormData nameLabelData = new FormData();
    nameLabelData.left = new FormAttachment(0, margin);
    nameLabelData.right = new FormAttachment(0, labelWidth);
    nameLabelData.top = new FormAttachment(0, margin);
    nameLabel.setLayoutData(nameLabelData);

    nameText = new Text(shell, SWT.BORDER);
    FormData nameData = new FormData();
    nameData.left = new FormAttachment(nameLabel, margin);
    nameData.right = new FormAttachment(100, -margin);
    nameData.top = new FormAttachment(0, margin);
    nameText.setLayoutData(nameData);

    // Description
    Label descLabel = new Label(shell, SWT.RIGHT);
    descLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Description"));
    FormData descLabelData = new FormData();
    descLabelData.left = new FormAttachment(0, margin);
    descLabelData.right = new FormAttachment(0, labelWidth);
    descLabelData.top = new FormAttachment(nameText, margin);
    descLabel.setLayoutData(descLabelData);

    descriptionText = new Text(shell, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL);
    FormData descData = new FormData();
    descData.left = new FormAttachment(descLabel, margin);
    descData.right = new FormAttachment(100, -margin);
    descData.top = new FormAttachment(nameText, margin);
    descData.height = 60;
    descriptionText.setLayoutData(descData);

    // Target Type
    Label targetLabel = new Label(shell, SWT.RIGHT);
    targetLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.TargetType"));
    FormData targetLabelData = new FormData();
    targetLabelData.left = new FormAttachment(0, margin);
    targetLabelData.right = new FormAttachment(0, labelWidth);
    targetLabelData.top = new FormAttachment(descriptionText, margin);
    targetLabel.setLayoutData(targetLabelData);

    targetCombo = new Combo(shell, SWT.DROP_DOWN | SWT.READ_ONLY);
    for (RuleTarget target : RuleTarget.values()) {
      targetCombo.add(target.getDisplayName());
    }
    targetCombo.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateFieldCombo(null);
          }
        });
    FormData targetData = new FormData();
    targetData.left = new FormAttachment(targetLabel, margin);
    targetData.right = new FormAttachment(100, -margin);
    targetData.top = new FormAttachment(descriptionText, margin);
    targetCombo.setLayoutData(targetData);

    // How the clauses combine. Only meaningful once there is more than one.
    Label combinatorLabel = new Label(shell, SWT.RIGHT);
    combinatorLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Match"));
    FormData combinatorLabelData = new FormData();
    combinatorLabelData.left = new FormAttachment(0, margin);
    combinatorLabelData.right = new FormAttachment(0, labelWidth);
    combinatorLabelData.top = new FormAttachment(targetCombo, margin);
    combinatorLabel.setLayoutData(combinatorLabelData);

    combinatorCombo = new Combo(shell, SWT.DROP_DOWN | SWT.READ_ONLY);
    combinatorCombo.setItems(
        new String[] {
          BaseMessages.getString(PKG, "RuleBuilderDialog.Combinator.AllOf"),
          BaseMessages.getString(PKG, "RuleBuilderDialog.Combinator.AnyOf")
        });
    combinatorCombo.select(0);
    FormData combinatorData = new FormData();
    combinatorData.left = new FormAttachment(combinatorLabel, margin);
    combinatorData.right = new FormAttachment(100, -margin);
    combinatorData.top = new FormAttachment(targetCombo, margin);
    combinatorCombo.setLayoutData(combinatorData);

    // The clauses themselves. The field, condition and value below edit whichever row is selected.
    clauseTable = new Table(shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE);
    clauseTable.setHeaderVisible(true);
    clauseTable.setLinesVisible(true);
    TableColumn clauseFieldColumn = new TableColumn(clauseTable, SWT.LEFT);
    clauseFieldColumn.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Field"));
    clauseFieldColumn.setWidth(180);
    TableColumn clauseConditionColumn = new TableColumn(clauseTable, SWT.LEFT);
    clauseConditionColumn.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Condition"));
    clauseConditionColumn.setWidth(160);
    TableColumn clauseValueColumn = new TableColumn(clauseTable, SWT.LEFT);
    clauseValueColumn.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Value"));
    clauseValueColumn.setWidth(140);
    clauseTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            loadSelectedClauseIntoWidgets();
          }
        });
    FormData clauseTableData = new FormData();
    clauseTableData.left = new FormAttachment(0, margin + labelWidth);
    clauseTableData.right = new FormAttachment(100, -margin);
    clauseTableData.top = new FormAttachment(combinatorCombo, margin);
    clauseTableData.height = 90;
    clauseTable.setLayoutData(clauseTableData);

    addClauseButton = new Button(shell, SWT.PUSH);
    addClauseButton.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Button.AddClause"));
    addClauseButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            addClause();
          }
        });
    FormData addClauseData = new FormData();
    addClauseData.left = new FormAttachment(0, margin + labelWidth);
    addClauseData.top = new FormAttachment(clauseTable, margin);
    addClauseButton.setLayoutData(addClauseData);

    removeClauseButton = new Button(shell, SWT.PUSH);
    removeClauseButton.setText(
        BaseMessages.getString(PKG, "RuleBuilderDialog.Button.RemoveClause"));
    removeClauseButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            removeSelectedClause();
          }
        });
    FormData removeClauseData = new FormData();
    removeClauseData.left = new FormAttachment(addClauseButton, margin);
    removeClauseData.top = new FormAttachment(clauseTable, margin);
    removeClauseButton.setLayoutData(removeClauseData);

    // Target Field
    Label fieldLabel = new Label(shell, SWT.RIGHT);
    fieldLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Field"));
    FormData fieldLabelData = new FormData();
    fieldLabelData.left = new FormAttachment(0, margin);
    fieldLabelData.right = new FormAttachment(0, labelWidth);
    fieldLabelData.top = new FormAttachment(removeClauseButton, margin);
    fieldLabel.setLayoutData(fieldLabelData);

    fieldCombo = new Combo(shell, SWT.DROP_DOWN | SWT.READ_ONLY);
    fieldCombo.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateConditionCombo(null);
            captureWidgetsIntoSelectedClause();
          }
        });
    FormData fieldData = new FormData();
    fieldData.left = new FormAttachment(fieldLabel, margin);
    fieldData.right = new FormAttachment(100, -margin);
    fieldData.top = new FormAttachment(removeClauseButton, margin);
    fieldCombo.setLayoutData(fieldData);

    // Condition
    Label conditionLabel = new Label(shell, SWT.RIGHT);
    conditionLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Condition"));
    FormData conditionLabelData = new FormData();
    conditionLabelData.left = new FormAttachment(0, margin);
    conditionLabelData.right = new FormAttachment(0, labelWidth);
    conditionLabelData.top = new FormAttachment(fieldCombo, margin);
    conditionLabel.setLayoutData(conditionLabelData);

    conditionCombo = new Combo(shell, SWT.DROP_DOWN | SWT.READ_ONLY);
    conditionCombo.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateValueField();
            captureWidgetsIntoSelectedClause();
          }
        });
    FormData conditionData = new FormData();
    conditionData.left = new FormAttachment(conditionLabel, margin);
    conditionData.right = new FormAttachment(100, -margin);
    conditionData.top = new FormAttachment(fieldCombo, margin);
    conditionCombo.setLayoutData(conditionData);

    // Value
    valueLabel = new Label(shell, SWT.RIGHT);
    valueLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Value"));
    FormData valueLabelData = new FormData();
    valueLabelData.left = new FormAttachment(0, margin);
    valueLabelData.right = new FormAttachment(0, labelWidth);
    valueLabelData.top = new FormAttachment(conditionCombo, margin);
    valueLabel.setLayoutData(valueLabelData);

    valueText = new Text(shell, SWT.BORDER);
    valueText.addModifyListener(e -> captureWidgetsIntoSelectedClause());
    FormData valueData = new FormData();
    valueData.left = new FormAttachment(valueLabel, margin);
    valueData.right = new FormAttachment(100, -margin);
    valueData.top = new FormAttachment(conditionCombo, margin);
    valueText.setLayoutData(valueData);

    // Severity
    Label severityLabel = new Label(shell, SWT.RIGHT);
    severityLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Severity"));
    FormData severityLabelData = new FormData();
    severityLabelData.left = new FormAttachment(0, margin);
    severityLabelData.right = new FormAttachment(0, labelWidth);
    severityLabelData.top = new FormAttachment(valueText, margin);
    severityLabel.setLayoutData(severityLabelData);

    severityCombo = new Combo(shell, SWT.DROP_DOWN | SWT.READ_ONLY);
    for (LintSeverity.Level level : LintSeverity.Level.values()) {
      severityCombo.add(level.name());
    }
    severityCombo.select(LintSeverity.Level.WARNING.ordinal());
    FormData severityData = new FormData();
    severityData.left = new FormAttachment(severityLabel, margin);
    severityData.right = new FormAttachment(100, -margin);
    severityData.top = new FormAttachment(valueText, margin);
    severityCombo.setLayoutData(severityData);

    // Enabled checkbox
    enabledCheck = new Button(shell, SWT.CHECK);
    enabledCheck.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Checkbox.Enabled"));
    enabledCheck.setSelection(true);
    FormData enabledData = new FormData();
    enabledData.left = new FormAttachment(severityLabel, margin);
    enabledData.top = new FormAttachment(severityCombo, margin);
    enabledCheck.setLayoutData(enabledData);

    // Help URL, written to SARIF as helpUri
    Label helpUriLabel = new Label(shell, SWT.RIGHT);
    helpUriLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.HelpUri"));
    FormData helpUriLabelData = new FormData();
    helpUriLabelData.left = new FormAttachment(0, margin);
    helpUriLabelData.right = new FormAttachment(0, labelWidth);
    helpUriLabelData.top = new FormAttachment(enabledCheck, margin);
    helpUriLabel.setLayoutData(helpUriLabelData);

    helpUriText = new Text(shell, SWT.BORDER);
    FormData helpUriData = new FormData();
    helpUriData.left = new FormAttachment(helpUriLabel, margin);
    helpUriData.right = new FormAttachment(100, -margin);
    helpUriData.top = new FormAttachment(enabledCheck, margin);
    helpUriText.setLayoutData(helpUriData);

    // Buttons
    Button okButton = new Button(shell, SWT.PUSH);
    okButton.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Button.Ok"));
    okButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ok();
          }
        });
    FormData okData = new FormData();
    okData.right = new FormAttachment(100, -margin);
    okData.bottom = new FormAttachment(100, -margin);
    okData.width = 80;
    okButton.setLayoutData(okData);

    Button cancelButton = new Button(shell, SWT.PUSH);
    cancelButton.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Button.Cancel"));
    cancelButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancel();
          }
        });
    FormData cancelData = new FormData();
    cancelData.right = new FormAttachment(okButton, -margin);
    cancelData.bottom = new FormAttachment(100, -margin);
    cancelData.width = 80;
    cancelButton.setLayoutData(cancelData);

    // Tags: one row per key and value, so a value never needs escaping. The table takes whatever
    // height is left between the help URL and the buttons, so resizing the dialog grows it rather
    // than pushing it under the buttons.
    Label tagsLabel = new Label(shell, SWT.RIGHT);
    tagsLabel.setText(BaseMessages.getString(PKG, "RuleBuilderDialog.Label.Tags"));
    tagsLabel.setToolTipText(BaseMessages.getString(PKG, "RuleBuilderDialog.Tags.Tooltip"));
    FormData tagsLabelData = new FormData();
    tagsLabelData.left = new FormAttachment(0, margin);
    tagsLabelData.right = new FormAttachment(0, labelWidth);
    tagsLabelData.top = new FormAttachment(helpUriText, margin);
    tagsLabel.setLayoutData(tagsLabelData);

    ColumnInfo[] tagColumns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "RuleBuilderDialog.Column.TagKey"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "RuleBuilderDialog.Column.TagValue"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false)
    };
    tagsTable =
        new TableView(
            Variables.getADefaultVariableSpace(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            tagColumns,
            1,
            null,
            PropsUi.getInstance());
    tagsTable.setToolTipText(BaseMessages.getString(PKG, "RuleBuilderDialog.Tags.Tooltip"));
    FormData tagsData = new FormData();
    tagsData.left = new FormAttachment(tagsLabel, margin);
    tagsData.right = new FormAttachment(100, -margin);
    tagsData.top = new FormAttachment(helpUriText, margin);
    tagsData.bottom = new FormAttachment(okButton, -margin);
    tagsTable.setLayoutData(tagsData);

    shell.setDefaultButton(okButton);
  }

  private void populateControls() {
    if (rule.getName() != null) {
      nameText.setText(rule.getName());
    }
    if (rule.getDescription() != null) {
      descriptionText.setText(rule.getDescription());
    }
    if (rule.getTarget() != null) {
      targetCombo.select(rule.getTarget().ordinal());
      updateFieldCombo(null);
    }
    // Selected by index: setText does nothing on a read-only combo, which left WARNING showing for
    // an INFO rule and saved it as WARNING.
    if (rule.getSeverity() != null) {
      int severityIndex = severityCombo.indexOf(rule.getSeverity().trim().toUpperCase());
      if (severityIndex >= 0) {
        severityCombo.select(severityIndex);
      }
    }
    enabledCheck.setSelection(rule.isEnabled());
    helpUriText.setText(rule.getHelpUri() != null ? rule.getHelpUri() : "");
    populateTags();
    loadClausesFromRule();

    // A native rule, such as HOP-CHECK, says how Hop's own verify remarks are reported. It has no
    // target, field or condition, and the editor refused to save it, even a severity change.
    if (rule.isNativeVerify()) {
      for (Control control :
          new Control[] {
            targetCombo,
            combinatorCombo,
            clauseTable,
            addClauseButton,
            removeClauseButton,
            fieldCombo,
            conditionCombo,
            valueText
          }) {
        control.setEnabled(false);
      }
    }
  }

  /**
   * @param currentField the field the clause being shown reads, kept in the list even when the
   *     editor would not have offered it
   */
  private void updateFieldCombo(String currentField) {
    fieldCombo.removeAll();
    conditionCombo.removeAll();
    conditionChoices = new ArrayList<>();

    int targetIndex = targetCombo.getSelectionIndex();
    if (targetIndex >= 0) {
      RuleTarget target = RuleTarget.values()[targetIndex];
      for (String field : RuleTargetFields.getFieldChoices(target, currentField)) {
        fieldCombo.add(field);
      }
    }
  }

  /**
   * @param currentCondition the condition the clause being shown uses, kept in the list even when
   *     the editor would not have offered it for this field
   */
  private void updateConditionCombo(RuleCondition currentCondition) {
    conditionCombo.removeAll();
    conditionChoices = new ArrayList<>();

    String selectedField = fieldCombo.getText();
    if (!Utils.isEmpty(selectedField)) {
      conditionChoices = RuleTargetFields.getConditionChoices(selectedField, currentCondition);
      for (RuleCondition condition : conditionChoices) {
        conditionCombo.add(condition.getDisplayName());
      }
    }
  }

  private RuleCondition selectedCondition() {
    int conditionIndex = conditionCombo.getSelectionIndex();
    if (conditionIndex < 0 || conditionIndex >= conditionChoices.size()) {
      return null;
    }
    return conditionChoices.get(conditionIndex);
  }

  private void updateValueField() {
    RuleCondition condition = selectedCondition();
    if (condition != null) {
      valueLabel.setVisible(condition.requiresValue());
      valueText.setVisible(condition.requiresValue());

      if (condition.requiresValue()) {
        valueText.setToolTipText(condition.getDescription());
      }
    }
    shell.layout(true, true);
  }

  private void ok() {
    if (validate()) {
      saveRule();
      ok = true;
      shell.dispose();
    }
  }

  private boolean validate() {
    if (Utils.isEmpty(nameText.getText())) {
      showError("Rule name is required");
      return false;
    }
    // A row with only a key or only a value is not a tag. Saving would drop it without a word.
    for (TableItem item : tagsTable.getNonEmptyItems()) {
      if (item.getText(1).trim().isEmpty() || item.getText(2).trim().isEmpty()) {
        showError(BaseMessages.getString(PKG, "RuleBuilderDialog.Tags.Incomplete"));
        return false;
      }
    }
    if (rule.isNativeVerify()) {
      return true;
    }
    if (targetCombo.getSelectionIndex() < 0) {
      showError("Target type must be selected");
      return false;
    }
    if (fieldCombo.getSelectionIndex() < 0) {
      showError("Field must be selected");
      return false;
    }
    if (conditionCombo.getSelectionIndex() < 0) {
      showError("Condition must be selected");
      return false;
    }

    // Check if condition requires a value
    RuleCondition condition = selectedCondition();
    if (condition != null && condition.requiresValue() && Utils.isEmpty(valueText.getText())) {
      showError("Value is required for this condition");
      return false;
    }

    return true;
  }

  private void saveRule() {
    rule.setName(nameText.getText());
    rule.setDescription(descriptionText.getText());
    rule.setSeverity(severityCombo.getText());
    rule.setEnabled(enabledCheck.getSelection());
    rule.setHelpUri(
        Utils.isEmpty(helpUriText.getText().trim()) ? null : helpUriText.getText().trim());
    rule.setTags(readTags());
    if (rule.isNativeVerify()) {
      return;
    }

    captureWidgetsIntoSelectedClause();
    rule.setTarget(RuleTarget.values()[targetCombo.getSelectionIndex()]);
    rule.setCombinator(
        combinatorCombo.getSelectionIndex() == 1 ? RuleCombinator.ANY_OF : RuleCombinator.ALL_OF);

    // The first clause lives in the rule's own fields and the rest hang off it, which is the shape
    // the YAML, the exporter and the rule table all expect.
    RuleClause first = editingClauses.get(0);
    rule.setTargetField(first.getTargetField());
    rule.setCondition(first.getCondition());
    rule.setConditionValue(first.getConditionValue());

    List<RuleClause> rest = new ArrayList<>();
    for (int i = 1; i < editingClauses.size(); i++) {
      rest.add(editingClauses.get(i).copy());
    }
    rule.setAdditionalClauses(rest);
  }

  private void populateTags() {
    tagsTable.clearAll(false);
    rule.getTags()
        .forEach(
            (key, values) -> {
              for (String value : values) {
                TableItem item = new TableItem(tagsTable.table, SWT.NONE);
                item.setText(1, key);
                item.setText(2, value);
              }
            });
    tagsTable.removeEmptyRows();
    tagsTable.setRowNums();
    tagsTable.optWidth(true);
  }

  /**
   * The tags as entered, grouped by key in the order the keys first appear. {@link #validate()} has
   * refused rows with only a key or only a value; a row entered twice is kept once.
   */
  private Map<String, List<String>> readTags() {
    Map<String, List<String>> tags = new LinkedHashMap<>();
    for (TableItem item : tagsTable.getNonEmptyItems()) {
      String key = item.getText(1).trim();
      String value = item.getText(2).trim();
      if (!key.isEmpty() && !value.isEmpty()) {
        List<String> values = tags.computeIfAbsent(key, k -> new ArrayList<>());
        if (!values.contains(value)) {
          values.add(value);
        }
      }
    }
    return tags;
  }

  /** Load the rule's clauses into the table, selecting the first. */
  private void loadClausesFromRule() {
    editingClauses.clear();
    for (RuleClause clause : rule.getClauses()) {
      editingClauses.add(clause.copy());
    }
    if (editingClauses.isEmpty()) {
      editingClauses.add(new RuleClause("", null, ""));
    }
    combinatorCombo.select(rule.getCombinator() == RuleCombinator.ANY_OF ? 1 : 0);
    refreshClauseTable();
    clauseTable.setSelection(0);
    loadSelectedClauseIntoWidgets();
  }

  private void refreshClauseTable() {
    int selected = clauseTable.getSelectionIndex();
    clauseTable.removeAll();
    for (RuleClause clause : editingClauses) {
      TableItem item = new TableItem(clauseTable, SWT.NONE);
      item.setText(0, clause.getTargetField() == null ? "" : clause.getTargetField());
      item.setText(1, clause.getCondition() == null ? "" : clause.getCondition().getDisplayName());
      item.setText(2, clause.getConditionValue() == null ? "" : clause.getConditionValue());
    }
    if (selected >= 0 && selected < clauseTable.getItemCount()) {
      clauseTable.setSelection(selected);
    }
    // One clause is an ordinary rule, so there is nothing to combine and nothing to remove.
    combinatorCombo.setEnabled(editingClauses.size() > 1);
    removeClauseButton.setEnabled(editingClauses.size() > 1);
  }

  /** Show the selected clause in the field, condition and value widgets. */
  private void loadSelectedClauseIntoWidgets() {
    int index = clauseTable.getSelectionIndex();
    if (index < 0 || index >= editingClauses.size()) {
      return;
    }
    RuleClause clause = editingClauses.get(index);
    loadingClause = true;
    try {
      updateFieldCombo(clause.getTargetField());
      if (clause.getTargetField() != null) {
        int fieldIndex = fieldCombo.indexOf(clause.getTargetField());
        if (fieldIndex >= 0) {
          fieldCombo.select(fieldIndex);
        }
      }
      updateConditionCombo(clause.getCondition());
      if (clause.getCondition() != null) {
        conditionCombo.select(conditionChoices.indexOf(clause.getCondition()));
      }
      updateValueField();
      valueText.setText(clause.getConditionValue() == null ? "" : clause.getConditionValue());
    } finally {
      loadingClause = false;
    }
  }

  /** Write the widgets back into the selected clause, so edits are not lost on row change. */
  private void captureWidgetsIntoSelectedClause() {
    if (loadingClause) {
      return;
    }
    int index = clauseTable.getSelectionIndex();
    if (index < 0 || index >= editingClauses.size()) {
      return;
    }
    RuleClause clause = editingClauses.get(index);
    clause.setTargetField(fieldCombo.getText());
    RuleCondition condition = selectedCondition();
    if (condition != null) {
      clause.setCondition(condition);
    }
    clause.setConditionValue(valueText.getText());
    refreshClauseTable();
  }

  private void addClause() {
    captureWidgetsIntoSelectedClause();
    editingClauses.add(new RuleClause("", null, ""));
    refreshClauseTable();
    clauseTable.setSelection(editingClauses.size() - 1);
    loadSelectedClauseIntoWidgets();
  }

  private void removeSelectedClause() {
    int index = clauseTable.getSelectionIndex();
    if (index < 0 || editingClauses.size() <= 1) {
      return;
    }
    editingClauses.remove(index);
    refreshClauseTable();
    clauseTable.setSelection(Math.min(index, editingClauses.size() - 1));
    loadSelectedClauseIntoWidgets();
  }

  private void cancel() {
    shell.dispose();
  }

  private void showError(String message) {
    MessageBox messageBox = new MessageBox(shell, SWT.ICON_ERROR | SWT.OK);
    messageBox.setText(
        BaseMessages.getString(PKG, "RuleBuilderDialog.Dialog.ValidationError.Title"));
    messageBox.setMessage(message);
    messageBox.open();
  }
}
