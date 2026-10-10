/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class WatchFilesDialog extends BaseTransformDialog {
  private final WatchFilesMeta input;
  private GuiCompositeWidgets widgets;
  private Label patternHelp;
  private Label stateIdentity;

  public WatchFilesDialog(
      Shell parent, IVariables variables, WatchFilesMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(WatchFilesMeta.class, "WatchFilesDialog.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
    changed = input.hasChanged();
    WatchFilesMeta form = (WatchFilesMeta) input.clone();
    form.setInitialScan(WatchFilesMeta.optionLabel("initialScan", input.getInitialScan()));
    form.setStrategy(WatchFilesMeta.optionLabel("strategy", input.getStrategy()));
    form.setPatternSyntax(WatchFilesMeta.optionLabel("patternSyntax", input.getPatternSyntax()));
    form.setMaximumRunTimeUnit(
        WatchFilesMeta.optionLabel("maximumRunTimeUnit", input.getMaximumRunTimeUnit()));
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            WatchFilesMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            form,
            groups -> {
              groups.registerExtraGroup(group("General"), "01", null, this::generalHelp);
              groups.registerExtraGroup(group("Maintenance"), "03", null, this::stateTools);
            });
    widgets.setCompositeWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control control, String id) {
            input.setChanged();
            updateOptions();
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {}
        });
    updateOptions();
    Control viewport = widgets.getWidgetsMap().get("directory").getParent();
    while (!(viewport instanceof org.eclipse.swt.custom.ScrolledComposite)
        || viewport.getParent() != shell) {
      viewport = viewport.getParent();
    }
    FormData viewportLayout = (FormData) viewport.getLayoutData();
    viewportLayout.width = 760;
    viewportLayout.height = 420;
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    readOptions(input);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private String message(String key) {
    return BaseMessages.getString(WatchFilesMeta.class, "WatchFilesDialog." + key);
  }

  private String group(String key) {
    return BaseMessages.getString(WatchFilesMeta.class, "WatchFilesMeta.Group." + key);
  }

  private void readOptions(WatchFilesMeta options) {
    String previousInitialScan = input.getInitialScan();
    widgets.getWidgetsContents(options, WatchFilesMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    // IGNORE_EXISTING and COMPARE_WITH_STATE have the same first-start baseline behavior.
    // Preserve the persisted choice when its displayed label has not changed.
    options.setInitialScan(
        java.util.Objects.equals(
                WatchFilesMeta.optionLabel("initialScan", previousInitialScan),
                options.getInitialScan())
            ? previousInitialScan
            : WatchFilesMeta.optionValue("initialScan", options.getInitialScan()));
    options.setMaximumRunTimeUnit(
        WatchFilesMeta.optionValue("maximumRunTimeUnit", options.getMaximumRunTimeUnit()));
    options.setStrategy(WatchFilesMeta.optionValue("strategy", options.getStrategy()));
    options.setPatternSyntax(
        WatchFilesMeta.optionValue("patternSyntax", options.getPatternSyntax()));
  }

  private void updateOptions() {
    WatchFilesMeta options = (WatchFilesMeta) input.clone();
    readOptions(options);
    Set<String> hidden = new HashSet<>();
    String duration = variables.resolve(options.getMaximumRunTime());
    if (duration == null || duration.isBlank()) hidden.add("maximumRunTimeUnit");
    if (!options.isEditWatchId()) hidden.add("watchId");
    if (!options.isWaitUntilStable()) {
      hidden.addAll(Set.of("stabilityChecks", "stabilityInterval"));
    }
    if ("POLLING".equals(options.getStrategy())) {
      hidden.addAll(Set.of("eventCapacity", "reconciliationInterval"));
    } else if ("NATIVE".equals(options.getStrategy())) {
      hidden.add("pollingInterval");
    }
    widgets.setWidgetsHidden(options, hidden);
    setHelp("strategy", "Detection." + options.getStrategy());
    setHelp("pollingInterval", "PollingHelp." + options.getStrategy());
    setHelp("reconciliationInterval", "ReconciliationHelp");
    setHelp("includeWildcard", "IncludeHelp." + options.getPatternSyntax());
    setHelp("excludeWildcard", "ExcludeHelp." + options.getPatternSyntax());
    patternHelp.setText(
        message("GeneralHelp") + "\n" + message("Patterns." + options.getPatternSyntax()));
    String id = options.getWatchId();
    boolean generated = id != null && id.matches("watch-[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}");
    stateIdentity.setText(message(generated ? "StateIdentity.Automatic" : "StateIdentity.Saved"));
    stateIdentity.setToolTipText(
        BaseMessages.getString(WatchFilesMeta.class, "WatchFilesDialog.StateIdentityHelp", id));
  }

  private void setHelp(String id, String key) {
    String help = message(key);
    Control widget = widgets.getWidgetsMap().get(id);
    if (widget != null) widget.setToolTipText(help);
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) label.setToolTipText(help);
  }

  private Composite extraPanel(Composite parent) {
    Control[] existing = parent.getChildren();
    Composite panel = new Composite(parent, SWT.NONE);
    FormData position = new FormData();
    position.left = new FormAttachment(0);
    position.right = new FormAttachment(100);
    position.top =
        existing.length == 0
            ? new FormAttachment(0)
            : new FormAttachment(existing[existing.length - 1], 10);
    panel.setLayoutData(position);
    panel.setLayout(new GridLayout(2, false));
    return panel;
  }

  private void generalHelp(Composite parent) {
    Composite panel = extraPanel(parent);
    stateIdentity = new Label(panel, SWT.NONE);
    stateIdentity.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
    patternHelp = new Label(panel, SWT.WRAP);
    GridData area = new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1);
    area.widthHint = 650;
    patternHelp.setLayoutData(area);
    patternHelp.setText(message("GeneralHelp"));
  }

  private void stateTools(Composite parent) {
    parent = extraPanel(parent);
    // Other tabs can require a wider scrollable form. Keep these few actions compact.
    FormData panelArea = (FormData) parent.getLayoutData();
    panelArea.right = null;
    panelArea.width = 650;
    Label help = new Label(parent, SWT.WRAP);
    GridData helpArea = new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1);
    helpArea.widthHint = 650;
    help.setLayoutData(helpArea);
    help.setText(message("MaintenanceHelp"));
    Text status =
        new Text(parent, SWT.MULTI | SWT.READ_ONLY | SWT.BORDER | SWT.WRAP | SWT.V_SCROLL);
    GridData area = new GridData(SWT.FILL, SWT.FILL, true, true, 2, 1);
    area.heightHint = 100;
    area.widthHint = 650;
    area.exclude = true;
    status.setLayoutData(area);
    status.setVisible(false);
    actionButton(parent, "Inspect", status, 1);
    actionButton(parent, "Backup", status, 1);
    actionButton(parent, "Replay", status, 2);
    Button advanced = new Button(parent, SWT.CHECK);
    advanced.setText(message("AdvancedRecovery"));
    advanced.setToolTipText(message("AdvancedRecoveryHelp"));
    advanced.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1));
    Composite recovery = new Composite(parent, SWT.NONE);
    recovery.setLayout(new GridLayout(2, false));
    GridData recoveryArea = new GridData(SWT.FILL, SWT.CENTER, true, false, 2, 1);
    recoveryArea.exclude = true;
    recovery.setLayoutData(recoveryArea);
    recovery.setVisible(false);
    actionButton(recovery, "Reset", status, 1);
    actionButton(recovery, "Restore", status, 1);
    actionButton(recovery, "Migrate", status, 2);
    advanced.addListener(
        SWT.Selection,
        event -> {
          recoveryArea.exclude = !advanced.getSelection();
          recovery.setVisible(advanced.getSelection());
          refreshStatePanel(status);
        });
  }

  private void actionButton(Composite parent, String action, Text status, int span) {
    Button button = new Button(parent, SWT.PUSH);
    button.setText(message(action));
    button.setToolTipText(message(action + "Help"));
    button.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false, span, 1));
    button.addListener(SWT.Selection, event -> stateAction(action, status));
  }

  private void refreshStatePanel(Text status) {
    Composite panel = status.getParent();
    panel.layout(true, true);
    Composite content = panel.getParent();
    content.layout(true, true);
    if (content.getParent() instanceof org.eclipse.swt.custom.ScrolledComposite scrolled) {
      scrolled.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    }
  }

  private void showStateResult(Text status, String result) {
    status.setText(result);
    ((GridData) status.getLayoutData()).exclude = false;
    status.setVisible(true);
    refreshStatePanel(status);
  }

  private void stateAction(String action, Text status) {
    try {
      WatchFilesMeta options = (WatchFilesMeta) input.clone();
      readOptions(options);
      if (action.equals("Replay")) {
        if (!options.isCreated()) throw new java.io.IOException(message("CreatedRequired"));
        if (!new WatchFilesReplayDialog(shell, variables, options).open()) return;
      }
      Path directory;
      try (org.apache.commons.vfs2.FileObject folder =
          VfsFileScanner.resolveFile(variables.resolve(options.getStateDirectory()), variables)) {
        if (!(folder instanceof org.apache.commons.vfs2.provider.local.LocalFile)) {
          throw new java.io.IOException(message("LocalStateRequired"));
        }
        directory = VfsFileScanner.localPath(folder);
        if (java.nio.file.Files.exists(directory)) directory = directory.toRealPath();
      }
      WatchFilesStateManager manager =
          new WatchFilesStateManager(
              directory,
              variables.resolve(options.getWatchId()),
              WatchFilesMeta.integer(variables, options.getMaximumEntries(), "Maximum entries"));
      Path selected = null;
      if (action.equals("Restore")) {
        FileDialog chooser = new FileDialog(shell, SWT.OPEN);
        chooser.setText(message("Restore"));
        chooser.setFilterPath(directory.toString());
        chooser.setFilterExtensions(new String[] {"*.json;*.bak", "*"});
        String filename = chooser.open();
        if (filename == null) return;
        selected = Path.of(filename);
      }
      if (action.equals("Replay")
          || action.equals("Reset")
          || action.equals("Restore")
          || action.equals("Migrate")) {
        if (!confirmStateAction(action)) return;
      }
      final Path restore = selected;
      AtomicReference<String> result = new AtomicReference<>();
      new ProgressMonitorDialog(shell)
          .run(
              false,
              monitor -> {
                monitor.beginTask(message(action), 1);
                try {
                  result.set(
                      switch (action) {
                        case "Inspect" -> manager.inspect();
                        case "Backup" -> message("BackupResult") + manager.backup();
                        case "Replay" ->
                            message("ReplayResult")
                                + manager.replay(variables.resolve(options.getReplayFilter()));
                        case "Reset" -> message("BackupResult") + manager.reset();
                        case "Restore" -> message("BackupResult") + manager.restoreBackup(restore);
                        case "Migrate" -> {
                          manager.migrate();
                          yield manager.inspect();
                        }
                        default -> throw new IllegalArgumentException(action);
                      });
                } catch (Exception failure) {
                  throw new java.lang.reflect.InvocationTargetException(failure);
                } finally {
                  monitor.done();
                }
              });
      showStateResult(status, result.get());
    } catch (Exception failure) {
      Throwable cause =
          failure instanceof java.lang.reflect.InvocationTargetException
              ? failure.getCause()
              : failure;
      showStateResult(status, message("StateError") + cause.getMessage());
    }
  }

  protected boolean confirmStateAction(String action) {
    MessageBox confirmation = new MessageBox(shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    confirmation.setText(message(action));
    confirmation.setMessage(message(action + "Confirm"));
    return confirmation.open() == SWT.YES;
  }
}
