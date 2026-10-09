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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Run with tools/with-isolated-display.sh and -Puitest. */
@Tag("uitest")
class WatchFilesDialogTest extends SwtBotTestBase {
  @BeforeAll
  static void registerWidgetsAndTransform() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            WatchFilesMeta.class.getClassLoader(),
            new ArrayList<>(),
            null,
            WatchFilesMeta.class.getName(),
            TransformPluginType.class,
            Transform.class,
            true);
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Field field : WatchFilesMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        var parent = registry.findGuiElements(WatchFilesMeta.class.getName(), element.parentId());
        if (parent == null || parent.findChild(element.id()) == null) {
          registry.addGuiWidgetElement(WatchFilesMeta.class.getName(), element, field);
        }
      }
    }
  }

  private PipelineMeta pipeline(WatchFilesMeta meta) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.addTransform(new TransformMeta("WatchFiles", "Watch input", meta));
    return pipeline;
  }

  @Test
  void groupedTabsRemainAboveButtonsAtDefaultAndSmallSizes() {
    WatchFilesMeta meta = new WatchFilesMeta();
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate();
          display.syncExec(
              () -> {
                CTabFolder folder = find(dialog.widget, CTabFolder.class);
                assertNotNull(folder);
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                    new String[] {"Wildcards (* and ?)", "Java regular expression"},
                    find(
                            (Composite) folder.getItem(0).getControl(),
                            org.eclipse.swt.widgets.Combo.class)
                        .getItems());
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                    new String[] {
                      "Automatic (recommended)",
                      "Native filesystem notifications",
                      "Periodic folder scans"
                    },
                    find(
                            (Composite) folder.getItem(1).getControl(),
                            org.eclipse.swt.widgets.Combo.class)
                        .getItems());
                ScrolledComposite viewport = find(dialog.widget, ScrolledComposite.class);
                assertNotNull(viewport);
                List<String> tabs = new ArrayList<>();
                for (var item : folder.getItems()) {
                  tabs.add(item.getText());
                }
                assertEquals(List.of("General", "Advanced", "Maintenance"), tabs);
                Button ok = null;
                for (Control control : dialog.widget.getChildren()) {
                  if (control instanceof Button button
                      && button
                          .getText()
                          .replace("&", "")
                          .equals(buttonLabel("System.Button.OK"))) {
                    ok = button;
                  }
                }
                assertNotNull(ok);
                if (Boolean.getBoolean("watchfiles.gui.capture")) {
                  SWTUtils.captureScreenshot("target/watchfiles-dialog-default.png");
                }
                for (Point size : List.of(dialog.widget.getSize(), new Point(560, 350))) {
                  dialog.widget.setSize(size);
                  dialog.widget.layout(true, true);
                  Rectangle bounds = displayBounds(viewport);
                  Rectangle buttons = displayBounds(ok);
                  assertFalse(
                      bounds.intersects(buttons), "Options viewport overlaps OK after resize");
                  assertTrue(buttons.y >= bounds.y + bounds.height, "OK must remain below options");
                  assertTrue(bounds.height > 50, "Options must remain usable");
                  assertTrue(displayBounds(folder).intersection(bounds).height > 50);
                  if (viewport.getContent().getSize().x > viewport.getClientArea().width) {
                    assertTrue(
                        viewport.getHorizontalBar().getVisible(),
                        "Width overflow must be scrollable");
                    assertTrue(
                        viewport.getHorizontalBar().getMaximum()
                            > viewport.getHorizontalBar().getThumb());
                  }
                  if (viewport.getContent().getSize().y > viewport.getClientArea().height) {
                    // Packed content is clipped by the viewport; overflow must remain accessible.
                    assertTrue(
                        viewport.getVerticalBar().getVisible(), "Overflow must be scrollable");
                    assertTrue(
                        viewport.getVerticalBar().getMaximum()
                            > viewport.getVerticalBar().getThumb());
                  }
                }
              });
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            // Let the native ConfigureNotify/redraw run before taking the optional QA artifact.
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-dialog-small.png"));
          }
          dialog.bot().button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void optionalRuntimeShowsUnitsAndPersistsFriendlyHourSelection() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDefault();
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          display.syncExec(
              () ->
                  assertFalse(
                      findLabel(bot.shell("Watch Files").widget, "Run time unit").getVisible()));
          dialog.textWithLabel("Maximum run time (blank = continuous)").setText("1.5");
          display.syncExec(
              () ->
                  assertTrue(
                      findLabel(bot.shell("Watch Files").widget, "Run time unit").getVisible()));
          dialog.comboBoxWithLabel("Run time unit").setSelection("Hours");
          dialog.textWithLabel("Maximum run time (blank = continuous)").setText("");
          display.syncExec(
              () ->
                  assertFalse(
                      findLabel(bot.shell("Watch Files").widget, "Run time unit").getVisible()));
          dialog.textWithLabel("Maximum run time (blank = continuous)").setText("${DURATION}");
          assertEquals("Hours", dialog.comboBoxWithLabel("Run time unit").getText());
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-dialog-timeout.png"));
          }
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("${DURATION}", meta.getMaximumRunTime());
    assertEquals("HOURS", meta.getMaximumRunTimeUnit());
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          assertEquals("Hours", dialog.comboBoxWithLabel("Run time unit").getText());
          dialog.textWithLabel("Maximum run time (blank = continuous)").setText("");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals("${DURATION}", meta.getMaximumRunTime());
    assertEquals("HOURS", meta.getMaximumRunTimeUnit());
  }

  @Test
  void okRoundTripsEveryConfiguredOption() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory("${INPUT_FOLDER}");
    meta.setWatchId("test-watch");
    meta.setEditWatchId(true);
    meta.setStateDirectory("${STATE_FOLDER}");
    meta.setPatternSyntax("WILDCARD");
    meta.setIncludeWildcard("*.csv");
    meta.setExcludeWildcard("ignore*");
    meta.setIncludeSubdirectories(true);
    meta.setStrategy("POLLING");
    meta.setInitialScan("EMIT_EXISTING");
    meta.setCreated(false);
    meta.setModified(false);
    meta.setDeleted(true);
    meta.setPollingInterval("1234");
    meta.setReconciliationInterval("4321");
    meta.setCheckpointInterval("5678");
    meta.setWaitUntilStable(false);
    meta.setMinimumAge("9876");
    meta.setStabilityChecks("4");
    meta.setStabilityInterval("300");
    meta.setMaximumEntries("54321");
    meta.setEventCapacity("321");
    meta.setDiagnosticsInterval("2468");
    meta.setSlowOperationThreshold("1357");
    meta.setReplayFilter("recover.*");
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot ->
            bot.shell("Watch Files")
                .activate()
                .bot()
                .button(buttonLabel("System.Button.OK"))
                .click());
    assertEquals("${INPUT_FOLDER}", meta.getDirectory());
    assertEquals("test-watch", meta.getWatchId());
    assertTrue(meta.isEditWatchId());
    assertEquals("${STATE_FOLDER}", meta.getStateDirectory());
    assertEquals("WILDCARD", meta.getPatternSyntax());
    assertEquals("*.csv", meta.getIncludeWildcard());
    assertEquals("ignore*", meta.getExcludeWildcard());
    assertTrue(meta.isIncludeSubdirectories());
    assertEquals("POLLING", meta.getStrategy());
    assertEquals("EMIT_EXISTING", meta.getInitialScan());
    assertFalse(meta.isCreated());
    assertFalse(meta.isModified());
    assertTrue(meta.isDeleted());
    assertEquals("1234", meta.getPollingInterval());
    assertEquals("4321", meta.getReconciliationInterval());
    assertEquals("5678", meta.getCheckpointInterval());
    assertFalse(meta.isWaitUntilStable());
    assertEquals("9876", meta.getMinimumAge());
    assertEquals("4", meta.getStabilityChecks());
    assertEquals("300", meta.getStabilityInterval());
    assertEquals("54321", meta.getMaximumEntries());
    assertEquals("321", meta.getEventCapacity());
    assertEquals("2468", meta.getDiagnosticsInterval());
    assertEquals("1357", meta.getSlowOperationThreshold());
    assertEquals("recover.*", meta.getReplayFilter());
  }

  @Test
  void friendlyChoicesAndConditionalFieldsPreserveIdentityAndHiddenValues() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDefault();
    String id = meta.getWatchId();
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-dialog-general.png"));
          }
          readinessCheckbox(bot.shell("Watch Files").widget).deselect();
          dialog
              .comboBoxWithLabel("First start without saved state")
              .setSelection("Process existing files");
          dialog.cTabItem("Advanced").activate();
          display.syncExec(
              () -> {
                var shell = bot.shell("Watch Files").widget;
                assertTrue(findLabel(shell, "Polling interval (ms)").getVisible());
                assertTrue(findLabel(shell, "Reconciliation interval (ms)").getVisible());
                assertTrue(
                    findLabel(shell, "Detection strategy")
                        .getToolTipText()
                        .contains("Both intervals"));
              });
          dialog
              .comboBoxWithLabel("Detection strategy")
              .setSelection("Native filesystem notifications");
          display.syncExec(
              () -> {
                var shell = bot.shell("Watch Files").widget;
                assertFalse(findLabel(shell, "Polling interval (ms)").getVisible());
                assertTrue(findLabel(shell, "Reconciliation interval (ms)").getVisible());
                assertTrue(findLabel(shell, "Native hint capacity").getVisible());
                assertTrue(
                    findLabel(shell, "Detection strategy")
                        .getToolTipText()
                        .contains("Reconciliation interval"));
                assertTrue(
                    findLabel(shell, "Reconciliation interval (ms)")
                        .getToolTipText()
                        .contains("1 minute"));
              });
          dialog.comboBoxWithLabel("Detection strategy").setSelection("Periodic folder scans");
          display.syncExec(
              () -> {
                var shell = bot.shell("Watch Files").widget;
                assertFalse(findLabel(shell, "Stability checks").getVisible());
                assertFalse(findLabel(shell, "Stability interval (ms)").getVisible());
                assertFalse(findLabel(shell, "Native hint capacity").getVisible());
                assertFalse(findLabel(shell, "Reconciliation interval (ms)").getVisible());
                assertTrue(findLabel(shell, "Minimum file age (ms)").getVisible());
                assertTrue(findLabel(shell, "Polling interval (ms)").getVisible());
                assertTrue(
                    findLabel(shell, "Polling interval (ms)")
                        .getToolTipText()
                        .contains("5 seconds"));
                assertTrue(
                    findLabel(shell, "Detection strategy")
                        .getToolTipText()
                        .contains("Polling interval"));
              });
          dialog.textWithLabel("Polling interval (ms)").setText("${POLL_INTERVAL}");
          dialog.comboBoxWithLabel("Detection strategy").setSelection("Automatic (recommended)");
          assertEquals("${POLL_INTERVAL}", dialog.textWithLabel("Polling interval (ms)").getText());
          assertEquals("60000", dialog.textWithLabel("Reconciliation interval (ms)").getText());
          dialog.comboBoxWithLabel("Detection strategy").setSelection("Periodic folder scans");
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-dialog-advanced.png"));
          }
          dialog.cTabItem("General").activate();
          dialog.comboBoxWithLabel("Pattern syntax").setSelection("Java regular expression");
          display.syncExec(
              () ->
                  assertTrue(
                      findLabel(bot.shell("Watch Files").widget, "Include filename pattern")
                          .getToolTipText()
                          .contains("Java regular expression")));
          dialog.comboBoxWithLabel("Pattern syntax").setSelection("Wildcards (* and ?)");
          display.syncExec(
              () ->
                  assertTrue(
                      findLabel(bot.shell("Watch Files").widget, "Include filename pattern")
                          .getToolTipText()
                          .contains("*test*")));
          readinessCheckbox(bot.shell("Watch Files").widget).select();
          dialog.cTabItem("Advanced").activate();
          display.syncExec(
              () ->
                  assertTrue(
                      findLabel(bot.shell("Watch Files").widget, "Stability checks").getVisible()));
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals(id, meta.getWatchId());
    assertEquals("POLLING", meta.getStrategy());
    assertEquals("EMIT_EXISTING", meta.getInitialScan());
    assertEquals("WILDCARD", meta.getPatternSyntax());
    assertEquals("${POLL_INTERVAL}", meta.getPollingInterval());
    assertEquals("4096", meta.getEventCapacity());
    assertEquals("60000", meta.getReconciliationInterval());
  }

  private org.eclipse.swtbot.swt.finder.widgets.SWTBotCheckBox readinessCheckbox(Composite shell) {
    return checkboxWithLabel(shell, "Wait until the file stops changing");
  }

  private org.eclipse.swtbot.swt.finder.widgets.SWTBotCheckBox checkboxWithLabel(
      Composite shell, String text) {
    java.util.concurrent.atomic.AtomicReference<Button> control =
        new java.util.concurrent.atomic.AtomicReference<>();
    display.syncExec(
        () -> {
          var label = findLabel(shell, text);
          Control[] siblings = label.getParent().getChildren();
          for (int i = 0; i < siblings.length - 1; i++) {
            if (siblings[i] == label) control.set((Button) siblings[i + 1]);
          }
        });
    return new org.eclipse.swtbot.swt.finder.widgets.SWTBotCheckBox(control.get());
  }

  private static org.eclipse.swt.widgets.Label findLabel(Composite parent, String text) {
    for (Control child : parent.getChildren()) {
      if (child instanceof org.eclipse.swt.widgets.Label label && label.getText().equals(text))
        return label;
      if (child instanceof Composite composite) {
        var found = findLabel(composite, text);
        if (found != null) return found;
      }
    }
    return null;
  }

  @Test
  void cancelDoesNotPersistEditedLocation() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory("original-folder");
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          dialog.textWithLabel("Directory").setText("changed-folder");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals("original-folder", meta.getDirectory());
  }

  @Test
  void simplifiedFirstStartChoiceKeepsLegacyCheckpointConfiguration() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setWatchId("sample-watch-files2");
    meta.setStateDirectory("${STATE_FOLDER}");
    meta.setInitialScan("COMPARE_WITH_STATE");
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          assertEquals(
              "Watch new changes only",
              dialog.comboBoxWithLabel("First start without saved state").getText());
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("COMPARE_WITH_STATE", meta.getInitialScan());
    assertEquals("REGEXP", meta.getPatternSyntax());
    assertEquals("sample-watch-files2", meta.getWatchId());
    assertEquals("${STATE_FOLDER}", meta.getStateDirectory());
  }

  @org.junit.jupiter.api.io.TempDir java.nio.file.Path stateDirectory;

  @Test
  void stateIdentityEditingIsOptionalAndNeverRegeneratesSavedIds() {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDefault();
    String original = meta.getWatchId();
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var shell = bot.shell("Watch Files").activate();
          var dialog = shell.bot();
          display.syncExec(
              () -> {
                var summary = findLabel(shell.widget, "State identifier: Automatic");
                assertTrue(summary.isVisible());
                assertTrue(summary.getToolTipText().contains(original));
              });
          dialog.cTabItem("Advanced").activate();
          display.syncExec(
              () -> assertFalse(findLabel(shell.widget, "State identifier").getVisible()));
          checkboxWithLabel(shell.widget, "Edit state identifier").select();
          assertEquals(original, dialog.textWithLabel("State identifier").getText());
          dialog.textWithLabel("State identifier").setText("manual-input");
          checkboxWithLabel(shell.widget, "Edit state identifier").deselect();
          display.syncExec(
              () -> assertFalse(findLabel(shell.widget, "State identifier").getVisible()));
          dialog.cTabItem("General").activate();
          display.syncExec(
              () -> assertTrue(findLabel(shell.widget, "State identifier: Saved").isVisible()));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals(original, meta.getWatchId());
    assertFalse(meta.isEditWatchId());
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var shell = bot.shell("Watch Files").activate();
          var dialog = shell.bot();
          dialog.cTabItem("Advanced").activate();
          checkboxWithLabel(shell.widget, "Edit state identifier").select();
          dialog.textWithLabel("State identifier").setText("manual-input");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("manual-input", meta.getWatchId());
    assertTrue(meta.isEditWatchId());
    withDialog(
        parent -> new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)).open(),
        bot -> {
          var shell = bot.shell("Watch Files").activate();
          var dialog = shell.bot();
          dialog.cTabItem("Advanced").activate();
          assertEquals("manual-input", dialog.textWithLabel("State identifier").getText());
          checkboxWithLabel(shell.widget, "Edit state identifier").deselect();
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals("manual-input", meta.getWatchId());
    assertFalse(meta.isEditWatchId());
  }

  @Test
  void recoveryTabInspectsBacksUpAndPersistsConfirmedReplay() throws Exception {
    try (JsonFileStateStore store =
        new JsonFileStateStore(stateDirectory, "gui", "root", "scope", 100)) {
      store.save(
          java.util.Map.of(
              "a", new FileState("a", "a.csv", "a.csv", "/", "file", 1, 100),
              "b", new FileState("b", "b.txt", "b.txt", "/", "file", 1, 100)));
    }
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setStateDirectory(stateDirectory.toString());
    meta.setWatchId("gui");
    withDialog(
        parent ->
            new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)) {
              @Override
              protected boolean confirmStateAction(String action) {
                return true;
              }
            }.open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          dialog.cTabItem("Maintenance").activate();
          display.syncExec(
              () -> {
                var shell = bot.shell("Watch Files").widget;
                assertFalse(findReadOnlyText(shell).getVisible());
                assertFalse(findButton(shell, "Clear saved history...").isVisible());
                assertFalse(findButton(shell, "Restore backup...").isVisible());
                assertFalse(findButton(shell, "Update saved-state format...").isVisible());
                assertTrue(findButton(shell, "Reprocess files...").isVisible());
                Button backup = findButton(shell, "Create backup");
                Rectangle visible = displayBounds(backup);
                Composite ancestor = backup.getParent();
                while (ancestor != null) {
                  Rectangle client = ancestor.getClientArea();
                  Point origin = ancestor.toDisplay(client.x, client.y);
                  visible =
                      visible.intersection(
                          new Rectangle(origin.x, origin.y, client.width, client.height));
                  ancestor = ancestor.getParent();
                }
                assertEquals(
                    displayBounds(backup),
                    displayBounds(backup).intersection(visible),
                    "Create backup must fit inside Maintenance at the default window size");
              });
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-maintenance-simple.png"));
          }
          dialog.button("View status").click();
          waitForRecovery(bot, "Observed files: 2");
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-dialog-maintenance.png"));
          }
          org.junit.jupiter.api.Assertions.assertTrue(
              recoveryText(bot.shell("Watch Files").widget).contains("Observed files: 2"));
          dialog.button("Create backup").click();
          waitForRecovery(bot, ".history");
          org.junit.jupiter.api.Assertions.assertTrue(
              recoveryText(bot.shell("Watch Files").widget).contains(".history"));
          dialog.button("Reprocess files...").click();
          var reprocess = bot.shell("Reprocess files").activate().bot();
          reprocess.textWithLabel("Filename pattern").setText(".*\\.csv");
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-reprocess-dialog.png"));
          }
          reprocess.button(buttonLabel("System.Button.OK")).click();
          waitForRecovery(bot, "next start: 1");
          org.junit.jupiter.api.Assertions.assertTrue(
              recoveryText(bot.shell("Watch Files").widget).endsWith("1"));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    try (JsonFileStateStore store =
        new JsonFileStateStore(stateDirectory, "gui", "root", "scope", 100)) {
      assertEquals(
          java.util.Set.of("b"),
          store.load().keySet()); // selected action is durable independently of dialog OK
    }
    assertEquals(
        ".*", meta.getReplayFilter()); // one-time selection leaves pipeline configuration unchanged
  }

  @Test
  void declinedRecoveryDoesNotChangeCheckpoint() throws Exception {
    try (JsonFileStateStore store =
        new JsonFileStateStore(stateDirectory, "gui", "root", "scope", 100)) {
      store.save(java.util.Map.of());
    }
    String original = java.nio.file.Files.readString(stateDirectory.resolve("gui.json"));
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setStateDirectory(stateDirectory.toString());
    meta.setWatchId("gui");
    withDialog(
        parent ->
            new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)) {
              @Override
              protected boolean confirmStateAction(String action) {
                return false;
              }
            }.open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          dialog.cTabItem("Maintenance").activate();
          dialog.checkBox("Advanced recovery").select();
          dialog.button("Clear saved history...").click();
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals(original, java.nio.file.Files.readString(stateDirectory.resolve("gui.json")));
  }

  @Test
  void invalidOrCancelledReprocessingLeavesHistoryAndConfigurationIntact() throws Exception {
    try (JsonFileStateStore store =
        new JsonFileStateStore(stateDirectory, "gui", "root", "scope", 100)) {
      store.save(java.util.Map.of("a", new FileState("a", "a.csv", "a.csv", "/", "file", 1, 100)));
    }
    String original = java.nio.file.Files.readString(stateDirectory.resolve("gui.json"));
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setWatchId("gui");
    meta.setStateDirectory(stateDirectory.toString());
    java.util.concurrent.atomic.AtomicBoolean confirmationCalled =
        new java.util.concurrent.atomic.AtomicBoolean();
    withDialog(
        parent ->
            new WatchFilesDialog(parent, new Variables(), meta, pipeline(meta)) {
              @Override
              protected boolean confirmStateAction(String action) {
                confirmationCalled.set(true);
                return false;
              }
            }.open(),
        bot -> {
          var dialog = bot.shell("Watch Files").activate().bot();
          dialog.cTabItem("Maintenance").activate();
          dialog.button("Reprocess files...").click();
          var reprocess = bot.shell("Reprocess files").activate().bot();
          reprocess.textWithLabel("Filename pattern").setText("*");
          reprocess.button(buttonLabel("System.Button.OK")).click();
          display.syncExec(
              () ->
                  assertTrue(
                      findLabelStartingWith(
                              bot.shell("Reprocess files").widget,
                              "Enter a valid Java regular expression")
                          .isVisible()));
          if (Boolean.getBoolean("watchfiles.gui.capture")) {
            bot.sleep(250);
            display.syncExec(
                () -> SWTUtils.captureScreenshot("target/watchfiles-reprocess-invalid.png"));
          }
          reprocess.textWithLabel("Filename pattern").setText(".*\\.csv");
          reprocess.button(buttonLabel("System.Button.Cancel")).click();
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertFalse(confirmationCalled.get());
    assertEquals(original, java.nio.file.Files.readString(stateDirectory.resolve("gui.json")));
    assertEquals(".*", meta.getReplayFilter());
  }

  private static Button findButton(Composite parent, String text) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button && button.getText().equals(text)) return button;
      if (child instanceof Composite composite) {
        var found = findButton(composite, text);
        if (found != null) return found;
      }
    }
    return null;
  }

  private static org.eclipse.swt.widgets.Label findLabelStartingWith(
      Composite parent, String prefix) {
    for (Control child : parent.getChildren()) {
      if (child instanceof org.eclipse.swt.widgets.Label label
          && label.getText().startsWith(prefix)) return label;
      if (child instanceof Composite composite) {
        var found = findLabelStartingWith(composite, prefix);
        if (found != null) return found;
      }
    }
    return null;
  }

  private void waitForRecovery(org.eclipse.swtbot.swt.finder.SWTBot bot, String expected) {
    bot.waitUntil(
        new org.eclipse.swtbot.swt.finder.waits.DefaultCondition() {
          @Override
          public boolean test() {
            return recoveryText(bot.shell("Watch Files").widget).contains(expected);
          }

          @Override
          public String getFailureMessage() {
            return "Expected "
                + expected
                + "; status="
                + recoveryText(bot.shell("Watch Files").widget);
          }
        });
  }

  private String recoveryText(org.eclipse.swt.widgets.Composite parent) {
    java.util.concurrent.atomic.AtomicReference<String> value =
        new java.util.concurrent.atomic.AtomicReference<>();
    display.syncExec(() -> value.set(findReadOnlyText(parent).getText()));
    return value.get();
  }

  private static org.eclipse.swt.widgets.Text findReadOnlyText(
      org.eclipse.swt.widgets.Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof org.eclipse.swt.widgets.Text text
          && (text.getStyle() & org.eclipse.swt.SWT.READ_ONLY) != 0) return text;
      if (child instanceof Composite composite) {
        var found = findReadOnlyText(composite);
        if (found != null) return found;
      }
    }
    return null;
  }

  private static <T> T find(Composite parent, Class<T> type) {
    for (Control child : parent.getChildren()) {
      if (type.isInstance(child)) {
        return type.cast(child);
      }
      if (child instanceof Composite composite) {
        T found = find(composite, type);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static Rectangle displayBounds(Control control) {
    Rectangle bounds = control.getBounds();
    Point origin = control.getParent().toDisplay(bounds.x, bounds.y);
    return new Rectangle(origin.x, origin.y, bounds.width, bounds.height);
  }
}
