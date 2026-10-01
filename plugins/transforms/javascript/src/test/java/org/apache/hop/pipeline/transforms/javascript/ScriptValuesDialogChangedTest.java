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

package org.apache.hop.pipeline.transforms.javascript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTBotPreferences;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Opening the JavaScript dialog and leaving it must not look like an edit.
 *
 * <p>The script tab uses the shared content editor. Its {@code setText} notifies modify listeners
 * on a later event-loop turn, after the dialog has restored the transform's original changed flag,
 * so Cancel warned that the content had changed (issue #8564).
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. The default reactor run still
 * includes it on a desktop; wrap Maven with {@code tools/with-isolated-display.sh} so the dialog
 * does not steal focus.
 */
@Tag("uitest")
class ScriptValuesDialogChangedTest extends SwtBotTestBase {

  /**
   * The script tab builds a syntax-highlighted editor, which loads its grammar before the dialog
   * opens. That takes longer than SWTBot's 5 second default on a cold run.
   */
  private static final long DIALOG_TIMEOUT_MS = 20_000L;

  private static long defaultTimeout;

  private static final String TRANSFORM_NAME = "javascript";

  private static final String DIALOG_TITLE =
      BaseMessages.getString(ScriptValuesMeta.class, "ScriptValuesDialogMod.Shell.Title");

  private static final String WARNING_TITLE =
      BaseMessages.getString(
          ScriptValuesMeta.class, "ScriptValuesModDialog.WarningDialogChanged.Title");

  @BeforeAll
  static void slowDownSwtBot() {
    defaultTimeout = SWTBotPreferences.TIMEOUT;
    SWTBotPreferences.TIMEOUT = DIALOG_TIMEOUT_MS;
  }

  @AfterAll
  static void restoreSwtBotTimeout() {
    SWTBotPreferences.TIMEOUT = defaultTimeout;
  }

  @Test
  void openingAndCancellingLeavesTheTransformUnchanged() {
    ScriptValuesMeta meta = new ScriptValuesMeta();
    meta.setChanged(false);
    String script = meta.getJsScripts().getFirst().getScript();
    String optimizationLevel = meta.getOptimizationLevel();
    String languageVersion = meta.getLanguageVersion();

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = openedDialog(bot);
          // Read the flag on the UI thread, after any modify notification queued while the dialog
          // was built. syncExec runs behind those notifications.
          display.syncExec(() -> {});
          assertFalse(meta.hasChanged(), "opening the dialog must not mark the transform changed");
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertFalse(meta.hasChanged(), "Cancel must restore the original changed flag");
    assertEquals(script, meta.getJsScripts().getFirst().getScript());
    assertEquals(optimizationLevel, meta.getOptimizationLevel());
    assertEquals(languageVersion, meta.getLanguageVersion());
    assertNoWarningLeftOpen();
  }

  @Test
  void editingTheScriptIsKeptOnOk() {
    ScriptValuesMeta meta = new ScriptValuesMeta();
    meta.setChanged(false);
    String original = meta.getJsScripts().getFirst().getScript();
    String edited = original + "var kept = 1;" + Const.CR;

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = openedDialog(bot);
          IContentEditorWidget editor = scriptEditor(bot.shell(DIALOG_TITLE).widget);
          // setText updates the document immediately and notifies listeners on the next turn.
          display.syncExec(() -> editor.setText(edited));
          display.syncExec(() -> {});
          assertTrue(meta.hasChanged(), "editing the script must mark the transform changed");
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertTrue(meta.hasChanged());
    assertEquals(edited, meta.getJsScripts().getFirst().getScript());
    assertNoWarningLeftOpen();
  }

  /**
   * The shell exists as soon as it is created, and it is not the active shell until it is opened.
   * Wait until it is visible, then drive that shell.
   */
  private SWTBot openedDialog(SWTBot bot) {
    SWTBotShell shell = bot.shell(DIALOG_TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return shell.isOpen();
          }

          @Override
          public String getFailureMessage() {
            return "JavaScript dialog did not open";
          }
        });
    return shell.bot();
  }

  private void assertNoWarningLeftOpen() {
    display.syncExec(
        () -> {
          for (Shell openShell : display.getShells()) {
            if (!openShell.isDisposed()) {
              assertFalse(
                  WARNING_TITLE.equals(openShell.getText()),
                  "the unsaved-changes warning should not be left open");
            }
          }
        });
  }

  /** The script tab stores its editor on the selected item, which SWTBot does not walk. */
  private static IContentEditorWidget scriptEditor(Shell shell) {
    AtomicReference<IContentEditorWidget> editor = new AtomicReference<>();
    display.syncExec(
        () -> {
          CTabFolder folder = findTabFolder(shell);
          assertNotNull(folder, "the dialog should show the script tabs");
          CTabItem item = folder.getSelection();
          assertNotNull(item, "a script tab should be selected");
          editor.set((IContentEditorWidget) item.getData("ContentEditorWidget"));
        });
    assertNotNull(editor.get(), "the selected script tab should hold its editor");
    return editor.get();
  }

  private static CTabFolder findTabFolder(Control control) {
    if (control instanceof CTabFolder folder) {
      return folder;
    }
    if (control instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        CTabFolder found = findTabFolder(child);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private Consumer<Shell> openerFor(ScriptValuesMeta meta) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    return parent -> new ScriptValuesDialog(parent, new Variables(), meta, pipelineMeta).open();
  }

  private static PipelineMeta pipelineWith(ScriptValuesMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "JavaScript transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    meta.setChanged(false);
    return pipelineMeta;
  }
}
