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

package org.apache.hop.ui.hopgui.perspective.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiAuditDelegate;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.ExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Closing the tab of a file with unsaved changes asks the user what to do. Answering Cancel means
 * "don't close": the tab has to stay exactly where it was.
 *
 * <p>Issue #8079 - the explorer perspective vetoed the close event but disposed the tab itself
 * right before, so the file closed anyway whatever the user answered.
 *
 * <p>The whole chain is exercised for real: the close button of a real {@link CTabFolder} is
 * clicked, SWT fires its close event, {@link ExplorerPerspective#closeTab} runs, the file type
 * handler puts up the real save prompt, and SWT decides on the event's {@code doit} whether to
 * dispose the tab.
 */
@Tag("uitest")
class ExplorerPerspectiveTabCloseTest extends SwtBotTestBase {

  private static final String CHANGED_FILE = "changed.txt";
  private static final String UNCHANGED_FILE = "unchanged.txt";

  /** Title of the prompt {@code BaseExplorerFileTypeHandler#isCloseable} puts up. */
  private static final String SAVE_PROMPT = "Save file?";

  /** How long to wait for the save prompt to appear or disappear. */
  private static final int POLL_ATTEMPTS = 100;

  private static final int POLL_MILLIS = 50;

  private final AtomicReference<CTabFolder> folder = new AtomicReference<>();
  private final AtomicReference<CTabItem> changedTab = new AtomicReference<>();

  @Test
  void cancellingTheSavePromptKeepsTheFileOpen() {
    withScene(
        this::twoOpenFiles,
        bot ->
            closeTabAndAnswer(
                bot,
                buttonLabel("System.Button.Cancel"),
                () -> {
                  assertFalse(
                      onUi(() -> changedTab.get().isDisposed()),
                      "Cancel means 'do not close': the file has to stay open (issue #8079)");
                  assertEquals(
                      2,
                      onUi(() -> folder.get().getItemCount()),
                      "no tab may be closed after Cancel");
                }));
  }

  /** The counterpart: answering the prompt with No does close the file, without saving it. */
  @Test
  void answeringNoClosesTheFileWithoutSaving() {
    withScene(
        this::twoOpenFiles,
        bot ->
            closeTabAndAnswer(
                bot,
                buttonLabel("System.Button.No"),
                () -> {
                  assertTrue(
                      onUi(() -> changedTab.get().isDisposed()),
                      "No means: close the file without saving");
                  assertEquals(
                      1,
                      onUi(() -> folder.get().getItemCount()),
                      "only the closed tab may disappear");
                }));
  }

  // ---------------------------------------------------------------- the scene

  /**
   * Two open files in one editor pane of the explorer perspective, the first one with unsaved
   * changes. Everything the close path touches is the real thing; only the surrounding application
   * chrome - the file tree, the toolbars, the audit log - is left out, as it is built when the Hop
   * GUI shell opens, which a test never does.
   */
  private void twoOpenFiles(Shell shell) {
    shell.setLayout(new FillLayout());

    HopGui hopGui = testHopGui(shell);
    ExplorerPerspective perspective = new ExplorerPerspective();
    setField(perspective, "hopGui", hopGui);

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER | SWT.CLOSE);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            perspective.closeTab(event, (CTabItem) event.item);
          }
        });
    // The perspective finds its open tabs through the editor layout tree; a single pane is that
    // one tab folder.
    setField(perspective, "editorRoot", tabFolder);

    changedTab.set(openFile(perspective, hopGui, tabFolder, CHANGED_FILE, true));
    openFile(perspective, hopGui, tabFolder, UNCHANGED_FILE, false);

    // Only the selected tab shows a close button, and that is the one to click.
    tabFolder.setSelection(changedTab.get());
    folder.set(tabFolder);
    shell.layout(true, true);
  }

  /** Opens one file in a tab, exactly the way the perspective registers a file it opened. */
  private CTabItem openFile(
      ExplorerPerspective perspective,
      HopGui hopGui,
      CTabFolder tabFolder,
      String name,
      boolean changed) {
    ExplorerFile explorerFile = new ExplorerFile(name, name, new ExplorerFileType());
    TestFileTypeHandler handler = new TestFileTypeHandler(hopGui, perspective, explorerFile);
    if (changed) {
      handler.setChanged();
    }

    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setText(name);
    tabItem.setData(handler);
    perspectiveItems(perspective).add(new TabItemHandler(tabItem, handler));
    return tabItem;
  }

  /**
   * A Hop GUI with a real set of delegates, hung off the test shell so the save prompt has a parent
   * to open on. Writing the list of open files is the one thing stubbed out: it needs the
   * perspective manager of a fully opened application and says nothing about closing a tab.
   */
  private HopGui testHopGui(Shell shell) {
    HopGui hopGui = new TestHopGui();
    hopGui.getShell().dispose();
    hopGui.setShell(shell);
    hopGui.setProps(PropsUi.getInstance());
    hopGui.auditDelegate =
        new HopGuiAuditDelegate(hopGui) {
          @Override
          public void writeLastOpenFiles() {
            // not what this test is about
          }
        };
    return hopGui;
  }

  // ---------------------------------------------------------------- interactions

  /**
   * Clicks the close button of the changed file's tab, answers the save prompt that comes up, and
   * only then runs the assertions.
   *
   * <p>The prompt runs its own event loop on the UI thread, so this worker gets control back while
   * it is on screen - and the UI thread stays inside that loop until the prompt is answered.
   * Whatever happens, the prompt is dismissed at the end: a worker walking away from an open prompt
   * would leave the UI thread parked in that loop forever.
   */
  private void closeTabAndAnswer(SWTBot bot, String answer, Runnable assertions) {
    try {
      bot.cTabItem(CHANGED_FILE).close();

      Shell prompt = awaitSavePrompt();
      new SWTBot(prompt).button(answer).click();
      waitUntilDisposed(prompt);

      assertions.run();
    } finally {
      dismissAnyOpenPrompt();
    }
  }

  /** The save prompt, once the close path has put it up. */
  private Shell awaitSavePrompt() {
    for (int attempt = 0; attempt < POLL_ATTEMPTS; attempt++) {
      Shell prompt = onUi(() -> shellTitled(SAVE_PROMPT));
      if (prompt != null) {
        return prompt;
      }
      pause();
    }
    throw new AssertionError(
        "closing the tab of a changed file should ask whether to save it, but only these windows "
            + "are open: "
            + onUi(this::shellTitles));
  }

  private void waitUntilDisposed(Shell prompt) {
    for (int attempt = 0; attempt < POLL_ATTEMPTS && !onUi(prompt::isDisposed); attempt++) {
      pause();
    }
    assertTrue(onUi(prompt::isDisposed), "the answered save prompt should have closed");
  }

  /** Hands the UI thread back its event loop, however the interactions ended. */
  private void dismissAnyOpenPrompt() {
    display.asyncExec(
        () -> {
          Shell prompt = shellTitled(SAVE_PROMPT);
          if (prompt != null) {
            prompt.close();
          }
        });
    display.wake();
  }

  private Shell shellTitled(String title) {
    for (Shell open : display.getShells()) {
      if (!open.isDisposed() && title.equals(open.getText())) {
        return open;
      }
    }
    return null;
  }

  private List<String> shellTitles() {
    List<String> titles = new ArrayList<>();
    for (Shell open : display.getShells()) {
      if (!open.isDisposed()) {
        titles.add("'" + open.getText() + "'");
      }
    }
    return titles;
  }

  private static void pause() {
    try {
      Thread.sleep(POLL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for the save prompt", e);
    }
  }

  // ---------------------------------------------------------------- test doubles

  /** The Hop GUI constructor is protected; a test builds one through a subclass. */
  private static class TestHopGui extends HopGui {
    // no additions: the delegates the close path uses are the real ones
  }

  /**
   * The real explorer file handler - the save prompt and its Yes/No/Cancel answers come from {@link
   * BaseExplorerFileTypeHandler} unchanged. Only rendering the file is left out: a tab does not
   * need contents to be closed.
   */
  private static class TestFileTypeHandler extends BaseExplorerFileTypeHandler {
    TestFileTypeHandler(HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
      super(hopGui, perspective, explorerFile);
    }

    @Override
    public void renderFile(Composite composite) {
      // nothing to render
    }
  }

  // ---------------------------------------------------------------- UI thread

  /** Runs {@code supplier} on the UI thread and hands its result back to the SWTBot worker. */
  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    return result.get();
  }

  // ---------------------------------------------------------------- reflection

  @SuppressWarnings("unchecked")
  private static List<TabItemHandler> perspectiveItems(ExplorerPerspective perspective) {
    return (List<TabItemHandler>) readField(perspective, "items");
  }

  private static Object readField(Object target, String name) {
    try {
      Field field = field(target, name);
      return field.get(target);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Could not read " + name, e);
    }
  }

  private static void setField(Object target, String name, Object value) {
    try {
      field(target, name).set(target, value);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Could not set " + name, e);
    }
  }

  private static Field field(Object target, String name) {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException e) {
        // keep walking up the hierarchy
      }
    }
    throw new AssertionError("No field '" + name + "' on " + target.getClass());
  }
}
