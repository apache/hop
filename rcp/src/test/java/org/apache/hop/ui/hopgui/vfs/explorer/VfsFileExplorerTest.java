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

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives a real VFS File Explorer through the paths that change files: a paste onto an existing
 * file, and deleting the folder that is being shown.
 */
@Tag("uitest")
class VfsFileExplorerTest extends SwtBotTestBase {

  private static final long WAIT_MILLIS = 20_000;

  @TempDir private Path folder;

  @Test
  void pasteOntoAnExistingFileAsksAndSkipKeepsTheOriginal() throws Exception {
    Path source = Files.createDirectory(folder.resolve("source"));
    Path target = Files.createDirectory(folder.resolve("target"));
    Files.writeString(source.resolve("data.csv"), "new");
    Files.writeString(target.resolve("data.csv"), "old");

    AtomicReference<VfsFileExplorer> explorer = new AtomicReference<>();
    withScene(
        shell -> explorer.set(createExplorer(shell, source)),
        bot -> {
          VfsFileExplorerLocation location = onUi(() -> explorer.get().activeLocation());
          waitForRows(location, 1);
          onUi(
              () -> {
                location.fileTable().setSelection(0);
                location.copySelectedFiles();
                location.navigateTo(target.toString(), true);
                return null;
              });
          waitUntilIdle(explorer.get());

          display.asyncExec(location::pasteIntoFileList);
          answerConflict(bot, "VfsFileExplorer.Conflict.Skip");
          waitUntilIdle(explorer.get());
          assertEquals("old", read(target.resolve("data.csv")));

          display.asyncExec(location::pasteIntoFileList);
          answerConflict(bot, "VfsFileExplorer.Conflict.Replace");
          waitUntilIdle(explorer.get());
          assertEquals("new", read(target.resolve("data.csv")));
          assertEquals("new", read(source.resolve("data.csv")));
        });

    try (var files = Files.list(target)) {
      assertEquals(1, files.count(), "No temporary file is left behind");
    }
  }

  @Test
  void deletingTheShownFolderOpensItsParent() throws Exception {
    Path shown = Files.createDirectory(folder.resolve("shown"));
    Files.writeString(shown.resolve("notes.txt"), "x", StandardCharsets.UTF_8);

    AtomicReference<VfsFileExplorer> explorer = new AtomicReference<>();
    withScene(
        shell -> explorer.set(createExplorer(shell, shown)),
        bot -> {
          VfsFileExplorerLocation location = onUi(() -> explorer.get().activeLocation());
          waitForRows(location, 1);
          onUi(
              () -> {
                location.folderTree().setSelection(location.folderTree().getItem(0));
                return null;
              });

          display.asyncExec(location::deleteSelectedFolders);
          SWTBotShell confirm =
              bot.shell(
                  BaseMessages.getString(VfsFileExplorer.PKG, "VfsFileExplorer.Delete.Title"));
          confirm.bot().button(BaseMessages.getString("System.Button.Yes")).click();
          waitUntilIdle(explorer.get());
          waitFor(
              () ->
                  onUi(() -> VfsLocations.sameLocation(location.shownFolder(), folder.toString())),
              "the explorer to show the parent folder");
        });

    assertFalse(Files.exists(shown));
  }

  private static VfsFileExplorer createExplorer(
      org.eclipse.swt.widgets.Shell shell, Path location) {
    shell.setLayout(new FormLayout());
    shell.setSize(1000, 700);
    VfsFileExplorer explorer = new VfsFileExplorer(shell, null);
    explorer.setLayoutData(new FormDataBuilder().fullSize().result());
    explorer.openLocation(location.toString());
    return explorer;
  }

  private static void answerConflict(SWTBot bot, String buttonKey) {
    SWTBotShell dialog =
        bot.shell(BaseMessages.getString(VfsFileExplorer.PKG, "VfsFileExplorer.Conflict.Title"));
    dialog.bot().button(BaseMessages.getString(VfsFileExplorer.PKG, buttonKey)).click();
  }

  private static void waitForRows(VfsFileExplorerLocation location, int rows) {
    waitFor(
        () -> {
          Table table = onUi(location::fileTable);
          return onUi(table::getItemCount) >= rows;
        },
        rows + " listed rows");
  }

  /** Every listing and file change has finished. */
  private static void waitUntilIdle(VfsFileExplorer explorer) {
    waitFor(() -> onUi(explorer::runningOperations) == 0, "the explorer operations to finish");
    // A finished change lists the folder again from the UI thread.
    onUi(() -> null);
    waitFor(() -> onUi(explorer::runningOperations) == 0, "the folder to be listed again");
  }

  private static void waitFor(Supplier<Boolean> condition, String what) {
    long deadline = System.currentTimeMillis() + WAIT_MILLIS;
    while (!condition.get()) {
      assertTrue(System.currentTimeMillis() < deadline, "Timed out waiting for " + what);
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private static String read(Path file) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    display.syncExec(() -> result.set(supplier.get()));
    return result.get();
  }
}
