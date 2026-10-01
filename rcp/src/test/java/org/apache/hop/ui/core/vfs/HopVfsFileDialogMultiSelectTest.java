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

package org.apache.hop.ui.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTreeItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives the real Open file dialog (the Hop VFS browser) to verify that more than one file can be
 * picked in one go, which is what File/Open in the Hop GUI relies on.
 */
@Tag("uitest")
class HopVfsFileDialogMultiSelectTest extends SwtBotTestBase {

  private static final String[] FILTER_EXTENSIONS = {"*.hpl"};
  private static final String[] FILTER_NAMES = {"Pipelines"};

  @TempDir private Path folder;

  @Test
  void multipleSelectedFilesAreAllReturned() throws Exception {
    createPipelineFile("first.hpl");
    createPipelineFile("second.hpl");
    createPipelineFile("third.hpl");

    List<String> listedFiles = new ArrayList<>();
    String[] filenames =
        openFilesInDialog(
            bot -> {
              selectFiles(bot, "first.hpl", "third.hpl");
              listedFiles.addAll(listedFiles(bot));
            });

    // The files come back in the order the browser lists them, whatever the active sort order is.
    //
    List<String> expected =
        listedFiles.stream().filter(name -> !"second.hpl".equals(name)).toList();
    assertEquals(2, expected.size(), "The browser should list the three pipeline files");
    assertEquals(2, filenames.length, "Both selected files should be returned");
    assertTrue(
        filenames[0].endsWith(expected.get(0)), "Unexpected first selected file: " + filenames[0]);
    assertTrue(
        filenames[1].endsWith(expected.get(1)), "Unexpected second selected file: " + filenames[1]);
  }

  @Test
  void selectingOneFileStillReturnsThatFile() throws Exception {
    createPipelineFile("only.hpl");
    createPipelineFile("other.hpl");

    String[] filenames = openFilesInDialog(bot -> selectFiles(bot, "other.hpl"));

    assertEquals(1, filenames.length, "A single selection should return exactly one file");
    assertTrue(filenames[0].endsWith("other.hpl"), "Unexpected selected file: " + filenames[0]);
  }

  @Test
  void singleFileDialogKeepsReturningOneFilename() throws Exception {
    createPipelineFile("single.hpl");

    // The dialog every other caller in Hop uses: one file, one filename.
    //
    AtomicReference<String> selected = new AtomicReference<>();
    withDialog(
        parent -> selected.set(presentSingleFileDialog(parent)),
        bot -> {
          SWTBotShell shell = openFileShell(bot);
          selectFiles(bot, "single.hpl");
          shell.bot().button(buttonLabel("System.Button.Open")).click();
        });

    assertNotNull(selected.get(), "The single file dialog should return the selected file");
    assertTrue(selected.get().endsWith("single.hpl"), "Unexpected file: " + selected.get());
  }

  @Test
  void foldersInTheSelectionAreIgnored() throws Exception {
    Files.createDirectory(folder.resolve("subfolder"));
    createPipelineFile("kept.hpl");

    String[] filenames = openFilesInDialog(bot -> selectFiles(bot, "subfolder", "kept.hpl"));

    assertEquals(1, filenames.length, "Only the file should be returned, not the folder");
    assertTrue(filenames[0].endsWith("kept.hpl"), "Unexpected selected file: " + filenames[0]);
  }

  @Test
  void cancellingTheDialogSelectsNothing() throws Exception {
    createPipelineFile("untouched.hpl");

    AtomicReference<String[]> selected = new AtomicReference<>();
    withDialog(
        parent -> selected.set(presentDialog(parent)),
        bot -> {
          SWTBotShell shell = openFileShell(bot);
          selectFiles(bot, "untouched.hpl");
          shell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(0, selected.get().length, "A cancelled dialog should not select anything");
  }

  /**
   * Opens the file dialog on the temporary folder, lets the caller pick files in it and returns
   * what the dialog reports back.
   */
  private String[] openFilesInDialog(Consumer<SWTBot> selection) {
    AtomicReference<String[]> selected = new AtomicReference<>();
    withDialog(
        parent -> selected.set(presentDialog(parent)),
        bot -> {
          SWTBotShell shell = openFileShell(bot);
          selection.accept(bot);
          shell.bot().button(buttonLabel("System.Button.Open")).click();
        });
    return selected.get();
  }

  private String[] presentDialog(Shell parent) {
    try {
      return BaseDialog.presentMultiFileDialog(
          parent,
          new Variables(),
          HopVfs.getFileObject(folder.toString()),
          FILTER_EXTENSIONS,
          FILTER_NAMES,
          false);
    } catch (Exception e) {
      throw new IllegalStateException("Error opening the file dialog", e);
    }
  }

  private String presentSingleFileDialog(Shell parent) {
    try {
      return BaseDialog.presentFileDialog(
          false,
          parent,
          null,
          new Variables(),
          HopVfs.getFileObject(folder.toString()),
          FILTER_EXTENSIONS,
          FILTER_NAMES,
          false);
    } catch (Exception e) {
      throw new IllegalStateException("Error opening the file dialog", e);
    }
  }

  private SWTBotShell openFileShell(SWTBot bot) {
    SWTBotShell shell = bot.shell(BaseMessages.getString(BaseDialog.class, "BaseDialog.OpenFile"));
    shell.activate();
    return shell;
  }

  /** Selects the given files in the browser tree of the open file dialog. */
  private void selectFiles(SWTBot bot, String... names) {
    SWTBotTreeItem folderItem = folderItem(bot);
    folderItem.expand();
    folderItem.select(names);
  }

  /** The files as the browser lists them, in the order they are shown. */
  private List<String> listedFiles(SWTBot bot) {
    return folderItem(bot).getNodes();
  }

  private SWTBotTreeItem folderItem(SWTBot bot) {
    return bot.tree().getTreeItem(folder.getFileName().toString());
  }

  private void createPipelineFile(String name) throws IOException {
    Files.writeString(folder.resolve(name), "<pipeline/>");
  }
}
