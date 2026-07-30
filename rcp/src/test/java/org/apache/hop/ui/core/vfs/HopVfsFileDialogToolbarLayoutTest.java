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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The toolbars of the Open file dialog have to sit where their layout puts them the moment the
 * dialog appears, without the user resizing the window first.
 *
 * <p>Restoring a remembered window geometry used to break that: the widgets were laid out while the
 * shell still had its pre-open size, so once the window trim (title bar) was subtracted from the
 * client area, everything inside a {@code SashForm} pane ended up shifted off the top of the panel
 * by the trim height - a toolbar clipped to a sliver above the file browser.
 */
@Tag("uitest")
class HopVfsFileDialogToolbarLayoutTest extends SwtBotTestBase {

  private static final int REMEMBERED_WIDTH = 1000;
  private static final int REMEMBERED_HEIGHT = 600;

  @TempDir private Path folder;

  @BeforeAll
  static void registerGuiPlugins() throws Exception {
    // The toolbars are built from @GuiToolbarElement annotations, which only reach the GuiRegistry
    // once the GUI plugin types have been scanned.
    HopGuiEnvironment.init();
  }

  @Test
  void toolbarsAreLaidOutWhenTheDialogRemembersItsGeometry() throws Exception {
    // Mimic a dialog that has been opened before: BaseTransformDialog.setSize() then restores the
    // geometry through WindowProperty instead of packing the shell.
    PropsUi.getInstance()
        .setSessionScreen(
            new WindowProperty(
                openFileTitle(), false, 100, 100, REMEMBERED_WIDTH, REMEMBERED_HEIGHT));

    List<String> problems = openDialogAndCheckToolbars();

    assertTrue(problems.isEmpty(), "Toolbars are misplaced on open: " + problems);
  }

  @Test
  void toolbarsAreLaidOutWhenTheDialogHasNoRememberedGeometry() throws Exception {
    List<String> problems = openDialogAndCheckToolbars();

    assertTrue(problems.isEmpty(), "Toolbars are misplaced on open: " + problems);
  }

  /**
   * Opens the real dialog and, without touching the window, reports every toolbar that is not fully
   * inside its parent's client area.
   */
  private List<String> openDialogAndCheckToolbars() throws IOException {
    createPipelineFile("one.hpl");

    AtomicReference<List<String>> problems = new AtomicReference<>();
    withDialog(
        parent -> presentDialog(parent),
        bot -> {
          SWTBotShell shell = bot.shell(openFileTitle());
          Shell swtShell = shell.widget;
          problems.set(collectMisplacedToolbars(swtShell));
          shell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });
    return problems.get();
  }

  private List<String> collectMisplacedToolbars(Shell shell) {
    AtomicReference<List<String>> result = new AtomicReference<>();
    shell.getDisplay().syncExec(() -> result.set(checkToolbars(shell)));
    return result.get();
  }

  private List<String> checkToolbars(Shell shell) {
    List<String> problems = new ArrayList<>();
    List<ToolBar> toolBars = new ArrayList<>();
    collectToolBars(shell, toolBars);
    assertFalse(toolBars.isEmpty(), "The dialog should have toolbars to check");

    for (ToolBar toolBar : toolBars) {
      Rectangle bounds = toolBar.getBounds();
      Rectangle parentArea = toolBar.getParent().getClientArea();
      if (bounds.y < 0 || bounds.y + bounds.height > parentArea.height) {
        problems.add(
            String.format(
                "toolbar with %d item(s) at %s does not fit its parent's client area %s",
                toolBar.getItemCount(), bounds, parentArea));
      }
      // A toolbar sitting in the right place is useless if its items are clipped out of it.
      for (var item : toolBar.getItems()) {
        Rectangle itemBounds = item.getBounds();
        if (itemBounds.y < 0 || itemBounds.y + itemBounds.height > bounds.height) {
          problems.add(
              String.format("item at %s is clipped by its toolbar %s", itemBounds, bounds));
        }
      }
    }
    return problems;
  }

  /**
   * Every toolbar row also has to keep its distance from the widget below it, which is what the
   * clipped-toolbar screenshot really shows.
   */
  @Test
  void theFileBrowserStartsBelowItsToolbar() throws Exception {
    PropsUi.getInstance()
        .setSessionScreen(
            new WindowProperty(
                openFileTitle(), false, 100, 100, REMEMBERED_WIDTH, REMEMBERED_HEIGHT));
    createPipelineFile("one.hpl");

    AtomicReference<Integer> toolbarTop = new AtomicReference<>();
    withDialog(
        parent -> presentDialog(parent),
        bot -> {
          SWTBotShell shell = bot.shell(openFileTitle());
          Shell swtShell = shell.widget;
          swtShell
              .getDisplay()
              .syncExec(
                  () -> {
                    List<ToolBar> toolBars = new ArrayList<>();
                    collectToolBars(swtShell, toolBars);
                    // The browser toolbar is the one sharing its parent with the search field.
                    int top =
                        toolBars.stream()
                            .mapToInt(toolBar -> toolBar.getBounds().y)
                            .min()
                            .orElse(Integer.MIN_VALUE);
                    toolbarTop.set(top);
                  });
          shell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(0, toolbarTop.get(), "No toolbar should be pushed above the top of its panel");
  }

  private void presentDialog(Shell parent) {
    try {
      BaseDialog.presentFileDialog(
          false,
          parent,
          null,
          new Variables(),
          HopVfs.getFileObject(folder.toString()),
          new String[] {"*.hpl"},
          new String[] {"Pipelines"},
          false);
    } catch (Exception e) {
      throw new IllegalStateException("Error opening the file dialog", e);
    }
  }

  private void collectToolBars(Composite parent, List<ToolBar> into) {
    for (Control child : parent.getChildren()) {
      if (child instanceof ToolBar toolBar) {
        into.add(toolBar);
      }
      if (child instanceof Composite composite) {
        collectToolBars(composite, into);
      }
    }
  }

  private static String openFileTitle() {
    return BaseMessages.getString(BaseDialog.class, "BaseDialog.OpenFile");
  }

  private void createPipelineFile(String name) throws IOException {
    Files.writeString(folder.resolve(name), "<pipeline/>");
  }
}
