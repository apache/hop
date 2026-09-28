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

package org.apache.hop.imports.kettle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SWTBot coverage for the two front-end parity fixes of issue #8516: the import dialog now imports
 * sub-folders by default (like {@code hop-import --skip-folders}) and can opt out of naming schemes
 * (like {@code hop-import --no-apply-naming-schemes}).
 *
 * <p>The dialog runs its own blocking event loop in {@code open()}, so {@link
 * SwtBotTestBase#withDialog} pumps it on the UI thread while the assertions run on a worker thread.
 * Nothing is ever imported: the dialog is closed, never confirmed.
 *
 * <p>Tagged {@code uitest} so it is skipped without a display. On a desktop wrap Maven with {@code
 * tools/with-isolated-display.sh} so the dialog does not steal focus.
 */
@Tag("uitest")
class KettleImportDialogLayoutTest extends SwtBotTestBase {

  private static final Class<?> PKG = KettleImportDialog.class;

  private static final String SKIP_FOLDERS = "KettleImportDialog.Label.SkipFolders";
  private static final String APPLY_NAMING_SCHEMES = "KettleImportDialog.Label.ApplyNamingSchemes";
  private static final String NAMING_SCHEME = "KettleImportDialog.NamingScheme.Label";
  private static final String PIPELINE_RUN_CONFIGURATION =
      "KettleImportDialog.Pipeline.RunConfiguration.Label";
  private static final String WORKFLOW_RUN_CONFIGURATION =
      "KettleImportDialog.Workflow.RunConfiguration.Label";

  /**
   * The CLI has always recursed into sub-folders while the dialog checkbox came up selected, so the
   * same source tree imported differently depending on which front-end launched it.
   */
  @Test
  void subFoldersAreImportedByDefault() {
    List<Boolean> skipping = new ArrayList<>();

    inDialog(shell -> skipping.add(checkBox(shell, SKIP_FOLDERS).getSelection()));

    assertFalse(
        skipping.get(0),
        "sub-folders must be imported by default, like 'hop-import --skip-folders'");
  }

  /** {@code --no-apply-naming-schemes} had no expression in the dialog at all. */
  @Test
  void turningNamingSchemesOffDisablesTheSchemeSelector() {
    List<Boolean> states = new ArrayList<>();

    inDialog(
        shell -> {
          Button apply = checkBox(shell, APPLY_NAMING_SCHEMES);
          MetaSelectionLine<?> scheme = metaSelectionLine(shell, NAMING_SCHEME);
          assertNotNull(scheme, "the naming-scheme selector is missing from the Metadata tab");

          states.add(apply.getSelection());
          states.add(scheme.getComboWidget().isEnabled());

          // setSelection() does not fire the listener, so drive it the way a click would.
          apply.setSelection(false);
          apply.notifyListeners(SWT.Selection, null);
          states.add(scheme.getComboWidget().isEnabled());
        });

    assertTrue(states.get(0), "naming schemes must stay on by default");
    assertTrue(states.get(1), "the scheme selector is usable while schemes are applied");
    assertFalse(states.get(2), "turning naming schemes off must disable the scheme selector");
  }

  /**
   * The Metadata tab rows are stacked with FormAttachments. Inserting the opt-out checkbox between
   * the workflow run configuration and the scheme selector means attaching both to their new
   * neighbour; attaching to the old one draws the rows on top of each other.
   */
  @Test
  void theMetadataTabRowsAreStackedWithoutOverlapping() {
    List<Rectangle> rows = new ArrayList<>();

    inDialog(
        shell -> {
          rows.add(displayBounds(metaSelectionLine(shell, PIPELINE_RUN_CONFIGURATION)));
          rows.add(displayBounds(metaSelectionLine(shell, WORKFLOW_RUN_CONFIGURATION)));
          rows.add(displayBounds(checkBox(shell, APPLY_NAMING_SCHEMES)));
          rows.add(displayBounds(metaSelectionLine(shell, NAMING_SCHEME)));
        });

    for (Rectangle row : rows) {
      assertTrue(row.width > 0 && row.height > 0, "a Metadata tab row was never laid out");
    }
    for (int i = 1; i < rows.size(); i++) {
      assertFalse(
          rows.get(i).intersects(rows.get(i - 1)),
          "Metadata tab row " + i + " overlaps the row above it");
      assertTrue(
          rows.get(i - 1).y < rows.get(i).y,
          "Metadata tab row " + i + " is not below the row above it");
    }
  }

  /**
   * Opens the real import dialog, runs {@code assertions} against its widget tree on the UI thread,
   * then closes it. Every tab is laid out up front so the Metadata tab has real bounds.
   */
  private void inDialog(Consumer<Shell> assertions) {
    withDialog(
        parent -> {
          try {
            new KettleImportDialog(parent, new Variables(), new KettleImport()).open();
          } catch (HopException e) {
            throw new IllegalStateException("Unable to open the Kettle import dialog", e);
          }
        },
        bot -> {
          SWTBotShell dialog = bot.shell(shellTitle());
          dialog.activate();
          display.syncExec(
              () -> {
                Shell shell = dialog.widget;
                layoutEveryTab(shell);
                assertions.accept(shell);
              });
          dialog.close();
        });
  }

  /**
   * A CTabFolder only lays out the selected tab, so select each one before measuring. Leaves the
   * Metadata tab showing, which is where the assertions look.
   */
  private static void layoutEveryTab(Shell shell) {
    shell.layout(true, true);
    for (Control control : shell.getChildren()) {
      if (control instanceof org.eclipse.swt.custom.CTabFolder folder) {
        for (int i = 0; i < folder.getItemCount(); i++) {
          folder.setSelection(i);
          shell.layout(true, true);
        }
      }
    }
  }

  /**
   * The checkbox of the row labelled {@code labelKey}: the Button created right after its Label.
   */
  private static Button checkBox(Composite parent, String labelKey) {
    String text = message(labelKey);
    Control[] children = parent.getChildren();
    for (int i = 0; i < children.length; i++) {
      if (children[i] instanceof Label label
          && text.equals(label.getText())
          && i + 1 < children.length
          && children[i + 1] instanceof Button button
          && (button.getStyle() & SWT.CHECK) != 0) {
        return button;
      }
      if (children[i] instanceof Composite composite && !(composite instanceof MetaSelectionLine)) {
        Button found = checkBox(composite, labelKey);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** The {@link MetaSelectionLine} carrying {@code labelKey}, or null when its plugin is absent. */
  private static MetaSelectionLine<?> metaSelectionLine(Composite parent, String labelKey) {
    String text = message(labelKey);
    for (Control child : parent.getChildren()) {
      if (child instanceof MetaSelectionLine<?> line
          && text.equals(line.getLabelWidget().getText())) {
        return line;
      }
      if (child instanceof Composite composite) {
        MetaSelectionLine<?> found = metaSelectionLine(composite, labelKey);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /** A control's position in display coordinates, so rows in different parents are comparable. */
  private static Rectangle displayBounds(Control control) {
    assertNotNull(control, "a Metadata tab row is missing from the dialog");
    Rectangle bounds = control.getBounds();
    Point origin = control.getParent().toDisplay(bounds.x, bounds.y);
    return new Rectangle(origin.x, origin.y, bounds.width, bounds.height);
  }

  private static String shellTitle() {
    return message("KettleImportDialog.Shell.Name");
  }

  private static String message(String key) {
    return BaseMessages.getString(PKG, key);
  }
}
