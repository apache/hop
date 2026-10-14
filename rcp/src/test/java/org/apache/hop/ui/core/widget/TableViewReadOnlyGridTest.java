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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolItem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A read-only grid ({@link TableView#setReadonly(boolean)}): a click selects the row, a
 * double-click opens the cell as a viewer that never writes back, and the selection and copy
 * actions work.
 *
 * <p>Clicks and menu opens are sent straight to the table's listeners. That is also what proves the
 * grid does its own bookkeeping, such as refreshing the toolbar, without counting on a native
 * {@link SWT#Selection} that platforms do not reliably send after the row was selected in code.
 */
@Tag("uitest")
class TableViewReadOnlyGridTest extends SwtBotTestBase {

  /**
   * The grid's toolbar items are its own {@code @GuiToolbarElement} methods, which the Hop GUI
   * registers at startup. Register them the same way here so the grid gets its toolbar.
   */
  @BeforeAll
  static void registerTableViewToolbar() {
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Method method : TableView.class.getDeclaredMethods()) {
      GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
      if (element != null && registry.findGuiToolbarItem(element.root(), element.id()) == null) {
        registry.addGuiToolbarElement(
            TableView.class.getName(), element, method, TableView.class.getClassLoader());
      }
    }
  }

  @Test
  void clickSelectsTheRowAndEnablesCopyWithoutOpeningAnEditor() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> viewRef.set(readOnlyGrid(shell, plainColumns(), null)),
        bot -> {
          waitForLayout(viewRef.get());
          Object[] state =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    ToolItem copy =
                        view.getToolbarWidgets().findToolItem(TableView.ID_TOOLBAR_COPY_SELECTED);
                    assertNotNull(copy, "the Copy toolbar item");
                    boolean copyEnabledBefore = copy.isEnabled();
                    sendMouse(view, SWT.MouseDown, 1, 1);
                    return new Object[] {
                      copyEnabledBefore,
                      view.table.getSelectionIndex(),
                      copy.isEnabled(),
                      findText(view.table)
                    };
                  });
          assertFalse((Boolean) state[0], "Copy starts disabled");
          assertEquals(1, state[1]);
          assertTrue((Boolean) state[2], "Copy is enabled once the click selected a row");
          assertNull(state[3], "a single click opens no editor");
        });
  }

  @Test
  void doubleClickOpensAViewerThatNeverWritesBack() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    AtomicInteger modifications = new AtomicInteger();
    withScene(
        shell ->
            viewRef.set(readOnlyGrid(shell, plainColumns(), e -> modifications.incrementAndGet())),
        bot -> {
          waitForLayout(viewRef.get());
          Object[] state =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    doubleClick(view, 0, 2);
                    Text viewer = findText(view.table);
                    assertNotNull(viewer, "a double-click opens the cell");
                    boolean readOnlyStyle = (viewer.getStyle() & SWT.READ_ONLY) != 0;
                    String shown = viewer.getText();
                    // Even text put into the viewer in code must not reach the cell.
                    viewer.setText("changed");
                    // Clicking another cell closes the viewer the way any open editor is closed.
                    sendMouse(view, SWT.MouseDown, 1, 1);
                    return new Object[] {
                      readOnlyStyle, shown, view.table.getItem(0).getText(2), findText(view.table)
                    };
                  });
          assertTrue((Boolean) state[0], "the viewer can't be typed in");
          assertEquals("b0", state[1]);
          assertEquals("b0", state[2], "the cell keeps its value");
          assertNull(state[3], "the viewer is gone");
          assertEquals(0, modifications.get(), "nothing reported a change");
        });
  }

  @Test
  void doubleClickIsLeftToAnOwnerThatOpensTheRow() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> {
          TableView view = readOnlyGrid(shell, plainColumns(), null);
          // An owner that picks or opens the double-clicked row, like the pipeline grid does.
          view.table.addListener(SWT.DefaultSelection, e -> {});
          viewRef.set(view);
        },
        bot -> {
          waitForLayout(viewRef.get());
          Text viewer =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    doubleClick(view, 0, 1);
                    return findText(view.table);
                  });
          assertNull(viewer, "the owner picks or opens the row, the grid opens no cell");
        });
  }

  @Test
  void selectionAdapterColumnOpensTheViewerInsteadOfTheAdapter() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    AtomicInteger adapterCalls = new AtomicInteger();
    String formula = "IF([amount] > 1000; \"large\"; IF([amount] > 100; \"medium\"; \"small\"))";
    withScene(
        shell -> {
          ColumnInfo[] columns = plainColumns();
          columns[1].setSelectionAdapter(
              new SelectionAdapter() {
                @Override
                public void widgetSelected(SelectionEvent e) {
                  adapterCalls.incrementAndGet();
                }
              });
          TableView view = readOnlyGrid(shell, columns, null);
          view.table.getItem(0).setText(2, formula);
          viewRef.set(view);
        },
        bot -> {
          waitForLayout(viewRef.get());
          Object[] state =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    doubleClick(view, 0, 2);
                    Text viewer = findText(view.table);
                    return new Object[] {
                      viewer == null ? null : viewer.getText(),
                      viewer != null && (viewer.getStyle() & SWT.READ_ONLY) != 0
                    };
                  });
          assertEquals(0, adapterCalls.get(), "the adapter edits the cell, so it is not run");
          assertEquals(formula, state[0], "the full value is shown instead");
          assertTrue((Boolean) state[1]);
        });
  }

  @Test
  void copyCellValueCopiesTheStoredValueAndNothingForANull() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    String multiLine = "first line\nsecond line";
    withScene(
        shell -> {
          TableView view = readOnlyGrid(shell, plainColumns(), null);
          view.setShortenDisplayedValues(true);
          TableItem row = view.table.getItem(0);
          view.setCellValue(row, 1, multiLine);
          // A NULL, drawn the way the data grids draw it, and a value that only reads "<null>".
          row.setText(2, "<null>");
          row.setForeground(2, GuiResource.getInstance().getColorBlue());
          view.table.getItem(1).setText(2, "<null>");
          viewRef.set(view);
        },
        bot -> {
          waitForLayout(viewRef.get());
          String[] copied =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    String displayed = view.table.getItem(0).getText(1);
                    sendMouse(view, SWT.MouseDown, 0, 1);
                    String stored = view.getActiveCellValueToCopy();
                    sendMouse(view, SWT.MouseDown, 0, 2);
                    String nullValue = view.getActiveCellValueToCopy();
                    sendMouse(view, SWT.MouseDown, 1, 2);
                    String text = view.getActiveCellValueToCopy();
                    return new String[] {displayed, stored, nullValue, text};
                  });
          assertFalse(multiLine.equals(copied[0]), "the cell shows a shortened value");
          assertEquals(multiLine, copied[1], "the stored value is copied, not the display text");
          assertEquals("", copied[2], "a NULL is copied as nothing");
          assertEquals("<null>", copied[3], "text that only reads <null> is copied as it is");
        });
  }

  @Test
  void copyCellValueOnlyActsOnTheCellTheMenuWasOpenedOn() {
    AtomicReference<TableView> viewRef = new AtomicReference<>();
    withScene(
        shell -> viewRef.set(readOnlyGrid(shell, plainColumns(), null)),
        bot -> {
          waitForLayout(viewRef.get());
          boolean[] enabled =
              onUi(
                  () -> {
                    TableView view = viewRef.get();
                    sendMouse(view, SWT.MouseDown, 0, 1);
                    Point onCell = center(view.table.getItem(0).getBounds(1));
                    Rectangle lastRow = view.table.getItem(1).getBounds(1);
                    Point belowRows = new Point(onCell.x, lastRow.y + lastRow.height * 3);
                    Point otherCell = center(view.table.getItem(1).getBounds(2));
                    return new boolean[] {
                      copyCellEnabled(view, SWT.MENU_MOUSE, onCell),
                      copyCellEnabled(view, SWT.MENU_MOUSE, belowRows),
                      // Shift/ctrl right-click does not move the active cell.
                      copyCellEnabled(view, SWT.MENU_MOUSE, otherCell),
                      copyCellEnabled(view, SWT.MENU_KEYBOARD, new Point(-1, -1))
                    };
                  });
          assertTrue(enabled[0], "menu opened on the active cell");
          assertFalse(enabled[1], "menu opened below the rows");
          assertFalse(enabled[2], "menu opened on another cell than the active one");
          assertTrue(enabled[3], "menu opened from the keyboard acts on the active cell");
        });
  }

  /** Open the row context menu the way SWT does and report whether Copy cell value is enabled. */
  private static boolean copyCellEnabled(TableView view, int detail, Point tablePoint) {
    Event detect = new Event();
    Point displayPoint = view.table.toDisplay(tablePoint);
    detect.x = displayPoint.x;
    detect.y = displayPoint.y;
    detect.detail = detail;
    view.table.notifyListeners(SWT.MenuDetect, detect);
    Menu menu = view.table.getMenu();
    menu.notifyListeners(SWT.Show, new Event());
    String label = BaseMessages.getString(TableView.class, "TableView.menu.CopyCellValue");
    for (MenuItem item : menu.getItems()) {
      if (label.equals(item.getText())) {
        return item.isEnabled();
      }
    }
    throw new AssertionError("no '" + label + "' item in the row menu");
  }

  private static ColumnInfo[] plainColumns() {
    return new ColumnInfo[] {
      new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
      new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
    };
  }

  /** Plain, editable columns in a grid that is read-only as a whole, as BaseDialog makes it. */
  private static TableView readOnlyGrid(
      Composite parent, ColumnInfo[] columns, ModifyListener modifyListener) {
    parent.setLayout(new FillLayout());
    // Room for the toolbar and both rows, so nothing has to scroll.
    parent.setSize(640, 400);
    TableView view =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            2,
            modifyListener,
            PropsUi.getInstance());
    for (int row = 0; row < 2; row++) {
      view.table.getItem(row).setText(1, "a" + row);
      view.table.getItem(row).setText(2, "b" + row);
    }
    view.setReadonly(true);
    return view;
  }

  /**
   * GTK sizes a shell some time after it opens. Until then the table has no client area and a click
   * on any row but the first finds no visible row to land in, so wait for the rows to show.
   */
  private static void waitForLayout(TableView view) {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!onUi(() -> rowsVisible(view))) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("the grid was never laid out: " + onUi(view.table::getClientArea));
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }

  private static boolean rowsVisible(TableView view) {
    Rectangle clientArea = view.table.getClientArea();
    Rectangle lastRow = view.table.getItem(view.table.getItemCount() - 1).getBounds(2);
    return clientArea.width >= lastRow.x + lastRow.width
        && clientArea.y + clientArea.height >= lastRow.y + lastRow.height;
  }

  private static void doubleClick(TableView view, int row, int column) {
    sendMouse(view, SWT.MouseDown, row, column);
    sendMouse(view, SWT.MouseDoubleClick, row, column);
  }

  private static void sendMouse(TableView view, int type, int row, int column) {
    Point point = center(view.table.getItem(row).getBounds(column));
    Event event = new Event();
    event.button = 1;
    event.x = point.x;
    event.y = point.y;
    view.table.notifyListeners(type, event);
  }

  private static Point center(Rectangle bounds) {
    return new Point(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2);
  }

  private static Text findText(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Text text && !text.isDisposed()) {
        return text;
      }
      if (child instanceof Composite composite) {
        Text found = findText(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    AtomicReference<Error> error = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          } catch (Error e) {
            error.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    if (error.get() != null) {
      throw error.get();
    }
    return result.get();
  }
}
