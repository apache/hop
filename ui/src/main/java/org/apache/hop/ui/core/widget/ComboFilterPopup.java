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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

/**
 * Click-to-select-all and type-to-filter support for a toolbar {@link Combo}. Typing opens a popup
 * list under the combo with the items that match the typed text.
 */
public class ComboFilterPopup {

  public static final String DATA_KEY = ComboFilterPopup.class.getName();

  private static final int MAX_VISIBLE_ITEMS = 10;

  private final Combo combo;
  private final Supplier<List<String>> itemsSupplier;
  private final Consumer<String> onSelect;

  private String originalText = "";
  private boolean updating;
  private boolean suppressSelectAll;
  private boolean applied;
  private boolean openingPopup;
  private Shell popup;
  private org.eclipse.swt.widgets.List list;
  private Listener mouseDownFilter;

  private ComboFilterPopup(
      Combo combo, Supplier<List<String>> itemsSupplier, Consumer<String> onSelect) {
    this.combo = combo;
    this.itemsSupplier = itemsSupplier;
    this.onSelect = onSelect;
  }

  /**
   * Attach type-to-filter behaviour to {@code combo}. Safe to call more than once; the same
   * instance is reused.
   */
  public static ComboFilterPopup attach(
      Combo combo, Supplier<List<String>> itemsSupplier, Consumer<String> onSelect) {
    ComboFilterPopup existing = get(combo);
    if (existing != null) {
      return existing;
    }
    ComboFilterPopup support = new ComboFilterPopup(combo, itemsSupplier, onSelect);
    support.install();
    combo.setData(DATA_KEY, support);
    return support;
  }

  public static ComboFilterPopup get(Combo combo) {
    if (combo == null || combo.isDisposed()) {
      return null;
    }
    Object data = combo.getData(DATA_KEY);
    return data instanceof ComboFilterPopup filter ? filter : null;
  }

  public boolean isPopupOpen() {
    return popup != null && !popup.isDisposed() && popup.isVisible();
  }

  /**
   * Rank and filter {@code items} with {@link SearchMatcher}. An empty query returns the items in
   * their original order.
   */
  public static List<String> filterItems(Collection<String> items, String query) {
    if (items == null || items.isEmpty()) {
      return List.of();
    }
    List<String> source = new ArrayList<>();
    for (String item : items) {
      if (StringUtils.isNotEmpty(item)) {
        source.add(item);
      }
    }
    if (source.isEmpty()) {
      return List.of();
    }
    if (StringUtils.isEmpty(query)) {
      return source;
    }
    SearchMatcher matcher = new SearchMatcher(query, false, false, true);
    record Scored(String name, double score) {}
    List<Scored> scored = new ArrayList<>();
    for (String name : source) {
      double score = matcher.score(name);
      if (score > 0.0) {
        scored.add(new Scored(name, score));
      }
    }
    scored.sort(
        Comparator.comparingDouble(Scored::score)
            .reversed()
            .thenComparing(Scored::name, String.CASE_INSENSITIVE_ORDER));
    List<String> matches = new ArrayList<>(scored.size());
    for (Scored item : scored) {
      matches.add(item.name());
    }
    return matches;
  }

  private void install() {
    combo.addListener(SWT.FocusIn, event -> onFocusIn());
    combo.addListener(
        SWT.MouseUp,
        event -> {
          suppressSelectAll = false;
          selectAllLater();
        });
    combo.addListener(SWT.Modify, event -> onModify());
    combo.addListener(SWT.KeyDown, this::onKeyDown);
    combo.addListener(SWT.Traverse, this::onTraverse);
    combo.addListener(SWT.FocusOut, event -> combo.getDisplay().asyncExec(this::onFocusLost));
    combo.addListener(SWT.Dispose, event -> closePopup());
  }

  private void onFocusIn() {
    // Reclaiming focus while the popup is open (Hop Web) must not reset the typed query.
    if (isPopupOpen()) {
      return;
    }
    originalText = Const.NVL(combo.getText(), "");
    suppressSelectAll = false;
    applied = false;
    selectAllLater();
  }

  private void selectAllLater() {
    Display display = combo.getDisplay();
    display.asyncExec(
        () -> {
          if (suppressSelectAll || combo.isDisposed() || !combo.isFocusControl()) {
            return;
          }
          String text = Const.NVL(combo.getText(), "");
          combo.setSelection(new Point(0, text.length()));
        });
  }

  private void onModify() {
    suppressSelectAll = true;
    if (updating || combo.isDisposed() || !combo.isFocusControl()) {
      return;
    }
    hideNativeList();
    updatePopup(typedQuery());
  }

  /**
   * The text the user actually typed. Native combo auto-complete selects the remainder of the first
   * match; that suffix is ignored for searching.
   */
  private String typedQuery() {
    String text = Const.NVL(combo.getText(), "");
    Point selection = combo.getSelection();
    if (selection != null && selection.y > selection.x && selection.y >= text.length()) {
      return text.substring(0, Math.min(selection.x, text.length()));
    }
    return text;
  }

  private void onKeyDown(Event event) {
    if (!isPopupOpen()) {
      return;
    }
    switch (event.keyCode) {
      case SWT.ARROW_DOWN -> {
        moveSelection(1);
        event.doit = false;
      }
      case SWT.ARROW_UP -> {
        moveSelection(-1);
        event.doit = false;
      }
      case SWT.CR, SWT.KEYPAD_CR -> {
        applySelection();
        event.doit = false;
      }
      case SWT.ESC -> {
        cancel();
        event.doit = false;
      }
      default -> {
        // other keys edit the combo as usual
      }
    }
  }

  private void onTraverse(Event event) {
    if (!isPopupOpen()) {
      return;
    }
    if (event.detail == SWT.TRAVERSE_RETURN || event.detail == SWT.TRAVERSE_ESCAPE) {
      event.doit = false;
      event.detail = SWT.TRAVERSE_NONE;
    }
  }

  private void onFocusLost() {
    if (combo.isDisposed()) {
      closePopup();
      return;
    }
    if (openingPopup) {
      return;
    }
    if (applied) {
      closePopup();
      return;
    }
    Control focus = combo.getDisplay().getFocusControl();
    if (focus == combo || isPopupControl(focus)) {
      return;
    }
    restoreOrApplyExactMatch();
    closePopup();
  }

  private boolean isPopupControl(Control control) {
    return popup != null
        && !popup.isDisposed()
        && control != null
        && (control == popup || control == list);
  }

  private void updatePopup(String query) {
    List<String> matches = filterItems(itemsSupplier.get(), query);
    if (matches.isEmpty()) {
      closePopup();
      return;
    }
    if (!isPopupOpen()) {
      openPopup();
    }
    if (list == null || list.isDisposed()) {
      return;
    }
    String previous = list.getSelectionCount() > 0 ? list.getSelection()[0] : null;
    list.setItems(matches.toArray(String[]::new));
    int index = previous == null ? 0 : matches.indexOf(previous);
    list.setSelection(index < 0 ? 0 : index);
    layoutPopup();
  }

  private void openPopup() {
    closePopup();
    Shell parent = combo.getShell();
    int style =
        EnvironmentUtils.getInstance().isWeb() ? SWT.NONE : SWT.ON_TOP | SWT.NO_FOCUS | SWT.TOOL;
    popup = new Shell(parent, style);
    popup.setLayout(new FillLayout());
    list = new org.eclipse.swt.widgets.List(popup, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER);
    PropsUi.setLook(list);
    list.addListener(SWT.MouseUp, event -> applySelection());
    list.addListener(SWT.DefaultSelection, event -> applySelection());
    layoutPopup();
    openingPopup = true;
    popup.setVisible(true);
    installMouseDownFilter();
    if (!combo.isFocusControl()) {
      combo.setFocus();
    }
    combo.getDisplay().asyncExec(() -> openingPopup = false);
  }

  private void layoutPopup() {
    if (popup == null || popup.isDisposed() || list == null || list.isDisposed()) {
      return;
    }
    Point comboSize = combo.getSize();
    Point location = GuiResource.calculateControlPosition(combo);
    int itemCount = Math.max(list.getItemCount(), 1);
    int visible = Math.min(itemCount, MAX_VISIBLE_ITEMS);
    int itemHeight = Math.max(list.getItemHeight(), 16);
    int height = itemHeight * visible + 8;
    int width = Math.max(comboSize.x, 200);

    Rectangle displayArea = combo.getMonitor().getClientArea();
    int below = location.y + comboSize.y;
    if (below + height > displayArea.y + displayArea.height
        && location.y - height >= displayArea.y) {
      location.y = location.y - height;
    } else {
      location.y = below;
    }
    popup.setBounds(location.x, location.y, width, height);
  }

  private void moveSelection(int delta) {
    if (list == null || list.isDisposed() || list.getItemCount() == 0) {
      return;
    }
    int index = list.getSelectionIndex();
    if (index < 0) {
      index = 0;
    } else {
      index = Math.max(0, Math.min(list.getItemCount() - 1, index + delta));
    }
    list.setSelection(index);
    list.showSelection();
  }

  private void applySelection() {
    if (list == null || list.isDisposed() || list.getSelectionCount() == 0) {
      closePopup();
      return;
    }
    String name = list.getSelection()[0];
    applied = true;
    updating = true;
    try {
      combo.setText(name);
    } finally {
      updating = false;
    }
    closePopup();
    if (onSelect != null) {
      onSelect.accept(name);
    }
  }

  private void cancel() {
    applied = true;
    updating = true;
    try {
      combo.setText(Const.NVL(originalText, ""));
    } finally {
      updating = false;
    }
    closePopup();
  }

  private void restoreOrApplyExactMatch() {
    String text = Const.NVL(combo.getText(), "");
    List<String> items = itemsSupplier.get();
    if (items != null && items.contains(text)) {
      if (!text.equals(originalText) && onSelect != null) {
        onSelect.accept(text);
      }
      return;
    }
    updating = true;
    try {
      combo.setText(Const.NVL(originalText, ""));
    } finally {
      updating = false;
    }
  }

  private void hideNativeList() {
    try {
      if (combo.getListVisible()) {
        combo.setListVisible(false);
      }
    } catch (SWTException ignored) {
      // Combo list visibility is not supported on every platform (e.g. some RAP builds).
    }
  }

  private void installMouseDownFilter() {
    removeMouseDownFilter();
    Display display = combo.getDisplay();
    mouseDownFilter =
        event -> {
          if (!isPopupOpen()) {
            return;
          }
          if (event.widget == combo || isPopupControl(asControl(event.widget))) {
            return;
          }
          display.asyncExec(this::onFocusLost);
        };
    display.addFilter(SWT.MouseDown, mouseDownFilter);
  }

  private void removeMouseDownFilter() {
    if (mouseDownFilter == null || combo.isDisposed()) {
      mouseDownFilter = null;
      return;
    }
    combo.getDisplay().removeFilter(SWT.MouseDown, mouseDownFilter);
    mouseDownFilter = null;
  }

  private static Control asControl(Object widget) {
    return widget instanceof Control control ? control : null;
  }

  private void closePopup() {
    openingPopup = false;
    removeMouseDownFilter();
    if (popup != null && !popup.isDisposed()) {
      popup.dispose();
    }
    popup = null;
    list = null;
  }
}
