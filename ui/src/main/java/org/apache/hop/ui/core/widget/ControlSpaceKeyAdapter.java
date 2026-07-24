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
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableRegistry;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

public class ControlSpaceKeyAdapter extends KeyAdapter {

  private static final Class<?> PKG = ControlSpaceKeyAdapter.class;

  /**
   * Variable names matching one of these (case-insensitive) get their value masked in the popup.
   */
  private static final String[] SECRET_HINTS = {"password", "passwd", "secret", "token"};

  private static final String MASKED_VALUE = "********";

  private final IGetCaretPosition getCaretPositionInterface;

  private final IInsertText insertTextInterface;

  private IVariables variables;

  private final Control control;

  /**
   * @param variables IVariables object
   * @param control a Text or CCombo box object
   */
  public ControlSpaceKeyAdapter(final IVariables variables, final Control control) {
    this(variables, control, null, null);
  }

  /**
   * @param variables IVariables object
   * @param control a Text or CCombo box object
   * @param getCaretPositionInterface
   * @param insertTextInterface
   */
  public ControlSpaceKeyAdapter(
      IVariables variables,
      final Control control,
      final IGetCaretPosition getCaretPositionInterface,
      final IInsertText insertTextInterface) {

    this.variables = variables;
    this.control = control;
    this.getCaretPositionInterface = getCaretPositionInterface;
    this.insertTextInterface = insertTextInterface;
  }

  /**
   * in chinese window, Ctrl-SPACE is reversed by system for input chinese character. use
   * Ctrl-ALT-SPACE instead.
   *
   * @param event the keyevent
   * @return true when ctrl-SPACE is pressed
   */
  private boolean isHotKey(KeyEvent event) {
    if (System.getProperty("user.language").equals("zh")) {
      return event.character == ' '
          && ((event.stateMask & SWT.CONTROL) != 0)
          && ((event.stateMask & SWT.ALT) != 0);
    } else if (OsHelper.isMac()) {
      // character is empty when pressing special key in macOs
      return event.keyCode == 32
          && ((event.stateMask & SWT.CONTROL) != 0)
          && ((event.stateMask & SWT.ALT) == 0);
    } else {
      return event.character == ' '
          && ((event.stateMask & SWT.CONTROL) != 0)
          && ((event.stateMask & SWT.ALT) == 0);
    }
  }

  @Override
  public void keyPressed(KeyEvent event) {
    // CTRL-<SPACE> --> Insert a variable
    if (isHotKey(event)) {
      event.doit = false;
      new VariablePopup().open();
    }
  }

  /**
   * The searchable, grouped variable picker shown on Ctrl/Cmd-Space. It replaces the old flat,
   * unsearchable list: a search box (reusing the shared {@link SearchMatcher}), a tree of variables
   * grouped by scope showing their resolved value, and a footer with the full value and description
   * of the highlighted variable.
   */
  private final class VariablePopup {

    private Shell shell;
    private Text wSearch;
    private Tree wTree;
    private Label wDescription;

    /** The smaller font the resolved value is drawn with under the variable name. */
    private Font valueFont;

    /**
     * Two-line rows are painted by hand; that is not supported in Hop Web, which falls back to a
     * two-column (name, value) layout.
     */
    private boolean ownerDrawn;

    private final List<VarCandidate> candidates = buildCandidates(variables);
    private final Map<TreeItem, VarCandidate> itemCandidates = new IdentityHashMap<>();
    private final List<TreeItem> leafItems = new ArrayList<>();
    private int position = -1;

    void open() {
      // textField.setData(TRUE) indicates we have transitioned from the textbox to list mode...
      // This will be set to false when the selection has been processed and the popup is disposed.
      control.setData(Boolean.TRUE);

      if (getCaretPositionInterface != null) {
        position = getCaretPositionInterface.getCaretPosition();
      }

      Rectangle bounds = control.getBounds();
      Point location;
      // StyledText is not supported in Hop Web
      if (!EnvironmentUtils.getInstance().isWeb() && control instanceof StyledText styledText) {
        // Position the popup under the caret
        location = styledText.getLocationAtOffset(styledText.getCaretOffset());
        location.y += styledText.getLineHeight();
        location = styledText.toDisplay(location);
      } else {
        // Position the popup under the control
        location = GuiResource.calculateControlPosition(control);
        location.y += bounds.height;
      }

      shell = new Shell(control.getShell(), SWT.NONE);
      shell.setLayout(new FormLayout());
      int width = Math.max(bounds.width, (int) (PropsUi.getInstance().getZoomFactor() * 450));
      int height = (int) (PropsUi.getInstance().getZoomFactor() * 360);
      shell.setSize(width, height);
      shell.setLocation(location.x, location.y);

      // Search box on top
      wSearch =
          new Text(
              shell,
              SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL);
      wSearch.setMessage(BaseMessages.getString(PKG, "ControlSpaceKeyAdapter.Search.Placeholder"));
      PropsUi.setLook(wSearch);
      FormData fdSearch = new FormData();
      fdSearch.left = new FormAttachment(0, 0);
      fdSearch.right = new FormAttachment(100, 0);
      fdSearch.top = new FormAttachment(0, 0);
      wSearch.setLayoutData(fdSearch);

      ownerDrawn = !EnvironmentUtils.getInstance().isWeb();

      // Description footer at the bottom
      wDescription = new Label(shell, SWT.LEFT | SWT.WRAP);
      PropsUi.setLook(wDescription);
      FormData fdDescription = new FormData();
      fdDescription.left = new FormAttachment(0, PropsUi.getMargin());
      fdDescription.right = new FormAttachment(100, -PropsUi.getMargin());
      fdDescription.bottom = new FormAttachment(100, 0);
      fdDescription.top =
          new FormAttachment(100, -(int) (PropsUi.getInstance().getZoomFactor() * 60));
      wDescription.setLayoutData(fdDescription);

      // A separator between the list and the value/description footer
      Label wSeparator = new Label(shell, SWT.SEPARATOR | SWT.HORIZONTAL);
      FormData fdSeparator = new FormData();
      fdSeparator.left = new FormAttachment(0, 0);
      fdSeparator.right = new FormAttachment(100, 0);
      fdSeparator.bottom = new FormAttachment(wDescription, -PropsUi.getMargin());
      wSeparator.setLayoutData(fdSeparator);

      // The smaller font used for the resolved value
      FontData[] fontData = wSearch.getFont().getFontData();
      valueFont =
          new Font(
              shell.getDisplay(),
              fontData[0].getName(),
              Math.max(8, fontData[0].getHeight() - 1),
              SWT.NORMAL);

      // The tree of variables in the middle
      int treeStyle = SWT.SINGLE | SWT.FULL_SELECTION | SWT.V_SCROLL;
      if (!ownerDrawn) {
        treeStyle |= SWT.H_SCROLL;
      }
      wTree = new Tree(shell, treeStyle);
      wTree.setHeaderVisible(false);
      wTree.setLinesVisible(false);
      PropsUi.setLook(wTree);
      new TreeColumn(wTree, SWT.LEFT);
      new TreeColumn(wTree, SWT.LEFT);
      FormData fdTree = new FormData();
      fdTree.left = new FormAttachment(0, 0);
      fdTree.right = new FormAttachment(100, 0);
      fdTree.top = new FormAttachment(wSearch, 0);
      fdTree.bottom = new FormAttachment(wSeparator, 0);
      wTree.setLayoutData(fdTree);

      // Listeners
      wSearch.addListener(SWT.Modify, e -> rebuild(wSearch.getText()));
      wSearch.addListener(SWT.KeyDown, this::onSearchKey);
      wTree.addListener(SWT.Selection, e -> syncSelectionFromTree());
      wTree.addListener(SWT.DefaultSelection, e -> insertSelected());
      wTree.addListener(SWT.Resize, e -> resizeColumns());
      if (ownerDrawn) {
        // Render each variable on two lines: name on top, resolved value smaller and muted below.
        wTree.addListener(SWT.MeasureItem, this::onMeasureItem);
        wTree.addListener(SWT.EraseItem, this::onEraseItem);
        wTree.addListener(SWT.PaintItem, this::onPaintItem);
      }
      shell.addListener(SWT.Deactivate, e -> dispose());
      shell.addListener(SWT.Traverse, this::onTraverse);

      rebuild("");

      shell.open();
      // The tree only has a real client area once it is laid out and shown.
      resizeColumns();
      wSearch.setFocus();
    }

    /**
     * When painting two-line rows a single full-width column is used; otherwise (Hop Web) the width
     * is split between the variable name and its value.
     */
    private void resizeColumns() {
      int available = wTree.getClientArea().width;
      if (available <= 0) {
        return;
      }
      if (ownerDrawn) {
        wTree.getColumn(0).setWidth(available);
        wTree.getColumn(1).setWidth(0);
      } else {
        int nameWidth = (int) (available * 0.45);
        wTree.getColumn(0).setWidth(nameWidth);
        wTree.getColumn(1).setWidth(available - nameWidth);
      }
    }

    /** Reserve room for two lines under each variable (name + value). */
    private void onMeasureItem(Event event) {
      if (!itemCandidates.containsKey((TreeItem) event.item)) {
        return; // group header keeps the default single-line height
      }
      GC gc = event.gc;
      gc.setFont(wTree.getFont());
      int nameHeight = gc.getFontMetrics().getHeight();
      gc.setFont(valueFont);
      int valueHeight = gc.getFontMetrics().getHeight();
      event.height = nameHeight + valueHeight + 4;
    }

    /** Suppress the default (single-line) text for leaves; we paint it ourselves. */
    private void onEraseItem(Event event) {
      if (itemCandidates.containsKey((TreeItem) event.item)) {
        // Keep the background (including the selection highlight), drop the default foreground.
        event.detail &= ~SWT.FOREGROUND;
      }
    }

    /** Paint a variable on two lines: name on top, resolved value smaller and muted below. */
    private void onPaintItem(Event event) {
      if (event.index != 0) {
        return; // everything is drawn in the first (full-width) column
      }
      VarCandidate candidate = itemCandidates.get((TreeItem) event.item);
      if (candidate == null) {
        return; // group header is painted by the default handler
      }
      GC gc = event.gc;
      boolean selected = (event.detail & SWT.SELECTED) != 0;
      Color nameColor;
      Color valueColor;
      if (selected) {
        nameColor = wTree.getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION_TEXT);
        valueColor = nameColor;
      } else {
        nameColor =
            candidate.category == VarCategory.DEPRECATED
                ? GuiResource.getInstance().getColorDarkGray()
                : wTree.getForeground();
        valueColor = GuiResource.getInstance().getColorDarkGray();
      }

      int x = event.x + 2;
      gc.setFont(wTree.getFont());
      gc.setForeground(nameColor);
      int nameHeight = gc.getFontMetrics().getHeight();
      gc.drawText(candidate.name, x, event.y + 1, SWT.DRAW_TRANSPARENT);

      gc.setFont(valueFont);
      gc.setForeground(valueColor);
      gc.drawText(candidate.displayValue, x, event.y + 1 + nameHeight, SWT.DRAW_TRANSPARENT);
    }

    /** Rebuild the tree for the given filter text, grouping the matches by scope. */
    private void rebuild(String filter) {
      wTree.setRedraw(false);
      wTree.removeAll();
      itemCandidates.clear();
      leafItems.clear();

      SearchMatcher matcher =
          StringUtils.isEmpty(filter) ? null : new SearchMatcher(filter, false, false, true);
      Color mutedColor = GuiResource.getInstance().getColorDarkGray();

      // Bucket the matching candidates per category, keeping a score for ranking.
      Map<VarCategory, List<Scored>> byCategory = new EnumMap<>(VarCategory.class);
      for (VarCandidate candidate : candidates) {
        double score = 1.0;
        if (matcher != null) {
          score = Math.max(matcher.score(candidate.name), matcher.score(candidate.rawValue));
          if (score <= 0.0) {
            continue;
          }
        }
        byCategory
            .computeIfAbsent(candidate.category, k -> new ArrayList<>())
            .add(new Scored(candidate, score));
      }

      for (VarCategory category : VarCategory.values()) {
        List<Scored> scoredList = byCategory.get(category);
        if (scoredList == null || scoredList.isEmpty()) {
          continue;
        }
        scoredList.sort(
            Comparator.comparingDouble((Scored s) -> s.score)
                .reversed()
                .thenComparing(s -> s.candidate.name, String.CASE_INSENSITIVE_ORDER));

        TreeItem groupItem = new TreeItem(wTree, SWT.NONE);
        groupItem.setText(
            0, BaseMessages.getString(PKG, category.messageKey) + "  (" + scoredList.size() + ")");
        groupItem.setFont(GuiResource.getInstance().getFontBold());

        for (Scored scored : scoredList) {
          VarCandidate candidate = scored.candidate;
          TreeItem item = new TreeItem(groupItem, SWT.NONE);
          item.setText(new String[] {candidate.name, candidate.displayValue});
          if (candidate.category == VarCategory.DEPRECATED) {
            item.setForeground(mutedColor);
          }
          if (!ownerDrawn) {
            // Two-column fallback: make the value column visually secondary.
            item.setFont(1, valueFont);
            item.setForeground(1, mutedColor);
          }
          itemCandidates.put(item, candidate);
          leafItems.add(item);
        }
        groupItem.setExpanded(true);
      }

      wTree.setRedraw(true);

      if (leafItems.isEmpty()) {
        wDescription.setText(
            BaseMessages.getString(PKG, "ControlSpaceKeyAdapter.NoResults", filter));
      } else {
        selectLeaf(0);
      }
    }

    /** Move the selection to the leaf variable at the given index (clamped) and reveal it. */
    private void selectLeaf(int index) {
      if (leafItems.isEmpty()) {
        return;
      }
      int clamped = Math.max(0, Math.min(index, leafItems.size() - 1));
      TreeItem item = leafItems.get(clamped);
      wTree.setSelection(item);
      wTree.showSelection();
      updateDescription(itemCandidates.get(item));
    }

    /** Keep the footer and the leaf cursor in step when the user clicks in the tree. */
    private void syncSelectionFromTree() {
      TreeItem[] selection = wTree.getSelection();
      if (selection.length == 0) {
        return;
      }
      VarCandidate candidate = itemCandidates.get(selection[0]);
      if (candidate != null) {
        updateDescription(candidate);
      }
    }

    private void updateDescription(VarCandidate candidate) {
      if (candidate == null) {
        wDescription.setText("");
        return;
      }
      StringBuilder message = new StringBuilder();
      message.append("${").append(candidate.name).append("} = ").append(candidate.displayValue);
      if (StringUtils.isNotEmpty(candidate.description)) {
        message.append('\n').append(candidate.description);
      }
      if (candidate.name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
        message
            .append('\n')
            .append(BaseMessages.getString(PKG, "TextVar.InternalVariable.Message"));
      }
      wDescription.setText(message.toString());
    }

    private int currentLeafIndex() {
      TreeItem[] selection = wTree.getSelection();
      if (selection.length > 0) {
        int index = leafItems.indexOf(selection[0]);
        if (index >= 0) {
          return index;
        }
      }
      return 0;
    }

    /** Drive the tree selection from the search box so the user never leaves the keyboard. */
    private void onSearchKey(Event event) {
      switch (event.keyCode) {
        case SWT.ARROW_DOWN:
          event.doit = false;
          selectLeaf(currentLeafIndex() + 1);
          break;
        case SWT.ARROW_UP:
          event.doit = false;
          selectLeaf(currentLeafIndex() - 1);
          break;
        case SWT.PAGE_DOWN:
          event.doit = false;
          selectLeaf(currentLeafIndex() + 10);
          break;
        case SWT.PAGE_UP:
          event.doit = false;
          selectLeaf(currentLeafIndex() - 10);
          break;
        case SWT.CR, SWT.KEYPAD_CR:
          event.doit = false;
          insertSelected();
          break;
        default:
          break;
      }
    }

    private void onTraverse(Event event) {
      if (event.detail == SWT.TRAVERSE_ESCAPE) {
        event.doit = false;
        dispose();
      }
    }

    private void insertSelected() {
      TreeItem[] selection = wTree.getSelection();
      VarCandidate candidate = selection.length > 0 ? itemCandidates.get(selection[0]) : null;
      if (candidate == null && !leafItems.isEmpty()) {
        candidate = itemCandidates.get(leafItems.get(0));
      }
      if (candidate == null) {
        dispose();
        return;
      }

      String extra = "${" + candidate.name + "}";
      if (insertTextInterface != null) {
        insertTextInterface.insertText(extra, position);
      } else if (!control.isDisposed()) {
        if (control instanceof Text text) {
          text.insert(extra);
        } else if (control instanceof CCombo combo) {
          // We can't know the location of the cursor yet. All we can do is overwrite.
          combo.setText(extra);
        } else if (control instanceof StyledText styledText) {
          styledText.insert(extra);
        }
      }
      dispose();
    }

    private void dispose() {
      if (shell != null && !shell.isDisposed()) {
        shell.dispose();
      }
      if (valueFont != null && !valueFont.isDisposed()) {
        valueFont.dispose();
      }
      if (!control.isDisposed()) {
        control.setData(Boolean.FALSE);
      }
    }
  }

  /** A single variable ready to be shown in the picker: name, resolved value, scope and help. */
  static final class VarCandidate {
    final String name;
    final String rawValue;
    final String displayValue;
    final String description;
    final VarCategory category;

    VarCandidate(
        String name,
        String rawValue,
        String displayValue,
        String description,
        VarCategory category) {
      this.name = name;
      this.rawValue = rawValue;
      this.displayValue = displayValue;
      this.description = description;
      this.category = category;
    }
  }

  private static final class Scored {
    final VarCandidate candidate;
    final double score;

    Scored(VarCandidate candidate, double score) {
      this.candidate = candidate;
      this.score = score;
    }
  }

  /** The scope groups shown as headers in the picker, in display order (deprecated sinks last). */
  enum VarCategory {
    PROJECT("ControlSpaceKeyAdapter.Category.Project"),
    CUSTOM("ControlSpaceKeyAdapter.Category.Custom"),
    INTERNAL("ControlSpaceKeyAdapter.Category.Internal"),
    APPLICATION("ControlSpaceKeyAdapter.Category.Application"),
    SYSTEM("ControlSpaceKeyAdapter.Category.System"),
    DEPRECATED("ControlSpaceKeyAdapter.Category.Deprecated");

    final String messageKey;

    VarCategory(String messageKey) {
      this.messageKey = messageKey;
    }
  }

  /**
   * Build the list of variables to show in the picker: resolves every value, masks secrets, looks
   * up descriptions and assigns each variable to a scope group.
   */
  static List<VarCandidate> buildCandidates(IVariables variables) {
    VariableRegistry registry = VariableRegistry.getInstance();
    Properties systemProperties = System.getProperties();
    Set<String> deprecatedSet = new HashSet<>(registry.getDeprecatedVariableNames());
    Set<String> systemSettings = registry.getVariableNames(VariableScope.SYSTEM);
    Set<String> applicationSet =
        registry.getVariableNames(VariableScope.APPLICATION, VariableScope.ENGINE);

    Map<String, String> pluginPrefixMap = new HashMap<>();
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI,
          variables,
          HopExtensionPoint.HopGuiGetControlSpaceSortOrderPrefix.name(),
          pluginPrefixMap);
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error calling extension point 'HopGuiGetControlSpaceSortOrderPrefix'", e);
    }

    List<VarCandidate> result = new ArrayList<>();
    for (String name : variables.getVariableNames()) {
      String rawValue = Const.NVL(variables.getVariable(name), "");
      String displayValue = isSecret(name) ? MASKED_VALUE : rawValue;
      DescribedVariable described = registry.findDescribedVariable(name);
      String description = described == null ? null : described.getDescription();
      VarCategory category =
          categorize(
              name,
              deprecatedSet,
              pluginPrefixMap.keySet(),
              systemSettings,
              applicationSet,
              systemProperties);
      result.add(new VarCandidate(name, rawValue, displayValue, description, category));
    }
    return result;
  }

  /**
   * Assign a variable to a scope group. Checked most-specific first: a deprecated variable is shown
   * as deprecated even if it is also internal, and a plugin-contributed (project/environment)
   * variable wins over the generic buckets.
   */
  static VarCategory categorize(
      String name,
      Set<String> deprecatedSet,
      Set<String> pluginVariables,
      Set<String> systemSettings,
      Set<String> applicationSet,
      Properties systemProperties) {
    if (deprecatedSet.contains(name)) {
      return VarCategory.DEPRECATED;
    }
    if (pluginVariables.contains(name)) {
      return VarCategory.PROJECT;
    }
    if (name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
      return VarCategory.INTERNAL;
    }
    if (systemSettings.contains(name) || systemProperties.getProperty(name) != null) {
      return VarCategory.SYSTEM;
    }
    if (applicationSet.contains(name)) {
      return VarCategory.APPLICATION;
    }
    return VarCategory.CUSTOM;
  }

  /** Whether a variable's value should be masked in the picker (looks like a credential). */
  static boolean isSecret(String name) {
    if (name == null) {
      return false;
    }
    String lower = name.toLowerCase();
    for (String hint : SECRET_HINTS) {
      if (lower.contains(hint)) {
        return true;
      }
    }
    return false;
  }

  public void setVariables(IVariables vars) {
    variables = vars;
  }
}
