/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchQuery;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.apache.hop.ui.hopgui.terminal.HopGuiBottomDock;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

/**
 * IntelliJ-style "Search Everywhere" popup. It runs the same searchable-analyzer pipeline as the
 * {@link HopGuiSearchResultsPanel} (across all available locations) plus the global GUI commands,
 * and renders the results as a single grouped, keyboard-navigable list. Selecting a result opens it
 * through its regular {@link org.apache.hop.core.search.ISearchableCallback}. "Show all" hands the
 * query off to a Search tab in the bottom dock.
 */
public class SearchEverywhereDialog {

  private static final Class<?> PKG = SearchEverywhereDialog.class; // i18n

  /** Debounce in ms between a keystroke and running the (potentially expensive) search. */
  private static final int SEARCH_DELAY_MS = 250;

  /**
   * Maximum number of results shown per group in the popup; the full set lives in the perspective.
   */
  private static final int MAX_PER_GROUP = 50;

  private static final String ACTIONS_GROUP = "Actions";

  /** Minimum size of the resizable popup. */
  private static final int MIN_WIDTH = 420;

  private static final int MIN_HEIGHT = 280;

  private final HopGui hopGui;
  private final Shell parent;
  private final PropsUi props;

  /** Searchables enumerated once when the popup opens and reused for every keystroke. */
  private List<ISearchable> cachedSearchables;

  private Map<Class<ISearchableAnalyser>, ISearchableAnalyser> cachedAnalysers;

  /** For each cached searchable (by dedup key) the index of the location it came from (0 = GUI). */
  private final Map<String, Integer> sourceByKey = new HashMap<>();

  private Shell shell;
  private Combo wSearch;
  private Button wCaseSensitive;
  private Button wRegEx;
  private Tree wTree;
  private TreeColumn nameColumn;
  private TreeColumn detailColumn;
  private Link wShowAll;

  private Color detailColor;
  private final Runnable searchRunnable = this::runSearch;
  private int fullResultCount;

  public SearchEverywhereDialog(Shell parent, HopGui hopGui) {
    this.parent = parent;
    this.hopGui = hopGui;
    this.props = PropsUi.getInstance();
  }

  public void open() {
    Display display = parent.getDisplay();
    detailColor = display.getSystemColor(SWT.COLOR_DARK_GRAY);

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "SearchEverywhereDialog.Shell.Title"));
    shell.setImage(GuiResource.getInstance().getImageSearch());
    PropsUi.setLook(shell);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getMargin();
    formLayout.marginHeight = PropsUi.getMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    // --- Search bar: text field + case / regex toggles ---
    Composite searchBar = new Composite(shell, SWT.NONE);
    PropsUi.setLook(searchBar);
    GridLayout gridLayout = new GridLayout(3, false);
    gridLayout.marginWidth = 0;
    gridLayout.horizontalSpacing = margin;
    searchBar.setLayout(gridLayout);
    FormData fdSearchBar = new FormData();
    fdSearchBar.top = new FormAttachment(0, 0);
    fdSearchBar.left = new FormAttachment(0, 0);
    fdSearchBar.right = new FormAttachment(100, 0);
    searchBar.setLayoutData(fdSearchBar);

    wSearch = new Combo(searchBar, SWT.SINGLE | SWT.BORDER | SWT.DROP_DOWN);
    wSearch.setToolTipText(
        BaseMessages.getString(PKG, "SearchEverywhereDialog.Search.Placeholder"));
    wSearch.setFont(GuiResource.getInstance().getFontBold());
    wSearch.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    PropsUi.setLook(wSearch);
    loadHistory();

    wCaseSensitive = new Button(searchBar, SWT.CHECK);
    wCaseSensitive.setText(
        BaseMessages.getString(PKG, "SearchEverywhereDialog.CaseSensitive.Label"));
    wCaseSensitive.setToolTipText(
        BaseMessages.getString(PKG, "SearchEverywhereDialog.CaseSensitive.Tooltip"));
    PropsUi.setLook(wCaseSensitive);

    wRegEx = new Button(searchBar, SWT.CHECK);
    wRegEx.setText(BaseMessages.getString(PKG, "SearchEverywhereDialog.RegEx.Label"));
    wRegEx.setToolTipText(BaseMessages.getString(PKG, "SearchEverywhereDialog.RegEx.Tooltip"));
    PropsUi.setLook(wRegEx);

    // --- Footer: hint + "show all" handoff to the search perspective ---
    wShowAll = new Link(shell, SWT.NONE);
    PropsUi.setLook(wShowAll);
    FormData fdShowAll = new FormData();
    fdShowAll.left = new FormAttachment(0, 0);
    fdShowAll.right = new FormAttachment(100, 0);
    fdShowAll.bottom = new FormAttachment(100, 0);
    wShowAll.setLayoutData(fdShowAll);
    wShowAll.addListener(SWT.Selection, e -> openShowAll());

    // --- The grouped result tree (two user-resizable columns + horizontal scroll) ---
    wTree =
        new Tree(shell, SWT.SINGLE | SWT.BORDER | SWT.FULL_SELECTION | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);
    wTree.setHeaderVisible(true);
    nameColumn = new TreeColumn(wTree, SWT.LEFT);
    nameColumn.setText(BaseMessages.getString(PKG, "SearchEverywhereDialog.Column.Name"));
    nameColumn.setResizable(true);
    detailColumn = new TreeColumn(wTree, SWT.LEFT);
    detailColumn.setText(BaseMessages.getString(PKG, "SearchEverywhereDialog.Column.Location"));
    detailColumn.setResizable(true);
    FormData fdTree = new FormData();
    fdTree.top = new FormAttachment(searchBar, margin);
    fdTree.left = new FormAttachment(0, 0);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.bottom = new FormAttachment(wShowAll, -margin);
    wTree.setLayoutData(fdTree);

    // The Location column always fills the remaining width (also when the window or the Name column
    // is resized).
    wTree.addListener(SWT.Resize, e -> fillLocationColumn());
    nameColumn.addListener(SWT.Resize, e -> fillLocationColumn());

    // Open a result on double-click.
    wTree.addListener(SWT.MouseDoubleClick, e -> openSelected());

    // All keyboard handling lives on the search field so focus never leaves it.
    wSearch.addListener(SWT.Modify, e -> scheduleSearch());
    wSearch.addListener(SWT.KeyDown, this::handleSearchKey);

    // Close on Escape or via the window close button (both persist the window size).
    shell.addListener(
        SWT.Traverse,
        e -> {
          if (e.detail == SWT.TRAVERSE_ESCAPE) {
            e.doit = false;
            dispose();
          }
        });
    shell.addListener(
        SWT.Close,
        e -> {
          e.doit = false;
          dispose();
        });

    PropsUi.setTheme(shell);

    updateShowAll();
    restoreSize();

    shell.open();
    wSearch.setFocus();
    initColumnWidths();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  /** Restore the previously remembered size/position, or center a sensible default. */
  private void restoreSize() {
    shell.setMinimumSize(MIN_WIDTH, MIN_HEIGHT);
    WindowProperty windowProperty = props.getScreen(shell.getText());
    if (windowProperty != null) {
      windowProperty.setShell(shell, MIN_WIDTH, MIN_HEIGHT);
    } else {
      sizeAndCenter();
    }
  }

  private void sizeAndCenter() {
    Rectangle parentBounds = parent.getBounds();
    int width = Math.min((int) (720 * props.getZoomFactor()), parentBounds.width - 100);
    int height = Math.min((int) (460 * props.getZoomFactor()), parentBounds.height - 120);
    width = Math.max(width, MIN_WIDTH);
    height = Math.max(height, MIN_HEIGHT);
    int x = parentBounds.x + (parentBounds.width - width) / 2;
    int y = parentBounds.y + Math.max(60, (parentBounds.height - height) / 3);
    shell.setBounds(x, y, width, height);
  }

  /**
   * Give the Name column a compact starting width; the Location column then fills the rest. The
   * user can still drag the Name column wider, and the Location column keeps filling the remaining
   * space.
   */
  private void initColumnWidths() {
    int width = treeWidth();
    if (width <= 0) {
      return;
    }
    nameColumn.setWidth((int) (width * 0.30));
    fillLocationColumn();
  }

  /** Size the Location column to take up whatever width is left after the Name column. */
  private void fillLocationColumn() {
    if (wTree == null || wTree.isDisposed()) {
      return;
    }
    int width = treeWidth();
    if (width <= 0) {
      return;
    }
    detailColumn.setWidth(Math.max(120, width - nameColumn.getWidth()));
  }

  private int treeWidth() {
    int width = wTree.getClientArea().width;
    if (width <= 0 && shell != null && !shell.isDisposed()) {
      width = shell.getClientArea().width;
    }
    return width;
  }

  private void scheduleSearch() {
    Display display = shell.getDisplay();
    display.timerExec(-1, searchRunnable);
    display.timerExec(SEARCH_DELAY_MS, searchRunnable);
  }

  private void handleSearchKey(Event event) {
    switch (event.keyCode) {
      case SWT.ARROW_DOWN:
        event.doit = false;
        moveSelection(1);
        break;
      case SWT.ARROW_UP:
        event.doit = false;
        moveSelection(-1);
        break;
      case SWT.CR:
      case SWT.KEYPAD_CR:
        event.doit = false;
        openSelected();
        break;
      default:
        // let the keystroke reach the text field
        break;
    }
  }

  /**
   * All selectable items in visual order: the object nodes and their match children (and action
   * items), skipping the section/type header rows (which carry no data). Used for keyboard
   * navigation.
   */
  private List<TreeItem> leaves() {
    List<TreeItem> leaves = new ArrayList<>();
    for (TreeItem item : wTree.getItems()) {
      collectSelectable(item, leaves);
    }
    return leaves;
  }

  private void collectSelectable(TreeItem item, List<TreeItem> leaves) {
    if (item.getData() != null) {
      leaves.add(item);
    }
    for (TreeItem child : item.getItems()) {
      collectSelectable(child, leaves);
    }
  }

  private void moveSelection(int delta) {
    List<TreeItem> leaves = leaves();
    if (leaves.isEmpty()) {
      return;
    }
    int index = -1;
    TreeItem[] selection = wTree.getSelection();
    if (selection.length > 0) {
      index = leaves.indexOf(selection[0]);
    }
    index += delta;
    if (index < 0) {
      index = 0;
    }
    if (index >= leaves.size()) {
      index = leaves.size() - 1;
    }
    TreeItem item = leaves.get(index);
    wTree.setSelection(item);
    wTree.showItem(item);
  }

  /**
   * Enumerate all searchables once (loading pipelines/workflows/metadata from disk) and cache them,
   * de-duplicating objects that several locations report (e.g. metadata and variables are listed by
   * both the GUI and the project location). Subsequent keystrokes only re-match against this cache.
   */
  private void ensureLoaded() {
    if (cachedSearchables != null) {
      return;
    }
    try {
      cachedAnalysers = HopGuiSearchHelper.loadSearchableAnalysers();
    } catch (Exception e) {
      hopGui.getLog().logError("Error loading search analysers", e);
      cachedAnalysers = new HashMap<>();
    }
    HopGuiSearchHelper.EnumeratedSearchables enumerated =
        HopGuiSearchHelper.enumerateAll(
            hopGui.getSearchablesLocations(),
            hopGui.getMetadataProvider(),
            hopGui.getVariables(),
            hopGui.getLog());
    cachedSearchables = enumerated.getSearchables();
    sourceByKey.clear();
    sourceByKey.putAll(enumerated.getSourceByKey());
  }

  private void runSearch() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    wTree.removeAll();
    fullResultCount = 0;

    String searchString = wSearch.getText();
    if (Utils.isEmpty(searchString)) {
      updateShowAll();
      return;
    }

    ensureLoaded();

    boolean caseSensitive = wCaseSensitive.getSelection();
    boolean regExp = wRegEx.getSelection();
    SearchQuery query = new SearchQuery(searchString, caseSensitive, regExp);

    // The searchables were enumerated once when the popup opened; here we only run the cheap
    // in-memory analyser matching against that cached set. Errors are logged (not shown in a modal
    // dialog) since this runs on every debounced keystroke.
    List<ISearchResult> results;
    try {
      results = HopGuiSearchHelper.analyseRanked(cachedSearchables, query, cachedAnalysers, true);
    } catch (Exception e) {
      hopGui.getLog().logError("Error while searching", e);
      results = new ArrayList<>();
    }

    // Render the two-tier tree: section (Open/Project) -> type -> object -> the matches inside it.
    String searchTerm = regExp ? null : searchString;
    for (HopGuiSearchHelper.SearchSection section :
        HopGuiSearchHelper.groupResults(results, sourceByKey)) {
      addSection(section, searchTerm);
    }
    addCommandGroup(searchString, caseSensitive);

    selectFirstLeaf();
    updateShowAll();
  }

  private void addSection(HopGuiSearchHelper.SearchSection section, String searchTerm) {
    fullResultCount += section.getObjectCount();
    TreeItem sectionItem = new TreeItem(wTree, SWT.NONE);
    sectionItem.setText(
        0, groupLabel(displaySectionLabel(section.getKey()), section.getObjectCount()));
    sectionItem.setFont(GuiResource.getInstance().getFontBold());

    for (HopGuiSearchHelper.SearchTypeGroup typeGroup : section.getTypeGroups()) {
      TreeItem typeItem = new TreeItem(sectionItem, SWT.NONE);
      typeItem.setText(0, groupLabel(typeGroup.getType(), typeGroup.getObjects().size()));
      typeItem.setFont(GuiResource.getInstance().getFontBold());

      int shown = Math.min(typeGroup.getObjects().size(), MAX_PER_GROUP);
      for (int i = 0; i < shown; i++) {
        addObject(typeItem, typeGroup.getObjects().get(i), searchTerm);
      }
      typeItem.setExpanded(true);
    }
    sectionItem.setExpanded(true);
  }

  /** Add an object node (col 0 = name, col 1 = location) and a child row for each inner match. */
  private void addObject(
      TreeItem parent, HopGuiSearchHelper.SearchObjectGroup group, String searchTerm) {
    ISearchable searchable = group.getSearchable();
    TreeItem objectItem = new TreeItem(parent, SWT.NONE);
    objectItem.setText(0, HopGuiSearchHelper.displayLabel(searchable, hopGui.getVariables()));
    objectItem.setText(1, objectLocation(searchable));
    objectItem.setForeground(1, detailColor);
    objectItem.setData(group.getPrimary());

    for (ISearchResult match : group.getMatches()) {
      TreeItem matchItem = new TreeItem(objectItem, SWT.NONE);
      matchItem.setText(0, HopGuiSearchHelper.matchWhere(match));
      matchItem.setText(1, HopGuiSearchHelper.matchSnippet(match, searchTerm));
      matchItem.setForeground(1, detailColor);
      matchItem.setData(match);
    }
    objectItem.setExpanded(true);
  }

  private String displaySectionLabel(String sectionKey) {
    if (HopGuiSearchHelper.SECTION_OPEN.equals(sectionKey)) {
      return BaseMessages.getString(PKG, "SearchEverywhereDialog.Group.Open");
    }
    return BaseMessages.getString(PKG, "SearchEverywhereDialog.Group.Project");
  }

  /** The file path (sanitized to {@code ${PROJECT_HOME}}) or location shown for an object node. */
  private String objectLocation(ISearchable searchable) {
    String detail = searchable.getFilename();
    return Utils.isEmpty(detail)
        ? Const.NVL(searchable.getLocation(), "")
        : HopGuiSearchHelper.sanitizePath(detail, hopGui.getVariables());
  }

  private void addCommandGroup(String searchString, boolean caseSensitive) {
    GuiMenuWidgets menuWidgets = hopGui.getMainMenuWidgets();
    if (menuWidgets == null) {
      return;
    }
    String needle = caseSensitive ? searchString : searchString.toLowerCase(Locale.ROOT);

    List<MenuItem> matches = new ArrayList<>();
    for (MenuItem menuItem : menuWidgets.getMenuItemMap().values()) {
      if (menuItem == null || menuItem.isDisposed() || !menuItem.isEnabled()) {
        continue;
      }
      int style = menuItem.getStyle();
      if ((style & (SWT.CASCADE | SWT.SEPARATOR | SWT.CHECK | SWT.RADIO)) != 0) {
        continue;
      }
      String label = commandLabel(menuItem);
      if (Utils.isEmpty(label)) {
        continue;
      }
      String haystack = caseSensitive ? label : label.toLowerCase(Locale.ROOT);
      if (haystack.contains(needle)) {
        matches.add(menuItem);
      }
    }
    if (matches.isEmpty()) {
      return;
    }

    fullResultCount += matches.size();
    TreeItem group = new TreeItem(wTree, SWT.NONE);
    group.setText(groupLabel(ACTIONS_GROUP, matches.size()));
    group.setFont(GuiResource.getInstance().getFontBold());
    int shown = Math.min(matches.size(), MAX_PER_GROUP);
    for (int i = 0; i < shown; i++) {
      MenuItem menuItem = matches.get(i);
      TreeItem item = new TreeItem(group, SWT.NONE);
      item.setText(0, commandLabel(menuItem));
      Image image = menuItem.getImage();
      if (image != null) {
        item.setImage(0, image);
      }
      item.setData(menuItem);
    }
    group.setExpanded(true);
  }

  /** Strip the accelerator suffix and mnemonic markers from a menu item label. */
  private static String commandLabel(MenuItem menuItem) {
    String label = menuItem.getText();
    if (label == null) {
      return "";
    }
    int tab = label.indexOf('\t');
    if (tab >= 0) {
      label = label.substring(0, tab);
    }
    return label.replace("&", "").trim();
  }

  private static String groupLabel(String type, int count) {
    String name = Utils.isEmpty(type) ? "?" : type;
    return name + "  (" + count + ")";
  }

  private void selectFirstLeaf() {
    List<TreeItem> leaves = leaves();
    if (!leaves.isEmpty()) {
      wTree.setSelection(leaves.get(0));
    }
  }

  private void updateShowAll() {
    if (fullResultCount > 0) {
      wShowAll.setText(
          BaseMessages.getString(
              PKG, "SearchEverywhereDialog.ShowAll.Label", Integer.toString(fullResultCount)));
    } else {
      wShowAll.setText(BaseMessages.getString(PKG, "SearchEverywhereDialog.Hint.Label"));
    }
    wShowAll.requestLayout();
  }

  private void openSelected() {
    TreeItem[] selection = wTree.getSelection();
    if (selection.length == 0) {
      return;
    }
    Object data = selection[0].getData();
    if (data == null) {
      // A group header was selected; do nothing.
      return;
    }
    if (data instanceof ISearchResult result) {
      openResult(result);
    } else if (data instanceof MenuItem menuItem) {
      persistHistory();
      dispose();
      if (!menuItem.isDisposed()) {
        menuItem.notifyListeners(SWT.Selection, new Event());
      }
    }
  }

  private void openResult(ISearchResult result) {
    ISearchable searchable = result.getMatchingSearchable();
    persistHistory();
    dispose();
    try {
      searchable.getSearchCallback().callback(searchable, result);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "SearchEverywhereDialog.Error.Title"),
          BaseMessages.getString(
              PKG,
              "SearchEverywhereDialog.Error.Open.Message",
              Const.NVL(searchable.getName(), "")),
          e);
    }
  }

  /**
   * Hand the current query off for deep browsing into a Search tab in the bottom dock. Each "Show
   * all" opens a new tab so several searches can be kept side by side.
   */
  private void openShowAll() {
    String searchString = wSearch.getText();
    boolean caseSensitive = wCaseSensitive.getSelection();
    boolean regExp = wRegEx.getSelection();
    persistHistory();
    dispose();

    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return;
    }
    Control content =
        dock.openToolTab(
            searchTabTitle(searchString),
            GuiResource.getInstance().getImage("ui/images/search.svg", 16, 16),
            true,
            container -> new HopGuiSearchResultsPanel(container, hopGui));
    if (content instanceof HopGuiSearchResultsPanel panel) {
      panel.setSearchQuery(searchString, caseSensitive, regExp);
      panel.focusSearchField();
    }
  }

  /** A dock tab title for a search: the query itself (truncated), so multiple tabs are tellable. */
  private String searchTabTitle(String query) {
    String title = query == null ? "" : query.trim();
    if (title.isEmpty()) {
      return BaseMessages.getString(PKG, "SearchEverywhereDialog.ShowAll.TabTitle");
    }
    if (title.length() > 30) {
      title = title.substring(0, 29) + "…";
    }
    return title;
  }

  /** Load the shared search history (also used by the search perspective) into the combo. */
  private void loadHistory() {
    try {
      wSearch.setItems(
          AuditManagerGuiUtil.getLastUsedValues(HopGuiSearchHelper.AUDIT_TYPE_SEARCH_STRING));
    } catch (Exception e) {
      hopGui.getLog().logError("Error reading search history", e);
    }
  }

  /** Remember the current search string in the shared history when a result is actually used. */
  private void persistHistory() {
    if (wSearch != null && !wSearch.isDisposed()) {
      String text = wSearch.getText();
      if (!Utils.isEmpty(text)) {
        AuditManagerGuiUtil.addLastUsedValue(HopGuiSearchHelper.AUDIT_TYPE_SEARCH_STRING, text);
      }
    }
  }

  private void dispose() {
    if (shell != null && !shell.isDisposed()) {
      // Remember the size and position for next time.
      props.setScreen(new WindowProperty(shell));
      shell.dispose();
    }
  }
}
