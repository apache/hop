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
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.search.SearchQuery;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

/**
 * The full search-results view (toolbar + grouped result tree + details panel), as a reusable
 * {@link Composite}. It runs the same searchable-analyser pipeline as the {@link
 * SearchEverywhereDialog} popup across all locations and renders the ranked results as the two-tier
 * section &#8594; type &#8594; object &#8594; matches tree.
 *
 * <p>This used to be the body of the dedicated search perspective; it is now embeddable anywhere
 * (e.g. as a tab in the bottom dock) so a single results UI is shared by every "show all" entry
 * point.
 */
public class HopGuiSearchResultsPanel extends Composite {

  private static final Class<?> PKG = HopGuiSearchResultsPanel.class;

  /** Persisted state (sash weights). Kept under the old key so existing weights still load. */
  private static final String SEARCH_AUDIT_TYPE = "search-perspective-state";

  private static final String STATE_SASH_WEIGHTS_KEY = "sash-weights";
  private static final String STATE_SASH_WEIGHTS_PROP = "weights";
  private static final int[] DEFAULT_SASH_WEIGHTS = {70, 30};

  /** Debounce in ms between a keystroke and running the search. */
  private static final int SEARCH_DELAY_MS = 300;

  private final HopGui hopGui;
  private final String eventListenerId;
  private List<ISearchablesLocation> searchablesLocations;

  private Combo wSearchString;
  private Button wCaseSensitive;
  private Button wRegEx;
  private SashForm sash;
  private Tree wTree;
  private Button wbOpen;

  /** Read-only labels that make up the details panel on the right. */
  private Label wdType;

  private Label wdName;
  private Label wdFile;
  private Label wdLocation;
  private Label wdMatch;
  private Label wdValue;
  private Label wdDescription;

  /** Searchables enumerated once across all locations and reused for every keystroke. */
  private HopGuiSearchHelper.EnumeratedSearchables cachedEnumeration;

  private Map<Class<ISearchableAnalyser>, ISearchableAnalyser> cachedAnalysers;

  private final Runnable searchRunnable = () -> search(new Event());

  public HopGuiSearchResultsPanel(Composite parent, HopGui hopGui) {
    super(parent, SWT.NONE);
    this.hopGui = hopGui;
    this.searchablesLocations = hopGui.getSearchablesLocations();
    this.eventListenerId = getClass().getName() + "-" + System.identityHashCode(this);

    buildUi();

    // Clear results when the active project changes (the searchables become stale).
    hopGui
        .getEventsHandler()
        .addEventListener(
            eventListenerId,
            e -> hopGui.getDisplay().asyncExec(this::clearSearchFilters),
            HopGuiEvents.ProjectActivated.name());

    addDisposeListener(
        e -> {
          // The Dispose event fires before the child sash is released, so weights are still valid.
          saveState();
          hopGui.getEventsHandler().removeEventListeners(eventListenerId);
        });
  }

  private void buildUi() {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    PropsUi.setLook(this);
    FormLayout layout = new FormLayout();
    layout.marginLeft = margin;
    layout.marginTop = margin;
    layout.marginRight = margin;
    layout.marginBottom = margin;
    setLayout(layout);

    // --- Top toolbar: search field, case/regex toggles, refresh ---
    //
    Composite toolbar = new Composite(this, SWT.NONE);
    PropsUi.setLook(toolbar);
    toolbar.setLayout(new GridLayout(5, false));
    FormData fdToolbar = new FormData();
    fdToolbar.left = new FormAttachment(0, 0);
    fdToolbar.right = new FormAttachment(100, 0);
    fdToolbar.top = new FormAttachment(0, 0);
    toolbar.setLayoutData(fdToolbar);

    Label wlSearch = new Label(toolbar, SWT.LEFT);
    PropsUi.setLook(wlSearch);
    wlSearch.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Label.Search"));

    wSearchString = new Combo(toolbar, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(wSearchString);
    wSearchString.setFont(GuiResource.getInstance().getFontBold());
    wSearchString.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    wSearchString.addListener(SWT.Modify, e -> scheduleSearch());
    wSearchString.addListener(
        SWT.DefaultSelection,
        e -> {
          persistSearchString();
          search(e);
        });

    wCaseSensitive = new Button(toolbar, SWT.CHECK);
    PropsUi.setLook(wCaseSensitive);
    wCaseSensitive.setText(
        BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Option.CaseSensitive"));
    wCaseSensitive.addListener(SWT.Selection, e -> scheduleSearch());

    wRegEx = new Button(toolbar, SWT.CHECK);
    PropsUi.setLook(wRegEx);
    wRegEx.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Option.RegEx"));
    wRegEx.addListener(SWT.Selection, e -> scheduleSearch());

    Button wbRefresh = new Button(toolbar, SWT.PUSH);
    PropsUi.setLook(wbRefresh);
    wbRefresh.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Refresh.Button.Label"));
    wbRefresh.setToolTipText(
        BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Refresh.Button.Tooltip"));
    wbRefresh.addListener(
        SWT.Selection,
        e -> {
          invalidateCache();
          search(e);
        });

    // --- Bottom: Open button ---
    //
    wbOpen = new Button(this, SWT.PUSH);
    PropsUi.setLook(wbOpen);
    wbOpen.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Open.Button.Label"));
    FormData fdbOpen = new FormData();
    fdbOpen.right = new FormAttachment(100, 0);
    fdbOpen.bottom = new FormAttachment(100, 0);
    wbOpen.setLayoutData(fdbOpen);
    wbOpen.addListener(SWT.Selection, this::open);
    wbOpen.setEnabled(false);

    // --- Middle: grouped result tree | details panel ---
    //
    sash = new SashForm(this, SWT.HORIZONTAL);
    PropsUi.setLook(sash);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.top = new FormAttachment(toolbar, margin);
    fdSash.bottom = new FormAttachment(wbOpen, -margin);
    sash.setLayoutData(fdSash);

    wTree =
        new Tree(sash, SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL | SWT.FULL_SELECTION | SWT.BORDER);
    PropsUi.setLook(wTree);
    wTree.setHeaderVisible(true);
    TreeColumn nameColumn = new TreeColumn(wTree, SWT.LEFT);
    nameColumn.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Column.Name"));
    nameColumn.setWidth((int) (340 * props.getZoomFactor()));
    TreeColumn matchColumn = new TreeColumn(wTree, SWT.LEFT);
    matchColumn.setText(BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Column.Match"));
    matchColumn.setWidth((int) (320 * props.getZoomFactor()));
    wTree.addListener(SWT.Selection, e -> treeSelectionChanged());
    wTree.addListener(SWT.DefaultSelection, this::open);

    // The last column (Matched text) always fills the remaining width.
    wTree.addListener(SWT.Resize, e -> fillLastColumn());
    nameColumn.addListener(SWT.Resize, e -> fillLastColumn());

    buildDetailsPanel(sash);

    restoreSashWeights();
  }

  private void buildDetailsPanel(Composite parent) {
    Composite details = new Composite(parent, SWT.NONE);
    PropsUi.setLook(details);
    details.setLayout(new GridLayout(2, false));

    wdType = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Type");
    wdName = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Name");
    wdFile = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.File");
    wdLocation = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Location");
    wdMatch = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Match");
    wdValue = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Value");
    wdDescription = addDetailRow(details, "HopGuiSearchResultsPanel.Detail.Description");
  }

  private Label addDetailRow(Composite details, String captionKey) {
    Label caption = new Label(details, SWT.LEFT);
    PropsUi.setLook(caption);
    caption.setText(BaseMessages.getString(PKG, captionKey) + ":");
    caption.setFont(GuiResource.getInstance().getFontBold());
    caption.setLayoutData(new GridData(SWT.LEFT, SWT.TOP, false, false));

    Label value = new Label(details, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(value);
    value.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
    return value;
  }

  /**
   * Pre-fill the search field with the given query and run it immediately. Used by the global
   * search popup to hand a query off for deep browsing ("Show all results").
   */
  public void setSearchQuery(String searchString, boolean caseSensitive, boolean regEx) {
    if (wSearchString == null || wSearchString.isDisposed()) {
      return;
    }
    wSearchString.setText(Const.NVL(searchString, ""));
    wCaseSensitive.setSelection(caseSensitive);
    wRegEx.setSelection(regEx);
    search(new Event());
  }

  /** Refresh searchable locations + history and re-enumerate; call when this panel is (re)shown. */
  public void prepareForActivation() {
    searchablesLocations = hopGui.getSearchablesLocations();
    refreshLastUsedSearchStrings();
    invalidateCache();
    focusSearchField();
  }

  public void focusSearchField() {
    if (wSearchString != null && !wSearchString.isDisposed()) {
      wSearchString.setFocus();
    }
  }

  private void refreshLastUsedSearchStrings() {
    if (wSearchString != null && !wSearchString.isDisposed()) {
      try {
        String[] lastUsedValues =
            AuditManagerGuiUtil.getLastUsedValues(HopGuiSearchHelper.AUDIT_TYPE_SEARCH_STRING);
        // Keep what the user is typing; only refresh the dropdown history.
        String current = wSearchString.getText();
        wSearchString.setItems(lastUsedValues);
        wSearchString.setText(current);
        wSearchString.setSelection(new Point(current.length(), current.length()));
      } catch (Exception e) {
        hopGui.getLog().logError("Error reading list of used search strings", e);
      }
    }
  }

  private void scheduleSearch() {
    if (isDisposed()) {
      return;
    }
    getDisplay().timerExec(-1, searchRunnable);
    getDisplay().timerExec(SEARCH_DELAY_MS, searchRunnable);
  }

  /** Size the last column (Matched text) to take whatever width is left after the Name column. */
  private void fillLastColumn() {
    if (wTree == null || wTree.isDisposed() || wTree.getColumnCount() < 2) {
      return;
    }
    int width = wTree.getClientArea().width;
    if (width <= 0) {
      return;
    }
    TreeColumn[] columns = wTree.getColumns();
    columns[1].setWidth(Math.max(150, width - columns[0].getWidth()));
  }

  /** Restore the remembered sash weights, using the same audit-state mechanism as perspectives. */
  private void restoreSashWeights() {
    int[] weights = DEFAULT_SASH_WEIGHTS;
    try {
      AuditStateMap stateMap =
          AuditManager.getActive()
              .loadAuditStateMap(HopNamespace.getNamespace(), SEARCH_AUDIT_TYPE);
      AuditState state = stateMap.get(STATE_SASH_WEIGHTS_KEY);
      if (state != null) {
        Object value = state.getStateMap().get(STATE_SASH_WEIGHTS_PROP);
        if (value != null) {
          String[] parts = value.toString().split(",");
          if (parts.length >= 2) {
            weights =
                new int[] {Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim())};
          }
        }
      }
    } catch (Exception e) {
      hopGui.getLog().logError("Error restoring search results sash weights", e);
    }
    sash.setWeights(weights);
  }

  /** Persist the sash weights. */
  public void saveState() {
    if (sash == null || sash.isDisposed()) {
      return;
    }
    try {
      int[] weights = sash.getWeights();
      if (weights == null || weights.length < 2) {
        return;
      }
      AuditStateMap stateMap = new AuditStateMap();
      stateMap.add(
          new AuditState(
              STATE_SASH_WEIGHTS_KEY,
              Map.of(STATE_SASH_WEIGHTS_PROP, weights[0] + "," + weights[1])));
      AuditManager.getActive()
          .saveAuditStateMap(HopNamespace.getNamespace(), SEARCH_AUDIT_TYPE, stateMap);
    } catch (Exception e) {
      hopGui.getLog().logError("Error saving search results sash weights", e);
    }
  }

  public void clearSearchFilters() {
    invalidateCache();
    if (wSearchString != null && !wSearchString.isDisposed()) {
      wSearchString.setText("");
    }
    if (wTree != null && !wTree.isDisposed()) {
      wTree.removeAll();
    }
    clearDetails();
    if (wbOpen != null && !wbOpen.isDisposed()) {
      wbOpen.setEnabled(false);
    }
  }

  private void invalidateCache() {
    cachedEnumeration = null;
  }

  /** Make sure the searchables across all locations are enumerated once (and cached). */
  private boolean ensureLoaded() {
    if (cachedEnumeration != null) {
      return true;
    }
    try {
      if (cachedAnalysers == null) {
        cachedAnalysers = HopGuiSearchHelper.loadSearchableAnalysers();
      }
      cachedEnumeration =
          HopGuiSearchHelper.enumerateAll(
              searchablesLocations,
              hopGui.getMetadataProvider(),
              hopGui.getVariables(),
              hopGui.getLog());
      return true;
    } catch (Exception e) {
      hopGui.getLog().logError("Error loading searchables", e);
      invalidateCache();
      return false;
    }
  }

  private void search(Event event) {
    if (wTree == null || wTree.isDisposed()) {
      return;
    }
    wTree.removeAll();
    clearDetails();
    wbOpen.setEnabled(false);

    String searchString = wSearchString.getText();
    if (Utils.isEmpty(searchString)) {
      return;
    }

    if (searchablesLocations == null) {
      searchablesLocations = hopGui.getSearchablesLocations();
    }
    if (!ensureLoaded()) {
      return;
    }

    SearchQuery query =
        new SearchQuery(searchString, wCaseSensitive.getSelection(), wRegEx.getSelection());

    // The searchables were enumerated once; here we only run the cheap in-memory matching.
    // Errors are logged (not shown in a modal dialog) since this runs on every debounced keystroke.
    List<ISearchResult> results;
    try {
      results =
          HopGuiSearchHelper.analyseRanked(
              cachedEnumeration.getSearchables(), query, cachedAnalysers, true);
    } catch (Exception e) {
      hopGui.getLog().logError("Error while searching", e);
      results = new ArrayList<>();
    }
    populateTree(results);
  }

  /**
   * Render the two-tier tree: section (Open/Project) -> type -> object -> the matches inside it.
   */
  private void populateTree(List<ISearchResult> results) {
    String searchTerm = wRegEx.getSelection() ? null : wSearchString.getText();

    TreeItem firstLeaf = null;
    for (HopGuiSearchHelper.SearchSection section :
        HopGuiSearchHelper.groupResults(results, cachedEnumeration.getSourceByKey())) {
      TreeItem sectionItem = new TreeItem(wTree, SWT.NONE);
      sectionItem.setText(
          0, sectionLabel(section.getKey()) + "  (" + section.getObjectCount() + ")");
      sectionItem.setFont(GuiResource.getInstance().getFontBold());

      for (HopGuiSearchHelper.SearchTypeGroup typeGroup : section.getTypeGroups()) {
        TreeItem typeItem = new TreeItem(sectionItem, SWT.NONE);
        typeItem.setText(0, typeGroup.getType() + "  (" + typeGroup.getObjects().size() + ")");
        typeItem.setFont(GuiResource.getInstance().getFontBold());

        for (HopGuiSearchHelper.SearchObjectGroup group : typeGroup.getObjects()) {
          TreeItem objectItem = new TreeItem(typeItem, SWT.NONE);
          objectItem.setText(
              0, HopGuiSearchHelper.displayLabel(group.getSearchable(), hopGui.getVariables()));
          objectItem.setData(group.getPrimary());
          if (firstLeaf == null) {
            firstLeaf = objectItem;
          }
          for (ISearchResult match : group.getMatches()) {
            TreeItem matchItem = new TreeItem(objectItem, SWT.NONE);
            matchItem.setText(0, HopGuiSearchHelper.matchWhere(match));
            matchItem.setText(1, HopGuiSearchHelper.matchSnippet(match, searchTerm));
            matchItem.setData(match);
          }
          objectItem.setExpanded(true);
        }
        typeItem.setExpanded(true);
      }
      sectionItem.setExpanded(true);
    }

    if (firstLeaf != null) {
      wTree.setSelection(firstLeaf);
      treeSelectionChanged();
    }
  }

  private String sectionLabel(String sectionKey) {
    if (HopGuiSearchHelper.SECTION_OPEN.equals(sectionKey)) {
      return BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Section.Open");
    }
    return BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Section.Project");
  }

  private void treeSelectionChanged() {
    TreeItem[] selection = wTree.getSelection();
    boolean isResult = selection.length > 0 && selection[0].getData() instanceof ISearchResult;
    wbOpen.setEnabled(isResult);
    if (isResult) {
      showDetails((ISearchResult) selection[0].getData());
    } else {
      clearDetails();
    }
  }

  private void showDetails(ISearchResult result) {
    ISearchable searchable = result.getMatchingSearchable();
    wdType.setText(Const.NVL(searchable.getType(), ""));
    wdName.setText(Const.NVL(searchable.getName(), ""));
    wdFile.setText(
        HopGuiSearchHelper.sanitizePath(searchable.getFilename(), hopGui.getVariables()));
    wdLocation.setText(Const.NVL(searchable.getLocation(), ""));
    wdMatch.setText(Const.NVL(result.getMatchingString(), ""));
    wdValue.setText(Const.NVL(result.getValue(), ""));
    wdDescription.setText(Const.NVL(Const.NVL(result.getComponent(), result.getDescription()), ""));
    wdType.getParent().layout();
  }

  private void clearDetails() {
    for (Label label :
        new Label[] {wdType, wdName, wdFile, wdLocation, wdMatch, wdValue, wdDescription}) {
      if (label != null && !label.isDisposed()) {
        label.setText("");
      }
    }
  }

  private void persistSearchString() {
    if (wSearchString != null && !wSearchString.isDisposed()) {
      String text = wSearchString.getText();
      if (!Utils.isEmpty(text)) {
        AuditManagerGuiUtil.addLastUsedValue(HopGuiSearchHelper.AUDIT_TYPE_SEARCH_STRING, text);
      }
    }
  }

  private void open(Event event) {
    TreeItem[] selection = wTree.getSelection();
    if (selection.length == 0 || !(selection[0].getData() instanceof ISearchResult)) {
      return;
    }
    persistSearchString();

    ISearchResult searchResult = (ISearchResult) selection[0].getData();
    ISearchable searchable = searchResult.getMatchingSearchable();
    try {
      searchable.getSearchCallback().callback(searchable, searchResult);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "HopGuiSearchResultsPanel.Error.Open.Title"),
          BaseMessages.getString(
              PKG,
              "HopGuiSearchResultsPanel.Error.Open.Message",
              Const.NVL(searchable.getName(), "")),
          e);
    }
  }
}
