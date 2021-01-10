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

package org.apache.hop.ui.hopgui.perspective.search;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.search.SearchQuery;
import org.apache.hop.core.search.SearchableAnalyserPluginType;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.shared.AuditManagerGuiUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@HopPerspectivePlugin(
  id = "300-HopSearchPerspective",
  name = "Search",
  description = "The Hop Search Perspective",
  image = "ui/images/search.svg"
)
@GuiPlugin(description="Hop Search Perspective GUI")
public class HopSearchPerspective implements IHopPerspective {

  public static final String ID_PERSPECTIVE_TOOLBAR_ITEM = "20020-perspective-search";

  public static final String AUDIT_TYPE_SEARCH_LOCATION = "search-location";
  public static final String AUDIT_TYPE_SEARCH_STRING = "search-string";

  private HopGui hopGui;
  private Composite parent;
  private Composite composite;
  private FormData formData;
  private List<ISearchablesLocation> searchablesLocations;
  private String[] locations;
  private Combo wLocations;
  private Combo wSearchString;
  private Button wCaseSensitive;
  private Button wRegEx;
  private TableView wResults;
  List<ISearchResult> allSearchResults;

  public HopSearchPerspective() {
  }

  @Override public String getId() {
    return "search";
  }


  public void activate() {
    // Someone clicked on the search icon of used CTRL-F
    //
    hopGui.setActivePerspective( this );
  }
  
  @Override public void perspectiveActivated() {
	    wSearchString.setFocus();

	    // Refresh the list of searchableLocations
	    //
	    searchablesLocations = hopGui.getSearchablesLocations();
	    locations = new String[ searchablesLocations.size() ];
	    for ( int i = 0; i < locations.length; i++ ) {
	      locations[ i ] = searchablesLocations.get( i ).getLocationDescription();
	    }

	    refreshLastUsedLocation();

	    refreshLastUsedSearchStrings();
  }
  
  
  private void refreshLastUsedLocation() {
    if (wLocations!=null && !wLocations.isDisposed()) {
      wLocations.setItems( locations );

      String lastLocation = AuditManagerGuiUtil.getLastUsedValue( AUDIT_TYPE_SEARCH_LOCATION );
      if (lastLocation!=null) {
        int index = Const.indexOfString( lastLocation, locations );
        if (index>=0) {
          wLocations.select(index);
        }
      }
    }
  }

  private void refreshLastUsedSearchStrings() {
    if (wSearchString!=null && !wSearchString.isDisposed()) {
      try {
        String[] lastUsedValues = AuditManagerGuiUtil.getLastUsedValues( AUDIT_TYPE_SEARCH_STRING );
        wSearchString.setItems( lastUsedValues );
      } catch(Exception e) {
        hopGui.getLog().logError( "Error reading list of used search strings", e );
      }
    }
  }

  @Override public IHopFileTypeHandler getActiveFileTypeHandler() {
    return new EmptyHopFileTypeHandler(); // Not handling anything really
  }

  @Override public void setActiveFileTypeHandler( IHopFileTypeHandler activeFileTypeHandler ) {

  }

  @Override public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  @Override
  public boolean isActive() {
	  return hopGui.isActivePerspective(this);
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
    this.hopGui = hopGui;
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();

    composite = new Composite( parent, SWT.NONE );
    props.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginLeft = props.getMargin();
    layout.marginTop = props.getMargin();
    layout.marginLeft = props.getMargin();
    layout.marginBottom = props.getMargin();
    composite.setLayout( layout );

    formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    composite.setLayoutData( formData );

    int margin = (int) ( props.getMargin() * props.getZoomFactor() );

    // Add a simple label to test
    //
    Label wlInfo = new Label( composite, SWT.LEFT );
    props.setLook( wlInfo );
    wlInfo.setText( "Select the location to search in and the search string. Then hit the 'Search' button." );
    wlInfo.setFont( GuiResource.getInstance().getFontBold() );
    FormData fdInfo = new FormData();
    fdInfo.left = new FormAttachment( 0, 0 );
    fdInfo.right = new FormAttachment( 100, 0 );
    fdInfo.top = new FormAttachment( 0, 0 );
    wlInfo.setLayoutData( fdInfo );
    Control lastControl = wlInfo;

    Label wlSep1 = new Label( composite, SWT.SEPARATOR | SWT.HORIZONTAL );
    props.setLook( wlSep1 );
    FormData fdlSep1 = new FormData();
    fdlSep1.left = new FormAttachment( 0, 0 );
    fdlSep1.right = new FormAttachment( 100, 0 );
    fdlSep1.top = new FormAttachment( lastControl, margin );
    wlSep1.setLayoutData( fdlSep1 );
    lastControl = wlSep1;

    // The location
    //
    Label wlLocations = new Label( composite, SWT.LEFT );
    props.setLook( wlLocations );
    wlLocations.setText( "Location:" );
    FormData fdlLocations = new FormData();
    fdlLocations.left = new FormAttachment( 0, 0 );
    fdlLocations.top = new FormAttachment( lastControl, margin );
    wlLocations.setLayoutData( fdlLocations );
    lastControl = wlLocations;

    wLocations = new Combo( composite, SWT.BORDER );
    props.setLook( wLocations );
    FormData fdLocations = new FormData();
    fdLocations.left = new FormAttachment( 0, 0 );
    fdLocations.top = new FormAttachment( lastControl, margin );
    fdLocations.right = new FormAttachment( 50, 0 );
    wLocations.setLayoutData( fdLocations );
    lastControl = wLocations;

    // The search query
    //
    Label wlSearchString = new Label( composite, SWT.LEFT );
    props.setLook( wlSearchString );
    wlSearchString.setText( "Search string:      " );
    FormData fdlSearchString = new FormData();
    fdlSearchString.left = new FormAttachment( 0, 0 );
    fdlSearchString.top = new FormAttachment( lastControl, margin );
    wlSearchString.setLayoutData( fdlSearchString );

    wCaseSensitive = new Button(composite, SWT.CHECK);
    props.setLook( wCaseSensitive );
    wCaseSensitive.setText("Case sensitive?     ");
    FormData fdCaseSensitive = new FormData();
    fdCaseSensitive.left = new FormAttachment(wlSearchString, margin*2);
    fdCaseSensitive.top = new FormAttachment(lastControl, margin);
    wCaseSensitive.setLayoutData( fdCaseSensitive );

    wRegEx = new Button(composite, SWT.CHECK);
    props.setLook( wRegEx );
    wRegEx.setText("Regular expression?     ");
    FormData fdRegEx = new FormData();
    fdRegEx.left = new FormAttachment(wCaseSensitive, margin*2);
    fdRegEx.top = new FormAttachment(lastControl, margin);
    wRegEx.setLayoutData( fdRegEx );
    lastControl = wCaseSensitive;

    wSearchString = new Combo( composite, SWT.BORDER | SWT.SINGLE );
    props.setLook( wSearchString );
    wSearchString.setFont( GuiResource.getInstance().getFontBold() );
    FormData fdSearchString = new FormData();
    fdSearchString.left = new FormAttachment( 0, 0 );
    fdSearchString.top = new FormAttachment( lastControl, margin );
    fdSearchString.right = new FormAttachment( 50, 0 );
    wSearchString.setLayoutData( fdSearchString );
    wSearchString.addListener( SWT.DefaultSelection, this::search );
    lastControl = wSearchString;

    Button wbSearch = new Button( composite, SWT.PUSH );
    props.setLook( wbSearch );
    wbSearch.setText( "  Search  " );
    FormData fdbSearch = new FormData();
    fdbSearch.left = new FormAttachment( 0, 0 );
    fdbSearch.top = new FormAttachment( lastControl, margin );
    wbSearch.setLayoutData( fdbSearch );
    wbSearch.addListener( SWT.Selection, this::search );
    lastControl = wbSearch;

    Button wbOpen = new Button( composite, SWT.PUSH );
    props.setLook( wbOpen );
    wbOpen.setText( "    Open    " );
    FormData fdbOpen = new FormData();
    fdbOpen.left = new FormAttachment( 50, 0 );
    fdbOpen.bottom = new FormAttachment( 100, -margin );
    wbOpen.setLayoutData( fdbOpen );
    wbOpen.addListener( SWT.Selection, this::open );


    // A table with the search results...
    //
    ColumnInfo[] resultsColumns = {
      new ColumnInfo( "Type", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( "File", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( "Location", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( "Matching text", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
      new ColumnInfo( "Description", ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
    };

    wResults = new TableView( hopGui.getVariables(), composite, SWT.V_SCROLL | SWT.V_SCROLL | SWT.SINGLE, resultsColumns, 0, null, props );
    props.setLook( wResults );
    wResults.setReadonly( true );
    FormData fdResults = new FormData();
    fdResults.left = new FormAttachment( 0, 0 );
    fdResults.right = new FormAttachment( 100, 0 );
    fdResults.top = new FormAttachment( lastControl, margin );
    fdResults.bottom = new FormAttachment( wbOpen, -2*margin );
    wResults.setLayoutData( fdResults );
    wResults.table.addListener( SWT.DefaultSelection, this::open );
  }

  private void open( Event event ) {
    int index = wResults.getSelectionIndex();
    if (index<0) {
      return;
    }
    TableItem tableItem = wResults.table.getSelection()[ 0 ];

    // Open the selected item.
    //
    int itemNr = Const.toInt( tableItem.getText(0), -1 );
    if (itemNr<=0) {
      return;
    }

    ISearchResult searchResult = allSearchResults.get( itemNr - 1 );
    ISearchable searchable = searchResult.getMatchingSearchable();
    try {
      searchable.getSearchCallback().callback( searchable, searchResult );
    } catch(Exception e) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error opening "+searchable.getName(), e );
    }
  }

  private void search( Event event ) {

    // Find the search location using the combo
    // Get the list of searchables.
    //
    boolean caseSensitive = wCaseSensitive.getSelection();
    boolean regularExpression = wRegEx.getSelection();

    SearchQuery searchQuery = new SearchQuery( wSearchString.getText(), caseSensitive, regularExpression );

    wResults.table.removeAll();
    allSearchResults = new ArrayList<>();

    try {
      // What is the list of available searchable analysers?
      //
      Map<Class<ISearchableAnalyser>, ISearchableAnalyser> searchableAnalyserMap = new HashMap<>();
      PluginRegistry registry = PluginRegistry.getInstance();
      for ( IPlugin analyserPlugin : registry.getPlugins( SearchableAnalyserPluginType.class ) ) {
        ISearchableAnalyser searchableAnalyser = (ISearchableAnalyser) registry.loadClass( analyserPlugin );
        searchableAnalyserMap.put( searchableAnalyser.getSearchableClass(), searchableAnalyser );
      }

      ISearchablesLocation searchablesLocation = getSelectedSearchLocation();
      if (searchablesLocation==null) {
        return;
      }

      // Save the last used location and search string...
      //
      AuditManagerGuiUtil.addLastUsedValue( AUDIT_TYPE_SEARCH_LOCATION, searchablesLocation.getLocationDescription() );
      AuditManagerGuiUtil.addLastUsedValue( AUDIT_TYPE_SEARCH_STRING, wSearchString.getText() );

      Iterator<ISearchable> iterator = searchablesLocation.getSearchables();
      while ( iterator.hasNext() ) {
        // Load the next object
        //
        ISearchable searchable = iterator.next();

        Object object = searchable.getSearchableObject();
        if (object!=null) {
          // Find an analyser...
          //
          ISearchableAnalyser searchableAnalyser = searchableAnalyserMap.get( object.getClass() );
          if ( searchableAnalyser != null ) {
            List<ISearchResult> searchResults = searchableAnalyser.search( searchable, searchQuery );
            addSearchResults( searchResults );
          }
        }
      }
      wResults.removeEmptyRows();
      wResults.setRowNums();
      wResults.optWidth( true );
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error searching", e );
    } finally {
      refreshLastUsedLocation();
      refreshLastUsedSearchStrings();
    }

  }

  private ISearchablesLocation getSelectedSearchLocation() {
    String locationDescription = wLocations.getText();
    for (ISearchablesLocation searchablesLocation : searchablesLocations) {
      if (searchablesLocation.getLocationDescription().equalsIgnoreCase( locationDescription )) {
        return searchablesLocation;
      }
    }
    return null;
  }

  private void addSearchResults( List<ISearchResult> searchResults ) {

    for ( ISearchResult searchResult : searchResults ) {
      ISearchable searchable = searchResult.getMatchingSearchable();

      TableItem item = new TableItem( wResults.table, SWT.NONE );
      int c = 1;
      item.setText( c++, Const.NVL( searchable.getType(), "" ) );
      item.setText( c++, Const.NVL( searchable.getName(), "" ) );
      item.setText( c++, Const.NVL( searchable.getFilename(), "" ) );
      item.setText( c++, Const.NVL( searchable.getLocation(), "" ) );
      item.setText( c++, Const.NVL( searchResult.getMatchingString(), "" ) );
      item.setText( c++, Const.NVL( searchResult.getDescription(), "" ) );

      allSearchResults.add(searchResult);

    }
  }

  @Override public boolean remove( IHopFileTypeHandler typeHandler ) {
    return false; // Nothing to do here
  }

  @Override public List<TabItemHandler> getItems() {
    return null;
  }

  @Override public void navigateToPreviousFile() {

  }

  @Override public void navigateToNextFile() {

  }

  @Override public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override public boolean hasNavigationNextFile() {
    return false;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public Control getControl() {
    return composite;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

  @Override public List<ISearchable> getSearchables() {
    List<ISearchable> searchables = new ArrayList<>();
    return searchables;
  }
}
