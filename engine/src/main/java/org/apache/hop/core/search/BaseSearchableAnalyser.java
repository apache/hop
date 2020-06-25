package org.apache.hop.core.search;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseSearchableAnalyser<T> {

  protected void matchProperty( ISearchable<T> parent, List<ISearchResult> searchResults, ISearchQuery searchQuery, String propertyName, String propertyValue) {
    if (searchQuery.matches( propertyName )) {
      searchResults.add(new SearchResult(parent, propertyName, "matching property name: "+propertyName) );
    }
    if ( StringUtils.isNotEmpty(propertyValue)) {
      if ( searchQuery.matches( propertyValue ) ) {
        searchResults.add( new SearchResult( parent, propertyValue, "matching property value: " + propertyValue ) );
      }
    }
  }

  protected void matchObjectFields(ISearchable<T> searchable, List<ISearchResult> searchResults, ISearchQuery searchQuery, Object object, String descriptionPrefix) {
    List<StringSearchResult> stringSearchResults = new ArrayList<>();
    StringSearcher.findMetaData( object, 1, stringSearchResults, searchable.getSearchableObject(), searchable );
    for ( StringSearchResult stringSearchResult : stringSearchResults ) {
      if (searchQuery.matches( stringSearchResult.getFieldName() )) {
        searchResults.add( new SearchResult( searchable, stringSearchResult.getFieldName(), descriptionPrefix + " : " + stringSearchResult.getFieldName() ) );
      }
      if (searchQuery.matches( stringSearchResult.getString() )) {
        searchResults.add( new SearchResult( searchable, stringSearchResult.getString(), descriptionPrefix + " : " + stringSearchResult.getFieldName() ) );
      }
    }
  }
}
