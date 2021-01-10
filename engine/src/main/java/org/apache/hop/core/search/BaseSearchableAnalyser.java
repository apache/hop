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

package org.apache.hop.core.search;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseSearchableAnalyser<T> {

  protected void matchProperty( ISearchable<T> parent, List<ISearchResult> searchResults, ISearchQuery searchQuery, String propertyName, String propertyValue, String component) {
    if (searchQuery.matches( propertyName )) {
      searchResults.add(new SearchResult(parent, propertyName, "matching property name: "+propertyName, component) );
    }
    if ( StringUtils.isNotEmpty(propertyValue)) {
      if ( searchQuery.matches( propertyValue ) ) {
        searchResults.add( new SearchResult( parent, propertyValue, "matching property value: " + propertyValue, component ) );
      }
    }
  }

  protected void matchObjectFields(ISearchable<T> searchable, List<ISearchResult> searchResults, ISearchQuery searchQuery, Object object, String descriptionPrefix, String component) {
    List<StringSearchResult> stringSearchResults = new ArrayList<>();
    StringSearcher.findMetaData( object, 1, stringSearchResults, searchable.getSearchableObject(), searchable );
    for ( StringSearchResult stringSearchResult : stringSearchResults ) {
      if (searchQuery.matches( stringSearchResult.getFieldName() )) {
        searchResults.add( new SearchResult( searchable, stringSearchResult.getFieldName(), descriptionPrefix + " : " + stringSearchResult.getFieldName(), component ) );
      }
      if (searchQuery.matches( stringSearchResult.getString() )) {
        searchResults.add( new SearchResult( searchable, stringSearchResult.getString(), descriptionPrefix + " : " + stringSearchResult.getFieldName(), component ) );
      }
    }
  }
}
