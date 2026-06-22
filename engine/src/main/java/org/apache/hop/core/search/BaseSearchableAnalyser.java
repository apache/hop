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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;

public abstract class BaseSearchableAnalyser<T> {

  /**
   * Match a single property of a searchable. Only the property <em>value</em> (the actual content)
   * is matched - not the internal property name/label - so the search behaves like a content search
   * instead of matching field labels. When it matches, the property name is recorded as the result
   * description so the UI can show <em>where</em> the hit was.
   *
   * @param propertyName a human-readable label for the property (e.g. "pipeline description"); used
   *     only to describe the match, it is not matched against
   * @param propertyValue the actual value that is matched against
   */
  protected void matchProperty(
      ISearchable<T> parent,
      List<ISearchResult> searchResults,
      ISearchQuery searchQuery,
      String propertyName,
      String propertyValue,
      String component) {
    if (StringUtils.isNotEmpty(propertyValue) && searchQuery.matches(propertyValue)) {
      searchResults.add(
          new SearchResult(parent, propertyValue, propertyName, component, propertyValue));
    }
  }

  /**
   * Reflectively match the string fields of an object (e.g. a transform's or action's settings).
   * Only field <em>values</em> are matched, not the Java field names. The field name is recorded in
   * the description so the UI can show which setting matched.
   */
  protected void matchObjectFields(
      ISearchable<T> searchable,
      List<ISearchResult> searchResults,
      ISearchQuery searchQuery,
      Object object,
      String descriptionPrefix,
      String component) {
    List<StringSearchResult> stringSearchResults = new ArrayList<>();
    StringSearcher.findMetaData(
        object, 1, stringSearchResults, searchable.getSearchableObject(), searchable);
    for (StringSearchResult stringSearchResult : stringSearchResults) {
      String resultString = stringSearchResult.getString();
      if (StringUtils.isNotEmpty(resultString) && searchQuery.matches(resultString)) {
        searchResults.add(
            new SearchResult(
                searchable,
                resultString,
                descriptionPrefix + " : " + stringSearchResult.getFieldName(),
                component,
                resultString));
      }
    }
  }
}
