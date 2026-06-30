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

public class SearchQuery implements ISearchQuery {
  private String searchString;
  private boolean caseSensitive;
  private boolean regEx;

  /** Lazily built matcher, shared by all search boxes. Rebuilt when the query changes. */
  private SearchMatcher matcher;

  public SearchQuery() {}

  public SearchQuery(String searchString, boolean caseSensitive, boolean regEx) {
    this.searchString = searchString;
    this.caseSensitive = caseSensitive;
    this.regEx = regEx;
  }

  private SearchMatcher matcher() {
    if (matcher == null) {
      // Content matching: normalized + multi-term substring (or regex). No fuzzy matching here -
      // fuzzy is only meaningful for short names, not for long descriptions / settings values.
      matcher = new SearchMatcher(searchString, caseSensitive, regEx, false);
    }
    return matcher;
  }

  @Override
  public boolean matches(String string) {
    return matcher().matches(string);
  }

  /**
   * Gets searchString
   *
   * @return value of searchString
   */
  @Override
  public String getSearchString() {
    return searchString;
  }

  /**
   * @param searchString The searchString to set
   */
  public void setSearchString(String searchString) {
    this.searchString = searchString;
    this.matcher = null;
  }

  /**
   * Gets caseSensitive
   *
   * @return value of caseSensitive
   */
  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * @param caseSensitive The caseSensitive to set
   */
  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    this.matcher = null;
  }

  /**
   * Gets regEx
   *
   * @return value of regEx
   */
  @Override
  public boolean isRegEx() {
    return regEx;
  }

  /**
   * @param regEx The regEx to set
   */
  public void setRegEx(boolean regEx) {
    this.regEx = regEx;
    this.matcher = null;
  }
}
