package org.apache.hop.core.search;

import org.apache.commons.lang.StringUtils;

public class SearchQuery implements ISearchQuery {
  private String searchString;
  private boolean caseSensitive;
  private boolean regEx;

  public SearchQuery() {
  }

  public SearchQuery( String searchString, boolean caseSensitive, boolean regEx ) {
    this.searchString = searchString;
    this.caseSensitive = caseSensitive;
    this.regEx = regEx;
  }

  public boolean matches(String string) {
    if ( StringUtils.isEmpty(searchString)) {
      // match everything non-null
      //
      return StringUtils.isNotEmpty( string );
    }

    if (regEx) {
      if (caseSensitive) {
        return string.matches( searchString );
      } else {
        return string.toLowerCase().matches( searchString.toLowerCase() );
      }
    } else {
      if (caseSensitive) {
        return string.contains( searchString );
      } else {
        return string.toLowerCase().contains( searchString.toLowerCase() );
      }
    }
  }

  /**
   * Gets searchString
   *
   * @return value of searchString
   */
  @Override public String getSearchString() {
    return searchString;
  }

  /**
   * @param searchString The searchString to set
   */
  public void setSearchString( String searchString ) {
    this.searchString = searchString;
  }

  /**
   * Gets caseSensitive
   *
   * @return value of caseSensitive
   */
  @Override public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * @param caseSensitive The caseSensitive to set
   */
  public void setCaseSensitive( boolean caseSensitive ) {
    this.caseSensitive = caseSensitive;
  }

  /**
   * Gets regEx
   *
   * @return value of regEx
   */
  @Override public boolean isRegEx() {
    return regEx;
  }

  /**
   * @param regEx The regEx to set
   */
  public void setRegEx( boolean regEx ) {
    this.regEx = regEx;
  }
}
