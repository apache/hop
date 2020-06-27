package org.apache.hop.core.search;

import org.apache.hop.core.exception.HopException;

public interface ISearchableCallback {
  /**
   * When a search result is selected, call this method.
   * It can be used to open the searchable in the GUI
   *
   * @param searchable
   * @param searchResult
   * @throws HopException
   */
  public void callback(ISearchable searchable, ISearchResult searchResult) throws HopException;
}
