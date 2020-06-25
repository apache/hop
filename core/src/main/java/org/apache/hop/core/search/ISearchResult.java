package org.apache.hop.core.search;

/**
 * After searching this describes a match
 */
public interface ISearchResult {

  /**
   * @return The matching searchable
   */
  ISearchable getMatchingSearchable();

  String getMatchingString();

  String getDescription();

}
