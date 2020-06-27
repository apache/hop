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

  /**
   * @return The transform, action, variable, ... where the string was found or null if the string was found in the searchable itself.
   */
  String getComponent();

}
