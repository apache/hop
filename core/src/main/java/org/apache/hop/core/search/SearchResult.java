package org.apache.hop.core.search;

public class SearchResult implements ISearchResult {

  private ISearchable matchingSearchable;
  private String matchingString;
  private String description;
  private String component;

  public SearchResult( ISearchable matchingSearchable, String matchingString, String description ) {
    this(matchingSearchable, matchingString, description, null);
  }

  public SearchResult( ISearchable matchingSearchable, String matchingString, String description, String component ) {
    this.matchingSearchable = matchingSearchable;
    this.matchingString = matchingString;
    this.description = description;
    this.component = component;
  }

  /**
   * Gets matchingSearchable
   *
   * @return value of matchingSearchable
   */
  @Override public ISearchable getMatchingSearchable() {
    return matchingSearchable;
  }

  /**
   * @param matchingSearchable The matchingSearchable to set
   */
  public void setMatchingSearchable( ISearchable matchingSearchable ) {
    this.matchingSearchable = matchingSearchable;
  }

  /**
   * Gets matchingString
   *
   * @return value of matchingString
   */
  @Override public String getMatchingString() {
    return matchingString;
  }

  /**
   * @param matchingString The matchingString to set
   */
  public void setMatchingString( String matchingString ) {
    this.matchingString = matchingString;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  @Override public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets component
   *
   * @return value of component
   */
  @Override public String getComponent() {
    return component;
  }

  /**
   * @param component The component to set
   */
  public void setComponent( String component ) {
    this.component = component;
  }
}
