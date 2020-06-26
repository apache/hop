package org.apache.hop.core.search;

import java.util.List;

/**
 * Describe a list of searchable objects at a certain location.
 */
public interface ISearchableProvider {
  List<ISearchablesLocation> getSearchablesLocations();
}
