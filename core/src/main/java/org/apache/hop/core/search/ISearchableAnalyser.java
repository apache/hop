package org.apache.hop.core.search;

import java.util.List;

/**
 * This object is capable of analysing an object, recognising the type and then to search its content.
 */
public interface ISearchableAnalyser<T> {

  Class<T> getSearchableClass();
  List<ISearchResult> search(ISearchable<T> searchable, ISearchQuery searchQuery);
}
