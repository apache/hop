package org.apache.hop.core.search;

public interface ISearchQuery {
  String getSearchString();
  boolean isCaseSensitive();
  boolean isRegEx();

  /* Match the actual string */
  boolean matches(String string);
}
