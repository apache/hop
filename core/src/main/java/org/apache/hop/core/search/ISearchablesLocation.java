package org.apache.hop.core.search;

import org.apache.hop.core.exception.HopException;

import java.util.Iterator;

/**
 * A location where searchables can be found.
 */
public interface ISearchablesLocation {
  String getLocationDescription();
  Iterator<ISearchable> getSearchables() throws HopException;
}
