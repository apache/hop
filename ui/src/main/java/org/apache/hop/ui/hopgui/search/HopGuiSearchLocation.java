package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.Iterator;

public class HopGuiSearchLocation implements ISearchablesLocation {
  protected HopGui hopGui;

  public HopGuiSearchLocation( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public String getLocationDescription() {
    return "Current objects loaded in the Hop GUI";
  }

  @Override public Iterator<ISearchable> getSearchables() throws HopException {
    return new HopGUiSearchLocationIterator( hopGui, this );
  }
}
