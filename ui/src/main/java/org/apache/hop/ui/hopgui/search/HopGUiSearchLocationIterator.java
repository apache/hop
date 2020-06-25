package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Lazily load the searchables during next()
 */
public class HopGUiSearchLocationIterator implements Iterator<ISearchable> {

  private HopGui hopGui;
  private HopGuiSearchLocation location;
  private List<ISearchable> searchables;
  private Iterator<ISearchable> searchableIterator;

  public HopGUiSearchLocationIterator( HopGui hopGui, HopGuiSearchLocation location ) throws HopException {
    this.hopGui = hopGui;
    this.location = location;

    searchables = new ArrayList<>();

    // Get a list of searchables from every perspective
    //
    for ( IHopPerspective perspective : hopGui.getPerspectiveManager().getPerspectives() ) {
      searchables.addAll( perspective.getSearchables() );
    }

    // Add the available metadata objects
    //
    for ( Class<IHopMetadata> metadataClass : hopGui.getMetadataProvider().getMetadataClasses() ) {
      IHopMetadataSerializer<IHopMetadata> serializer = hopGui.getMetadataProvider().getSerializer( metadataClass );
      for ( final String metadataName : serializer.listObjectNames() ) {
        IHopMetadata hopMetadata = serializer.load( metadataName );
        HopGuiMetadataSearchable searchable = new HopGuiMetadataSearchable( hopGui.getMetadataProvider(), serializer, hopMetadata, serializer.getManagedClass() );
        searchables.add( searchable );
      }
    }

    // the described variables in HopConfig...
    //
    List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
    for ( DescribedVariable describedVariable : describedVariables ) {
      searchables.add( new HopGuiDescribedVariableSearchable( describedVariable, null ) );
    }

    searchableIterator = searchables.iterator();
  }

  @Override public boolean hasNext() {
    return searchableIterator.hasNext();
  }

  @Override public ISearchable next() {
    return searchableIterator.next();
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Gets location
   *
   * @return value of location
   */
  public HopGuiSearchLocation getLocation() {
    return location;
  }

  /**
   * @param location The location to set
   */
  public void setLocation( HopGuiSearchLocation location ) {
    this.location = location;
  }
}
