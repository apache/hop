package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.eclipse.swt.widgets.Composite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class helps the perspective plugins to keep track of their visualisation.
 * The main principle is that a perspective has it's own composite and draws on it.  It's shown or not depending on what is selected.
 */
public class HopGuiPerspectiveManager {

  private HopGui hopGui;
  private Composite mainPerspectiveComposite;

  private Map<Class<? extends IHopPerspective>, IHopPerspective> perspectivesMap;

  public HopGuiPerspectiveManager( HopGui hopGui, Composite mainPerspectiveComposite ) {
    this.hopGui = hopGui;
    this.mainPerspectiveComposite = mainPerspectiveComposite;
    this.perspectivesMap = new HashMap<>();
  }

  public void addPerspective( IHopPerspective perspective ) {
    perspectivesMap.put( perspective.getClass(), perspective );
  }

  public IHopPerspective getComposite( Class<? extends IHopPerspective> perspectiveClass ) {
    return perspectivesMap.get( perspectiveClass );
  }

  public void showPerspective( Class<? extends IHopPerspective> perspectiveClass ) {
    // Hide all perspectives but one...
    //
    for ( IHopPerspective perspective : perspectivesMap.values() ) {
      if ( perspective.getClass().equals( perspectiveClass ) ) {
        perspective.show();
        hopGui.setActivePerspective( perspective );
      } else {
        perspective.hide();
      }
    }
    mainPerspectiveComposite.layout();
  }

  public IHopPerspective findPerspective( Class<? extends IHopPerspective> perspectiveClass ) {
    for ( IHopPerspective perspective : perspectivesMap.values() ) {
      if ( perspective.getClass().equals( perspectiveClass ) ) {
        return perspective;
      }
    }
    return null;
  }

  /**
   * Loop over all perspectives and see if any one of them recognises the object
   *
   * @param fileMetadata
   * @return
   */
  public HopFileTypeInterface findFileTypeHandler( IHasFilename fileMetadata ) {
    for ( IHopPerspective perspective : getPerspectives() ) {
      for ( HopFileTypeInterface fileType : perspective.getSupportedHopFileTypes() ) {
        if ( fileType.supportsFile( fileMetadata ) ) {
          return fileType;
        }
      }
    }
    return new EmptyFileType();
  }


  /**
   * Get a copy of all the handled/registered perspectives
   *
   * @return All perspectives copied over in a new list
   */
  public List<IHopPerspective> getPerspectives() {
    return new ArrayList<>( perspectivesMap.values() );
  }

}
