package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

/**
 * This interface describes the methods of a Hop GUI perspective.
 *
 */
public interface IHopPerspective {

  /**
   * Get the active file type handler capable of saving, executing, printing, ... a file
   * @return The active file type handler
   */
  HopFileTypeHandlerInterface getActiveFileTypeHandler();

  /**
   * Switch to this perspective.
   */
  void show();

  /**
   * Hide this perspective.
   */
  void hide();

  /**
   * See if this perspective is active (shown)
   * @return True if the perspective is currently shown
   */
  boolean isActive();

  /**
   * Initialize this perspective
   * @param hopGui The parent Hop GUI
   * @parem parent the parent perspectives composite
   */
  void initialize( HopGui hopGui, Composite parent );

  /**
   * @return The composite of this perspective
   */
  Composite getComposite();

  /**
   * @return The layout data of the composite of this perspective
   */
  FormData getFormData();
}
