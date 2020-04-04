package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

import java.util.List;

/**
 * This interface describes the methods of a Hop GUI perspective.
 */
public interface IHopPerspective extends IActionContextHandlersProvider {

  String getId();

  /**
   * Get the active file type handler capable of saving, executing, printing, ... a file
   *
   * @return The active file type handler
   */
  IHopFileTypeHandler getActiveFileTypeHandler();

  /**
   * Get a list of supported file types for this perspective
   *
   * @return The list of supported file types
   */
  List<IHopFileType> getSupportedHopFileTypes();

  /**
   * Switch to this perspective.
   */
  void show();

  /**
   * Hide this perspective.
   */
  void hide();

  /**
   * Navigate the file usage history to the previous file
   */
  void navigateToPreviousFile();

  /**
   * Navigate the file usage history to the next file
   */
  void navigateToNextFile();

  /**
   * See if this perspective is active (shown)
   *
   * @return True if the perspective is currently shown
   */
  boolean isActive();

  /**
   * Initialize this perspective
   *
   * @param hopGui The parent Hop GUI
   * @parem parent the parent perspectives composite
   */
  void initialize( HopGui hopGui, Composite parent );

  boolean hasNavigationPreviousFile();

  boolean hasNavigationNextFile();

  /**
   * @return The composite of this perspective
   */
  Composite getComposite();

  /**
   * @return The layout data of the composite of this perspective
   */
  FormData getFormData();

  /**
   * Remove this file type handler from the perspective
   *
   * @param typeHandler The file type handler to remove
   * @return
   */
  boolean remove( IHopFileTypeHandler typeHandler );

  /**
   * Get the list of tabs handled by and currently open in the perspective
   * @return The list of tab items
   */
  List<TabItemHandler> getItems();

}
