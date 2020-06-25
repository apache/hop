/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.search.ISearchable;
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
   * Set the focus on the given file type handler.
   *
   * @param activeFileTypeHandler
   */
  void setActiveFileTypeHandler( IHopFileTypeHandler activeFileTypeHandler );

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

  /**
   * @return A list of searchable items
   */
  List<ISearchable> getSearchables();
}
