/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.perspective;

import java.util.List;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/** This interface describes the methods of a Hop GUI perspective. */
public interface IHopPerspective extends IActionContextHandlersProvider {

  String getId();

  /**
   * Get the active file type handler capable of saving, executing, printing, ... a file
   *
   * @return The active file type handler
   */
  default IHopFileTypeHandler getActiveFileTypeHandler() {
    return new EmptyHopFileTypeHandler();
  }

  /**
   * Set the focus on the given file type handler.
   *
   * @param fileTypeHandler the file type handler to activate
   */
  default void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    // Do nothing by default
  }

  /**
   * Get a list of supported file types for this perspective
   *
   * @return The list of supported file types
   */
  default List<IHopFileType> getSupportedHopFileTypes() {
    return List.of();
  }

  /** Switch to this perspective (shown). */
  void activate();

  /** Notify this perspective that it has been activated. */
  void perspectiveActivated();

  /** Navigate the file usage history to the previous file */
  default void navigateToPreviousFile() {
    // Do nothing by default
  }

  /** Navigate the file usage history to the next file */
  default void navigateToNextFile() {
    // Do nothing by default
  }

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
   * @param parent the parent perspectives composite
   */
  void initialize(HopGui hopGui, Composite parent);

  default boolean hasNavigationPreviousFile() {
    return false;
  }

  default boolean hasNavigationNextFile() {
    return false;
  }

  /**
   * @return The control of this perspective
   */
  Control getControl();

  /**
   * Remove this file type handler from the perspective
   *
   * @param typeHandler The file type handler to remove
   */
  default boolean remove(IHopFileTypeHandler typeHandler) {
    throw new IllegalStateException("Perspective does not support removing file type handlers");
  }

  /**
   * Get the list of tabs handled by and currently open in the perspective
   *
   * @return The list of tab items
   */
  default List<TabItemHandler> getItems() {
    return List.of();
  }

  /**
   * @return A list of searchable items
   */
  default List<ISearchable> getSearchables() {
    return List.of();
  }
}
