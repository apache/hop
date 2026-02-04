/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.selection;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks the last selected item type to help route keyboard shortcuts correctly. This is needed
 * when multiple components (like file explorer and pipeline graph) share the same keyboard
 * shortcuts but need to act on different types of items.
 *
 * <p>Listeners can register to be notified when a given selection type is set (e.g. pipeline graph
 * selection changed).
 */
public class HopGuiSelectionTracker {

  private static HopGuiSelectionTracker instance;

  /** The type of item that was last selected */
  public enum SelectionType {
    /** A file or folder in the file explorer */
    FILE_EXPLORER,
    /** A transform, note, or hop in a pipeline graph */
    PIPELINE_GRAPH,
    /** An action, note, or hop in a workflow graph */
    WORKFLOW_GRAPH,
    /** No specific selection */
    NONE
  }

  private SelectionType lastSelectionType = SelectionType.NONE;

  private final Map<SelectionType, List<Runnable>> selectionListeners =
      new EnumMap<>(SelectionType.class);

  private HopGuiSelectionTracker() {
    for (SelectionType t : SelectionType.values()) {
      selectionListeners.put(t, new ArrayList<>());
    }
  }

  public static HopGuiSelectionTracker getInstance() {
    if (instance == null) {
      instance = new HopGuiSelectionTracker();
    }
    return instance;
  }

  /**
   * Add a listener that runs when the given selection type is set.
   *
   * @param selectionType the type to listen for
   * @param listener runnable to execute when setLastSelectionType(selectionType) is called
   */
  public void addSelectionListener(SelectionType selectionType, Runnable listener) {
    selectionListeners.get(selectionType).add(listener);
  }

  /**
   * Set the last selected item type and notify any listeners for this type.
   *
   * @param selectionType The type of item that was selected
   */
  public void setLastSelectionType(SelectionType selectionType) {
    this.lastSelectionType = selectionType;
    for (Runnable listener : selectionListeners.get(selectionType)) {
      listener.run();
    }
  }

  /**
   * Get the last selected item type
   *
   * @return The type of item that was last selected
   */
  public SelectionType getLastSelectionType() {
    return lastSelectionType;
  }

  /**
   * Check if the last selection matches the given type
   *
   * @param selectionType The type to check against
   * @return true if the last selection matches the given type
   */
  public boolean isLastSelection(SelectionType selectionType) {
    return lastSelectionType == selectionType;
  }
}
