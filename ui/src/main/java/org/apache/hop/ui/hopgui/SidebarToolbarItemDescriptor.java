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

package org.apache.hop.ui.hopgui;

import java.util.Collections;
import java.util.Set;
import lombok.Builder;
import lombok.Value;

/**
 * Descriptor for a button in the bottom-left sidebar toolbar (below the perspective buttons).
 * Visibility is driven by the active perspective so any combination of buttons can be shown or
 * hidden per perspective.
 *
 * <p>Visibility rules (evaluated in order):
 *
 * <ul>
 *   <li>If {@link #visibleForPerspectiveIds} is non-empty: button is visible only when the active
 *       perspective id is in that set (e.g. only in file explorer).
 *   <li>Else: button is visible in all perspectives except those in {@link
 *       #hiddenForPerspectiveIds} (empty = visible everywhere, e.g. terminal).
 * </ul>
 *
 * This allows e.g. "only in explorer", "always", or "everywhere except X".
 */
@Value
@Builder
public class SidebarToolbarItemDescriptor {

  /** Unique id for this item (e.g. for extensions to replace or identify). */
  String id;

  /**
   * When non-empty: button is visible only when the active perspective id is in this set. When
   * empty: visibility is "all perspectives except {@link #hiddenForPerspectiveIds}".
   */
  @Builder.Default Set<String> visibleForPerspectiveIds = Collections.emptySet();

  /**
   * Only used when {@link #visibleForPerspectiveIds} is empty. Perspective ids in this set hide the
   * button; empty = visible in all perspectives.
   */
  @Builder.Default Set<String> hiddenForPerspectiveIds = Collections.emptySet();

  /** Image path (e.g. "ui/images/show-results.svg"). */
  String imagePath;

  /** Icon size in pixels. */
  int imageSize;

  /** Tooltip text. */
  String tooltip;

  /** Called when the button is selected. */
  Runnable onSelect;

  /** Whether this item is available (e.g. terminal only when not in web). */
  @Builder.Default boolean available = true;
}
