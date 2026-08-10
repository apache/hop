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

package org.apache.hop.ui.hopgui.shared;

import java.util.List;
import org.apache.hop.core.gui.AreaOwner;

/**
 * Contract for graph editors that participate in the Hop Web SVG canvas stack.
 *
 * <p>Pipeline and workflow graphs implement this interface. Plugins with custom canvas editors can
 * implement it as well, then {@link org.apache.hop.ui.hopgui.CanvasSvgFacade#registerCanvas} and
 * {@link org.apache.hop.ui.hopgui.CanvasSvgFacade#publishSnapshot} to reuse the existing client
 * overlay without hard-coding graph types in the RAP module.
 */
public interface IWebCanvasGraph {

  /**
   * Replace the server-side click-map used for mouse hit testing after an SVG render.
   *
   * @param owners area owners produced by the SVG renderer (may be empty, not null preferred)
   */
  void replaceAreaOwners(List<AreaOwner> owners);

  /**
   * Handle a hover event from the Hop Web client (graph coordinates plus screen coordinates).
   *
   * @param graphX graph X (logical canvas space)
   * @param graphY graph Y (logical canvas space)
   * @param screenX screen X relative to the canvas widget
   * @param screenY screen Y relative to the canvas widget
   */
  void handleWebCanvasHover(int graphX, int graphY, int screenX, int screenY);
}
