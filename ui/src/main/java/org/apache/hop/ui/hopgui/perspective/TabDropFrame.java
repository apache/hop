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

import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Rectangle;

/**
 * The frame {@link TabItemReorder} draws while a tab is dragged over a folder: around the tab the
 * drop would swap with, or around the half of the folder an edge drop would split off. The desktop
 * draws it as a floating shell, Hop Web as a child of the folder; see {@link #create()}.
 */
interface TabDropFrame {

  /**
   * Show the frame around a rectangle of the folder, moving it when it is already up.
   *
   * @param folder the folder the drag is over
   * @param bounds the rectangle to frame, in folder coordinates
   */
  void show(CTabFolder folder, Rectangle bounds);

  /** Take the frame down; a no-op when it is not up. */
  void hide();

  /** Release whatever the frame holds. */
  void dispose();

  /** The frame for the platform Hop runs on. */
  static TabDropFrame create() {
    if (EnvironmentUtils.getInstance().isWeb()) {
      return new WebTabDropFrame();
    }
    return new ShellTabDropFrame();
  }
}
