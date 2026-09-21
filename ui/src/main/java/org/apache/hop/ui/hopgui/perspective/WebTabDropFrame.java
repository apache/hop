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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Composite;

/**
 * The Hop Web drop frame: a child composite of the folder, drawn by the {@code
 * Composite.hopDropFrame} rule of the theme CSS as a border with a transparent centre. RAP has no
 * shell regions, but it does not need one here: its client resolves the drop target by walking up
 * from the element under the pointer, so a child of the folder hands the drag to the folder instead
 * of stealing it. The composite only exists while a drag is over the folder.
 */
final class WebTabDropFrame implements TabDropFrame {

  /** Custom variant of the frame composite, styled in the Hop Web theme CSS. */
  static final String CUSTOM_VARIANT = "hopDropFrame";

  private static final String RWT_CUSTOM_VARIANT = "org.eclipse.rap.rwt.customVariant";

  private Composite frame;

  @Override
  public void show(CTabFolder folder, Rectangle bounds) {
    if (folder.isDisposed() || bounds.width <= 0 || bounds.height <= 0) {
      hide();
      return;
    }
    if (frame == null || frame.isDisposed() || frame.getParent() != folder) {
      dispose();
      frame = new Composite(folder, SWT.NONE);
      frame.setData(RWT_CUSTOM_VARIANT, CUSTOM_VARIANT);
    }
    frame.setBounds(bounds);
    frame.moveAbove(null);
    frame.setVisible(true);
  }

  @Override
  public void hide() {
    dispose();
  }

  @Override
  public void dispose() {
    if (frame != null && !frame.isDisposed()) {
      frame.dispose();
    }
    frame = null;
  }
}
