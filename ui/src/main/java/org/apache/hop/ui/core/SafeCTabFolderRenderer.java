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

package org.apache.hop.ui.core;

import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolderRenderer;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;

/**
 * A safe CTabFolderRenderer that catches IllegalArgumentException when drawing tab images. This
 * prevents crashes on some Linux desktop environments where images may be invalid or disposed
 * during rendering. When an image drawing error occurs, the image is skipped but the text is still
 * rendered.
 *
 * <p>This class is in a separate file to avoid loading CTabFolderRenderer (which doesn't exist in
 * RAP/web mode) when PropsUi is loaded. The class is only instantiated on Linux desktop
 * environments where it's needed.
 */
public class SafeCTabFolderRenderer extends CTabFolderRenderer {
  private final CTabFolder parentFolder;

  public SafeCTabFolderRenderer(CTabFolder parent) {
    super(parent);
    this.parentFolder = parent;
  }

  @Override
  protected void draw(int part, int state, Rectangle bounds, GC gc) {
    if (bounds != null && (bounds.width <= 0 || bounds.height <= 0)) {
      return;
    }
    try {
      super.draw(part, state, bounds, gc);
    } catch (IllegalArgumentException e) {
      // If image drawing fails, temporarily remove images from tab items and redraw
      // This allows text to be rendered even when images are invalid
      if (parentFolder != null && !parentFolder.isDisposed()) {
        CTabItem[] items = parentFolder.getItems();
        Image[] savedImages = new Image[items.length];
        boolean hadImages = false;

        // Save and temporarily clear images
        for (int i = 0; i < items.length; i++) {
          savedImages[i] = items[i].getImage();
          if (savedImages[i] != null) {
            items[i].setImage(null);
            hadImages = true;
          }
        }

        if (hadImages) {
          try {
            // Redraw without images - this should succeed and render the text
            super.draw(part, state, bounds, gc);
          } finally {
            // Restore images (even if they're invalid, we'll catch the exception next time)
            for (int i = 0; i < items.length; i++) {
              if (savedImages[i] != null) {
                items[i].setImage(savedImages[i]);
              }
            }
          }
        }
      }
    }
  }
}
