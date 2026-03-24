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
 * prevents crashes on some Linux desktop environments (e.g., KDE Plasma/Wayland) where images may
 * be invalid or disposed during rendering. When an image drawing error occurs, the image is skipped
 * but the text is still rendered.
 *
 * <p>This class is in the RCP module (desktop-specific) and loaded via reflection from
 * PropsUi.ensureSafeRenderer(). Since that method short-circuits in web mode (isWeb() check), and
 * this class is not included in web builds (hop-ui-rcp is excluded), it will never be loaded in
 * RAP/web mode.
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

    // Check for invalid/disposed images before drawing to prevent IllegalArgumentException
    // on Linux environments (e.g., KDE Plasma/Wayland) where images may be invalid or disposed
    Image[] savedImages = null;
    boolean hadInvalidImages = false;

    if (parentFolder != null && !parentFolder.isDisposed()) {
      CTabItem[] items = parentFolder.getItems();
      savedImages = new Image[items.length];

      // Check each tab item's image for validity before drawing
      for (int i = 0; i < items.length; i++) {
        Image image = items[i].getImage();
        savedImages[i] = image;

        // If image is disposed/invalid, temporarily remove it to prevent rendering errors
        if (image != null && image.isDisposed()) {
          items[i].setImage(null);
          hadInvalidImages = true;
        }
      }
    }

    try {
      super.draw(part, state, bounds, gc);
    } catch (IllegalArgumentException e) {
      // If image drawing still fails (e.g., image becomes invalid during drawing),
      // temporarily remove all images and redraw to allow text to be rendered
      if (parentFolder != null && !parentFolder.isDisposed()) {
        CTabItem[] items = parentFolder.getItems();

        // Clear all images that weren't already cleared
        for (int i = 0; i < items.length && savedImages != null && i < savedImages.length; i++) {
          if (savedImages[i] != null && items[i].getImage() != null) {
            items[i].setImage(null);
          }
        }

        try {
          // Redraw without images - this should succeed and render the text
          super.draw(part, state, bounds, gc);
        } finally {
          // Restore images (even if they're invalid, we'll catch it next time)
          restoreImages(items, savedImages);
        }
      }
    } finally {
      // Restore images that were temporarily removed due to being disposed
      if (hadInvalidImages && parentFolder != null && !parentFolder.isDisposed()) {
        restoreImages(parentFolder.getItems(), savedImages);
      }
    }
  }

  /** Restores images to tab items, skipping any that are disposed */
  private void restoreImages(CTabItem[] items, Image[] savedImages) {
    if (items == null || savedImages == null) {
      return;
    }

    for (int i = 0; i < items.length && i < savedImages.length; i++) {
      if (savedImages[i] != null && !savedImages[i].isDisposed()) {
        items[i].setImage(savedImages[i]);
      }
    }
  }
}
