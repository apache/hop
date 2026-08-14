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

package org.apache.hop.core;

import java.awt.image.BufferedImage;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hop.core.exception.HopRuntimeException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.ImageDataProvider;
import org.eclipse.swt.graphics.PaletteData;
import org.eclipse.swt.graphics.RGB;

/**
 * Universal image storage for SWT processing. It contains SVG or bitmap image depends on file and
 * settings.
 */
public abstract class SwtUniversalImage {

  private Map<String, Image> cache = new TreeMap<>();

  /**
   * @deprecated
   * @param device
   * @return
   */
  @Deprecated(since = "2.0")
  protected abstract Image renderSimple(Device device);

  protected abstract Image renderSimple(Device device, int width, int height);

  protected abstract Image renderRotated(Device device, int width, int height, double angleRadians);

  public synchronized void dispose() {
    if (cache == null) {
      return;
    }

    for (Image img : cache.values()) {
      if (!img.isDisposed()) {
        img.dispose();
      }
    }
    cache = null;
  }

  private void checkDisposed() {
    if (cache == null) {
      throw new HopRuntimeException("Already disposed");
    }
  }

  /**
   * @deprecated Use getAsBitmapForSize() instead.
   */
  @Deprecated(since = "2.0")
  public synchronized Image getAsBitmap(Device device) {
    checkDisposed();

    Image result = cache.get("");

    if (result == null) {
      result = renderSimple(device);
      cache.put("", result);
    }
    return result;
  }

  /** Method getAsBitmapForSize(..., angle) can't be called, because it returns bigger picture. */
  public synchronized Image getAsBitmapForSize(Device device, int width, int height) {
    checkDisposed();

    String key = width + "x" + height;
    Image result = cache.get(key);
    if (result == null) {
      result = renderSimple(device, width, height);
      cache.put(key, result);
    }
    return result;
  }

  /** Draw rotated image on double canvas size. It required against lost corners on rotate. */
  public synchronized Image getAsBitmapForSize(
      Device device, int width, int height, double angleRadians) {
    checkDisposed();

    int angleDegree = (int) Math.round(Math.toDegrees(angleRadians));
    while (angleDegree < 0) {
      angleDegree += 360;
    }
    angleDegree %= 360;
    angleRadians = Math.toRadians(angleDegree);

    String key = width + "x" + height + "/" + angleDegree;
    Image result = cache.get(key);
    if (result == null) {
      result = renderRotated(device, width, height, angleRadians);
      cache.put(key, result);
    }

    return result;
  }

  /**
   * SWT 3.134+ on Windows treats {@code new Image(device, ImageData)} as 100% zoom and
   * raster-scales it to the monitor zoom (SMOOTH), which makes icons blurry at 200% DPI. {@link
   * ImageDataProvider} re-rasterize at the requested zoom instead. RAP has no per-monitor zoom, so
   * keep the ImageData constructor there.
   */
  static boolean isDpiAwareImageProviderSupported() {
    return !"rap".equals(SWT.getPlatform());
  }

  /**
   * Pixel size of a logical extent at an SWT zoom percentage. Must be linear ({@code 200} → {@code
   * 2 * 100}) to satisfy the {@link ImageDataProvider} contract.
   */
  static int pixelSize(int logical, int zoom) {
    return Math.max(1, logical * zoom / 100);
  }

  /**
   * Creates an {@link Image} that can supply native pixels for every SWT zoom. On RAP the 100%
   * variant is used as-is.
   */
  public static Image createDpiAwareImage(Device device, ImageDataProvider provider) {
    if (!isDpiAwareImageProviderSupported()) {
      return new Image(device, provider.getImageData(100));
    }
    return new Image(device, provider);
  }

  /** ImageData at the given zoom, with a RAP-safe fallback that scales the 100% variant. */
  public static ImageData getImageDataAtZoom(Image image, int zoom) {
    if (isDpiAwareImageProviderSupported()) {
      return image.getImageData(zoom);
    }
    ImageData data = image.getImageData();
    if (zoom == 100) {
      return data;
    }
    return data.scaledTo(pixelSize(data.width, zoom), pixelSize(data.height, zoom));
  }

  /** Converts BufferedImage to SWT ImageData with alpha channel. */
  static ImageData toImageData(BufferedImage img) {
    PaletteData palette = new PaletteData(0xFF0000, 0xFF00, 0xFF);
    ImageData data = new ImageData(img.getWidth(), img.getHeight(), 32, palette);
    for (int y = 0; y < data.height; y++) {
      for (int x = 0; x < data.width; x++) {
        int rgba = img.getRGB(x, y);
        int rgb = palette.getPixel(new RGB((rgba >> 16) & 0xFF, (rgba >> 8) & 0xFF, rgba & 0xFF));
        int a = (rgba >> 24) & 0xFF;
        data.setPixel(x, y, rgb);
        data.setAlpha(x, y, a);
      }
    }
    return data;
  }

  /**
   * Creates a zoom-aware SWT image from a renderer that produces pixels at a concrete width/height.
   */
  protected Image createDpiAwareImage(
      Device device, int width, int height, ImageDataAtSize renderer) {
    return createDpiAwareImage(
        device, zoom -> renderer.render(pixelSize(width, zoom), pixelSize(height, zoom)));
  }

  /** Converts BufferedImage to SWT/Image with alpha channel. */
  protected Image swing2swt(Device device, BufferedImage img) {
    return new Image(device, toImageData(img));
  }

  @FunctionalInterface
  protected interface ImageDataAtSize {
    ImageData render(int width, int height);
  }
}
