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
import java.util.function.Supplier;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.ui.core.widget.svg.SvgImageFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
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
    return cached(width + "x" + height, () -> renderSimple(device, width, height));
  }

  /**
   * Like {@link #getAsBitmapForSize(Device, int, int)} but guaranteed to carry pixels. Use it where
   * the image is read back ({@link Image#getImageData()}, {@code SWT.IMAGE_GRAY}): on Hop Web the
   * plain variant may be a vector the client renders, which has no pixel data on the server.
   */
  public synchronized Image getAsRasterForSize(Device device, int width, int height) {
    return getAsBitmapForSize(device, width, height);
  }

  /** The image under {@code key}, rendered on first use and disposed with this instance. */
  protected synchronized Image cached(String key, Supplier<Image> renderer) {
    checkDisposed();

    Image result = cache.get(key);
    if (result == null) {
      result = renderer.get();
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
   * raster-scales it to the monitor zoom (SMOOTH), which makes icons blurry at 200% DPI. Desktop
   * SWT re-rasterizes via ImageDataProvider instead. RAP has no per-monitor zoom and does not ship
   * that type, so keep the ImageData constructor there and never link the desktop API from this
   * class.
   */
  static boolean isDpiAwareImageProviderSupported() {
    return !"rap".equals(SWT.getPlatform());
  }

  /**
   * Pixel size of a logical extent at an SWT zoom percentage. Must be linear ({@code 200} → {@code
   * 2 * 100}) to satisfy the desktop ImageDataProvider contract.
   */
  public static int pixelSize(int logical, int zoom) {
    return Math.max(1, logical * zoom / 100);
  }

  /**
   * Creates an {@link Image} that can supply native pixels for every SWT zoom. On RAP the 100%
   * variant is used as-is. The renderer type is Hop-owned so RAP class loading does not resolve
   * desktop-only {@code org.eclipse.swt.graphics.ImageDataProvider}.
   */
  public static Image createDpiAwareImage(Device device, ImageDataAtZoom renderer) {
    if (!isDpiAwareImageProviderSupported()) {
      return new Image(device, renderer.render(100));
    }
    return SwtDesktopDpiImages.create(device, renderer);
  }

  /** ImageData at the given zoom, with a RAP-safe fallback that scales the 100% variant. */
  public static ImageData getImageDataAtZoom(Image image, int zoom) {
    if (isDpiAwareImageProviderSupported()) {
      return SwtDesktopDpiImages.getImageData(image, zoom);
    }
    // A vector-backed Hop Web image has no pixels of its own; rasterise it at this zoom instead.
    ImageData vector = SvgImageFacade.rasterize(image, zoom);
    if (vector != null) {
      return vector;
    }
    ImageData data = image.getImageData();
    if (zoom == 100) {
      return data;
    }
    return data.scaledTo(pixelSize(data.width, zoom), pixelSize(data.height, zoom));
  }

  /** Converts BufferedImage to SWT ImageData with alpha channel. */
  public static ImageData toImageData(BufferedImage img) {
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

  /**
   * Supplies {@link ImageData} for an SWT zoom percentage (100, 150, 200, …). Same contract as
   * desktop ImageDataProvider, without depending on that RAP-missing type.
   */
  @FunctionalInterface
  public interface ImageDataAtZoom {
    ImageData render(int zoom);
  }

  @FunctionalInterface
  protected interface ImageDataAtSize {
    ImageData render(int width, int height);
  }
}
