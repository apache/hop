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

package org.apache.hop.ui.core.widget.svg;

import java.awt.geom.Dimension2D;
import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.w3c.dom.Document;

/**
 * Widget images that the client draws as vectors.
 *
 * <p>Hop Web has no per-monitor zoom: an {@link Image} is one bitmap at its logical size and the
 * browser scales it up on a high-DPI screen, so a 16px tree icon comes out blurry. The RAP
 * implementation instead publishes the SVG, sized to the requested pixels, as a web resource and
 * returns an image that points at it; the browser then rasterises the vector at device resolution.
 * On desktop, where SWT re-rasterises per zoom itself, the implementation returns {@code null}.
 */
public abstract class SvgImageFacade {

  private static final SvgImageFacade IMPL;

  static {
    IMPL = (SvgImageFacade) ImplementationLoader.newInstance(SvgImageFacade.class);
  }

  /**
   * @param device the display the image belongs to
   * @param document the SVG, already colour-adjusted for the theme
   * @param intrinsicSize the size the drawing was authored for, in user units
   * @param width the requested width in pixels
   * @param height the requested height in pixels
   * @return an image the client renders as a vector, or {@code null} when this platform or thread
   *     cannot provide one and the caller has to rasterise
   */
  public static Image createImage(
      Device device, Document document, Dimension2D intrinsicSize, int width, int height) {
    return IMPL.createImageInternal(device, document, intrinsicSize, width, height);
  }

  protected abstract Image createImageInternal(
      Device device, Document document, Dimension2D intrinsicSize, int width, int height);

  /**
   * Pixels for an image handed out by {@link #createImage}, for the few callers that read an image
   * back (badge compositing, grayscaling).
   *
   * @param image an image, vector-backed or not
   * @param zoom the SWT zoom percentage (100, 200, ...) the pixels are for
   * @return the image rasterised at that zoom, or {@code null} when it is not vector-backed
   */
  public static ImageData rasterize(Image image, int zoom) {
    return IMPL.rasterizeInternal(image, zoom);
  }

  protected abstract ImageData rasterizeInternal(Image image, int zoom);

  /**
   * {@code badge} drawn into the bottom-right corner of {@code base}, as one image the client still
   * renders as a vector. Sizes are in the logical pixels of {@code base}.
   *
   * @return the badged image, or {@code null} when either image is not vector-backed and the caller
   *     has to composite pixels instead
   */
  public static Image overlay(Device device, Image base, Image badge, int badgeSize, int margin) {
    return IMPL.overlayInternal(device, base, badge, badgeSize, margin);
  }

  protected abstract Image overlayInternal(
      Device device, Image base, Image badge, int badgeSize, int margin);
}
