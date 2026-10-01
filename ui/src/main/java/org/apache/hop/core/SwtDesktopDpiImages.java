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

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.ImageDataProvider;

/**
 * Desktop SWT HiDPI image helpers. RAP/RWT does not ship {@link ImageDataProvider} or {@link
 * Image#getImageData(int)}, so this class must never be loaded on Hop Web. Keep all references
 * behind {@code SWT.getPlatform() != "rap"} in {@link SwtUniversalImage}.
 */
final class SwtDesktopDpiImages {

  private SwtDesktopDpiImages() {}

  static Image create(Device device, SwtUniversalImage.ImageDataAtZoom renderer) {
    ImageDataProvider provider = renderer::render;
    return new Image(device, provider);
  }

  static ImageData getImageData(Image image, int zoom) {
    return image.getImageData(zoom);
  }
}
