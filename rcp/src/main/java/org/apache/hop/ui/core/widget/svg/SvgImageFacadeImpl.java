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
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.w3c.dom.Document;

/** Desktop SWT rasterises per monitor zoom itself, so there is nothing to do here. */
public class SvgImageFacadeImpl extends SvgImageFacade {

  @Override
  protected Image createImageInternal(
      Device device, Document document, Dimension2D intrinsicSize, int width, int height) {
    return null;
  }

  @Override
  protected ImageData rasterizeInternal(Image image, int zoom) {
    return null;
  }

  @Override
  protected Image overlayInternal(
      Device device, Image base, Image badge, int badgeSize, int margin) {
    return null;
  }
}
