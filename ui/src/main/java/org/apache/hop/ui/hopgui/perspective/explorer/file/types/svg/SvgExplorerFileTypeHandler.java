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
 *
 */

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.svg;

import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

/** How do we handle an SVG file in file explorer perspective? */
public class SvgExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  public SvgExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  private static void paintControl(PaintEvent event, ExplorerFile explorerFile, Canvas canvas) {
    // Render the SVG file...
    //
    Rectangle area = canvas.getBounds();

    try {
      SvgCacheEntry entry =
          SvgCache.loadSvg(
              new SvgFile(explorerFile.getFilename(), SvgExplorerFileType.class.getClassLoader()));
      SwtUniversalImageSvg svg =
          new SwtUniversalImageSvg(new SvgImage(entry.getSvgDocument()), true);

      float factorX = (float) area.width / entry.getWidth();
      float factorY = (float) area.height / entry.getHeight();
      float minFactor = Math.min(factorX, factorY);

      int imageWidth = (int) (entry.getWidth() * minFactor);
      int imageHeight = (int) (entry.getHeight() * minFactor);

      Image image = svg.getAsBitmapForSize(canvas.getDisplay(), imageWidth, imageHeight);

      event.gc.drawImage(image, 0, 0);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error rendering SVG", e);
    }
  }

  // Render the SVG file...
  //
  @Override
  public void renderFile(Composite composite) {
    final Canvas wCanvas = new Canvas(composite, SWT.NONE);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    wCanvas.setLayoutData(fdCanvas);

    wCanvas.addPaintListener(e -> paintControl(e, explorerFile, wCanvas));
  }
}
