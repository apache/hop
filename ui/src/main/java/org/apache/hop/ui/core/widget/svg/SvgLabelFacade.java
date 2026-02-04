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
 *
 */

package org.apache.hop.ui.core.widget.svg;

import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ToolItem;

public abstract class SvgLabelFacade {

  private static final SvgLabelFacade IMPL;
  private static Object object = new Object();

  static {
    IMPL = (SvgLabelFacade) ImplementationLoader.newInstance(SvgLabelFacade.class);
  }

  public static synchronized void setData(String id, Label label, String imageFile, int size) {
    synchronized (object) {
      IMPL.setDataInternal(id, label, imageFile, size);
    }
  }

  public abstract void setDataInternal(String id, Label label, String imageFile, int size);

  public static synchronized void enable(
      ToolItem toolItem, String id, Label label, boolean enable) {
    synchronized (object) {
      IMPL.enableInternal(toolItem, id, label, enable);
    }
  }

  public abstract void enableInternal(ToolItem toolItem, String id, Label label, boolean enable);

  public static void shadeSvg(Label label, String id, boolean shaded) {
    synchronized (object) {
      IMPL.shadeSvgInternal(label, id, shaded);
    }
  }

  public abstract void shadeSvgInternal(Label label, String id, boolean shaded);

  /**
   * Update only the image source of an existing img element (e.g. when toggling toolbar icon). In
   * RWT this uses JavaScript to set the img src so the icon updates without replacing the whole
   * label markup. No-op on desktop.
   *
   * @param id the DOM element id of the img (same uniqueId used in setData)
   * @param label the label widget (unused in RWT but required for API)
   * @param imagePath the new image path (e.g. "ui/images/show-selected.svg")
   */
  public static void updateImageSource(String id, Label label, String imagePath) {
    synchronized (object) {
      IMPL.updateImageSourceInternal(id, label, imagePath);
    }
  }

  public abstract void updateImageSourceInternal(String id, Label label, String imagePath);
}
