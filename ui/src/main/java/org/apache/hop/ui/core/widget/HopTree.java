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

package org.apache.hop.ui.core.widget;

import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;

/**
 * A drop-in replacement for SWT {@link Tree} that caps its <em>preferred</em> height.
 *
 * <p>A plain {@link Tree} reports a {@link #computeSize(int, int, boolean) computeSize} height that
 * grows with the number of (visible) items. When such a tree is placed in a dialog that sizes
 * itself to its content (i.e. {@code shell.pack()} on first open, before any geometry is
 * remembered), a long list makes the whole shell grow taller than the screen. This mirrors the
 * problem that {@link TableView} already solves for tables (issue #7220).
 *
 * <p>This widget only ever <em>shrinks</em> an oversized preferred height down to {@link
 * TableView#HEIGHT_HINT_MAX_PX} (scaled by the native zoom factor); it never enlarges a small tree.
 * Because the cap applies to {@code computeSize} only, the tree still stretches to fill whatever
 * space its layout gives it (e.g. a {@code FormAttachment} to the bottom of the shell, or a manual
 * resize) — exactly like {@link TableView}.
 */
public class HopTree extends Tree {

  public HopTree(Composite parent, int style) {
    super(parent, style);
  }

  @Override
  public Point computeSize(int wHint, int hHint, boolean changed) {
    Point size = super.computeSize(wHint, hHint, changed);
    if (hHint == SWT.DEFAULT) {
      int maxHeight =
          (int) Math.round(TableView.HEIGHT_HINT_MAX_PX * PropsUi.getNativeZoomFactor());
      if (size.y > maxHeight) {
        size.y = maxHeight;
      }
    }
    return size;
  }

  @Override
  protected void checkSubclass() {
    // Allow subclassing of the SWT Tree widget: this is a thin, behaviour-only wrapper that adds
    // no native resources and overrides nothing but computeSize.
  }
}
