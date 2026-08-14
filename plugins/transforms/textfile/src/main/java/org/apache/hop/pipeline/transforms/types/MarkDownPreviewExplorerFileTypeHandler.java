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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.Const;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.widgets.Composite;

/**
 * Shows the rendered HTML of a Markdown file in its own explorer tab. The content comes from the
 * editor buffer of the Markdown tab it was opened from, so it is a snapshot: previewing again
 * refreshes this tab instead of opening a second one.
 */
public class MarkDownPreviewExplorerFileTypeHandler extends BaseExplorerFileTypeHandler {

  private Browser wBrowser;
  private String html;

  public MarkDownPreviewExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    wBrowser = new Browser(composite, SWT.NONE);
    PropsUi.setLook(wBrowser);
    wBrowser.setLayoutData(FormDataBuilder.builder().fullSize().build());

    reload();
  }

  /**
   * Whether the perspective still has a tab for the given handler. Closing a tab disposes its
   * {@link org.eclipse.swt.custom.CTabItem} but not the control it shows, so widgets outlive the
   * tab and can't answer this: the perspective's own list of tabs is what counts.
   */
  static boolean isTabOpen(ExplorerPerspective perspective, IHopFileTypeHandler handler) {
    for (TabItemHandler item : perspective.getItems()) {
      if (item.getTypeHandler() == handler) {
        return true;
      }
    }
    return false;
  }

  /** True as long as the preview tab is open, false once it has been closed. */
  public boolean isOpen() {
    return isTabOpen(perspective, this);
  }

  /** Disposes the widgets left behind after the tab was closed. */
  public void disposeWidgets() {
    if (hasBrowser()) {
      wBrowser.getParent().dispose();
    }
    wBrowser = null;
  }

  /** Sets the HTML to show. Can be called before the tab is rendered. */
  public void setHtml(String html) {
    this.html = html;
    reload();
  }

  @Override
  public void reload() {
    if (hasBrowser()) {
      wBrowser.setText(Const.NVL(html, ""));
      clearChanged();
    }
  }

  private boolean hasBrowser() {
    return wBrowser != null && !wBrowser.isDisposed();
  }

  @Override
  public void selectAll() {
    // The browser widget has no selection API we can drive from here
  }

  @Override
  public void unselectAll() {
    // The browser widget has no selection API we can drive from here
  }

  @Override
  public void copySelectedToClipboard() {
    // The browser widget has no selection API we can drive from here
  }
}
