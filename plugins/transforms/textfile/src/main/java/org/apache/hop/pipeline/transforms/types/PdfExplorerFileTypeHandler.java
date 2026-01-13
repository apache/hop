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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

/** This handles a PDF file in the file explorer perspective: open, view, ... */
public class PdfExplorerFileTypeHandler extends BaseExplorerFileTypeHandler {

  private Browser wBrowser;

  public PdfExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    // Render the file by showing the PDF content in a browser widget
    // The browser widget can display PDFs using the browser's built-in PDF viewer
    //
    wBrowser = new Browser(composite, SWT.NONE);
    PropsUi.setLook(wBrowser);
    FormData fdBrowser = new FormData();
    fdBrowser.left = new FormAttachment(0, 0);
    fdBrowser.right = new FormAttachment(100, 0);
    fdBrowser.top = new FormAttachment(0, 0);
    fdBrowser.bottom = new FormAttachment(100, 0);
    wBrowser.setLayoutData(fdBrowser);

    reload();
  }

  @Override
  public void reload() {
    try {
      // Get the file URL and load it in the browser
      // The browser widget will use its built-in PDF viewer to display the PDF
      //
      String filename = explorerFile.getFilename();

      // Check if file exists
      if (!HopVfs.fileExists(filename)) {
        showError("File not found: " + filename);
        return;
      }

      // Convert the filename to a file URL
      // For local files, we need to use the file:// protocol
      String fileUrl = HopVfs.getFileObject(filename).getURL().toString();

      // Set the URL in the browser widget
      wBrowser.setUrl(fileUrl);

      // Clear any change flags since we just reloaded
      clearChanged();
    } catch (Exception e) {
      LogChannel.UI.logError("Error loading PDF file '" + explorerFile.getFilename() + "'", e);
      showError("Error loading PDF file: " + Const.NVL(e.getMessage(), "Unknown error"));
    }
  }

  /**
   * Show an error message in the browser widget
   *
   * @param message The error message to display
   */
  private void showError(String message) {
    wBrowser.setText(
        "<html><body><h1>Error loading PDF file</h1><p>" + message + "</p></body></html>");
  }

  @Override
  public void selectAll() {
    // Browser widget doesn't support selectAll for PDF content
    // For now, do nothing
  }

  @Override
  public void unselectAll() {
    // Browser widget doesn't support unselectAll for PDF content
    // For now, do nothing
  }

  @Override
  public void copySelectedToClipboard() {
    // Browser widget doesn't directly support copy to clipboard for PDF content
    // For now, do nothing
  }
}
