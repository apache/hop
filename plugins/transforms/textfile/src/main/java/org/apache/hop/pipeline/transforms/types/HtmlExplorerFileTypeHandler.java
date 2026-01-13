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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

/** This handles an HTML file in the file explorer perspective: open, save, ... */
public class HtmlExplorerFileTypeHandler extends BaseExplorerFileTypeHandler {

  private Browser wBrowser;
  private String originalHtmlContent;

  public HtmlExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    // Render the file by showing the HTML content in a browser widget
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
  public void save() throws HopException {
    try {
      // Save the current HTML content
      //
      String filename = explorerFile.getFilename();

      boolean fileExist = HopVfs.fileExists(filename);

      // Save the HTML content
      // Note: We save the original content since Browser widget doesn't easily expose
      // edited content. For full editing support, a source view would be needed.
      //
      if (originalHtmlContent != null) {
        try (OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
          outputStream.write(originalHtmlContent.getBytes(StandardCharsets.UTF_8));
          outputStream.flush();
        }
      } else {
        throw new HopException("No HTML content to save");
      }

      this.clearChanged();

      // Update menu options, tab and tree item
      updateGui();

      // If we create a new file, refresh the explorer perspective tree
      if (!fileExist) {
        perspective.refresh();
      }
    } catch (Exception e) {
      throw new HopException("Unable to save HTML file '" + explorerFile.getFilename() + "'", e);
    }
  }

  @Override
  public void saveAs(String filename) throws HopException {
    try {
      // Enforce file extension
      if (!filename.toLowerCase().endsWith(".html") && !filename.toLowerCase().endsWith(".htm")) {
        filename = filename + ".html";
      }

      // Normalize file name
      filename = HopVfs.normalize(filename);

      FileObject fileObject = HopVfs.getFileObject(filename);
      if (fileObject.exists()) {
        MessageBox box =
            new MessageBox(hopGui.getActiveShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Overwrite?");
        box.setMessage("Are you sure you want to overwrite file '" + filename + "'?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return;
        }
      }

      setFilename(filename);

      save();
      hopGui.fileRefreshDelegate.register(filename, this);
    } catch (Exception e) {
      throw new HopException("Error validating file existence for '" + filename + "'", e);
    }
  }

  @Override
  public void reload() {
    try {
      String filename = explorerFile.getFilename();
      if (filename.toLowerCase().startsWith("http://")
          || filename.toLowerCase().startsWith("https://")) {
        wBrowser.setUrl(filename);
        clearChanged();
        return;
      }

      // Read HTML content from file
      String htmlContent = readTextFileContent("UTF-8");
      originalHtmlContent = Const.NVL(htmlContent, "");

      // Display HTML in browser widget
      wBrowser.setText(originalHtmlContent);

      // Clear any change flags since we just reloaded
      clearChanged();
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error reading contents of HTML file '" + explorerFile.getFilename() + "'", e);
      // Show error in browser
      wBrowser.setText(
          "<html><body><h1>Error loading HTML file</h1><p>"
              + Const.NVL(e.getMessage(), "Unknown error")
              + "</p></body></html>");
    }
  }

  @Override
  public void selectAll() {
    // Browser widget doesn't support selectAll in the same way as Text widget
    // Could use JavaScript: wBrowser.execute("document.execCommand('selectAll',
    // false, null);");
    // For now, do nothing
  }

  @Override
  public void unselectAll() {
    // Browser widget doesn't support unselectAll
    // For now, do nothing
  }

  @Override
  public void copySelectedToClipboard() {
    // Browser widget doesn't directly support copy to clipboard
    // Could use JavaScript: wBrowser.execute("document.execCommand('copy', false,
    // null);");
    // For now, do nothing
  }
}
