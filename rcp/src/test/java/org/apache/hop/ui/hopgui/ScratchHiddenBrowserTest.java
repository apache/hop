/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.Test;

class ScratchHiddenBrowserTest {

  @Test
  void setTextOnHiddenBrowserTab() {
    Display display = Display.getDefault();
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    shell.setSize(500, 400);
    CTabFolder folder = new CTabFolder(shell, SWT.CLOSE);

    // Tab 1: stands in for the markdown editor
    CTabItem editorItem = new CTabItem(folder, SWT.CLOSE);
    Composite editorContent = new Composite(folder, SWT.NONE);
    editorContent.setLayout(new FillLayout());
    new Text(editorContent, SWT.MULTI);
    editorItem.setControl(editorContent);

    // Tab 2: the preview
    CTabItem previewItem = new CTabItem(folder, SWT.CLOSE);
    Composite previewContent = new Composite(folder, SWT.NONE);
    previewContent.setLayout(new FillLayout());
    Browser browser = new Browser(previewContent, SWT.NONE);
    previewItem.setControl(previewContent);

    folder.setSelection(previewItem);
    shell.open();
    browser.setText("<html><body><p>FIRST</p></body></html>");
    pump(display, 60);
    System.out.println("### visible, first load : " + innerText(browser));

    // Now hide the preview tab, as when the user goes back to the editor tab, and refresh it
    folder.setSelection(editorItem);
    pump(display, 10);
    browser.setText("<html><body><p>SECOND</p></body></html>");
    pump(display, 60);
    System.out.println("### while hidden        : " + innerText(browser));

    // Bring the preview back up: does it show the content set while hidden?
    folder.setSelection(previewItem);
    pump(display, 60);
    System.out.println("### after showing again : " + innerText(browser));

    shell.dispose();
  }

  private static String innerText(Browser browser) {
    try {
      Object result = browser.evaluate("return document.body.innerText;");
      return result == null ? "<null>" : result.toString().trim();
    } catch (Exception e) {
      return "<evaluate failed: " + e.getMessage() + ">";
    }
  }

  private static void pump(Display display, int iterations) {
    for (int i = 0; i < iterations; i++) {
      while (display.readAndDispatch()) {
        // keep going
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
