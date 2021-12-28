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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.text;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** This handles a text file in the file explorer perspective: open, save, ... */
public class BaseTextExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  private Text wText;

  public BaseTextExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    // Render the file by simply showing the file content as a text widget...
    //
    wText = new Text(composite, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.getInstance().setLook(wText, Props.WIDGET_STYLE_FIXED);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(0, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    // TODO: add bottom section to show status, size, changed dates, cursor position...
    // TODO: options for validation, pretty print, ...
    // TODO: options for reading the file with a various transform plugins
    // TODO: option to discard changes (reload from disk)
    // TODO: find in file feature, hook it up to the project find function
    //

    try {
      String contents = readTextFileContent("UTF-8");
      wText.setText(Const.NVL(contents, ""));
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error reading contents of file '" + explorerFile.getFilename() + "'", e);
    }

    // If the widget changes after this it's been changed by the user
    //
    wText.addModifyListener(
        e -> {
          explorerFile.setChanged();
          perspective.updateGui();
        });
  }

  @Override
  public void save() throws HopException {

    try {
      // Save the current explorer file ....
      //
      String filename = explorerFile.getFilename();

      // Save the file...
      //
      try (OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
        outputStream.write(wText.getText().getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      }

      explorerFile.clearChanged();
      perspective.refresh(); // refresh the explorer perspective tree
      perspective.updateGui(); // Update menu options
    } catch (Exception e) {
      throw new HopException("Unable to save file '" + explorerFile.getFilename() + "'", e);
    }
  }
}
