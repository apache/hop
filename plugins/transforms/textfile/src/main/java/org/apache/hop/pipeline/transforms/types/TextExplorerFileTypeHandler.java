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

package org.apache.hop.pipeline.transforms.types;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
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

/** How do we handle an SVG file in file explorer perspective? */
public class TextExplorerFileTypeHandler extends BaseExplorerFileTypeHandler
    implements IExplorerFileTypeHandler {

  public TextExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    // Render the file by simply showing the TXT content as a text widget...
    //
    Text wText = new Text(composite, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.getInstance().setLook(wText, Props.WIDGET_STYLE_FIXED);
    wText.setEditable(false);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(0, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    // TODO: add bottom section to show status, size, cursor position...
    //

    // Load the content of the JSON file...
    //
    try {
      String contents = readTextFileContent("UTF-8");
      wText.setText(Const.NVL(contents, ""));
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error reading contents of file '" + explorerFile.getFilename() + "'", e);
    }
  }
}
