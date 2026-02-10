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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.raw;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.BinaryDetectionUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

/**
 * Handler for viewing any file as raw text. When content looks like text (no null byte in first
 * 8KB), the file is editable and save/save-as are enabled; when binary, view is read-only and save
 * buttons are disabled.
 */
public class RawExplorerFileTypeHandler extends BaseTextExplorerFileTypeHandler {

  private Text wText;

  /** True if file was detected as binary (null byte in sample); save is disabled. */
  private boolean binary;

  public RawExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    binary = detectBinary();
    int style = SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL;
    if (binary) {
      style |= SWT.READ_ONLY;
    }
    wText = new Text(composite, style);
    PropsUi.setLook(wText, Props.WIDGET_STYLE_FIXED);
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(0, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    if (!binary) {
      wText.addModifyListener(
          e -> {
            if (reloadListener) {
              this.setChanged();
              perspective.updateGui();
            }
          });
    }

    reloadListener = false;
    reload();
    reloadListener = !binary;
  }

  private boolean detectBinary() {
    try {
      FileObject file = HopVfs.getFileObject(getFilename(), getVariables());
      if (!file.exists() || !file.isFile()) {
        return false;
      }
      try (InputStream in = HopVfs.getInputStream(file)) {
        return !BinaryDetectionUtil.looksLikeText(in);
      }
    } catch (Exception e) {
      LogChannel.UI.logBasic(
          getClass().getSimpleName(),
          "Error sampling file for binary detection, treating as text",
          e);
      return false;
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    if (binary
        && (IHopFileType.CAPABILITY_SAVE.equals(capability)
            || IHopFileType.CAPABILITY_SAVE_AS.equals(capability))) {
      return false;
    }
    return super.hasCapability(capability);
  }

  @Override
  public void save() throws HopException {
    if (binary) {
      throw new HopException("Binary file cannot be saved as text.");
    }
    try {
      String filename = explorerFile.getFilename();
      boolean fileExist = HopVfs.fileExists(filename);
      try (java.io.OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
        outputStream.write(wText.getText().getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
      }
      this.clearChanged();
      updateGui();
      if (!fileExist) {
        perspective.refresh();
      }
    } catch (Exception e) {
      throw new HopException("Unable to save file '" + explorerFile.getFilename() + "'", e);
    }
  }

  @Override
  public void saveAs(String filename) throws HopException {
    if (binary) {
      throw new HopException("Binary file cannot be saved as text.");
    }
    filename = HopVfs.normalize(filename);
    setFilename(filename);
    save();
    hopGui.fileRefreshDelegate.register(filename, this);
  }

  @Override
  public boolean hasChanged() {
    return !binary && super.hasChanged();
  }

  @Override
  public void reload() {
    try {
      reloadListener = false;
      String contents = readTextFileContent("UTF-8");
      wText.setText(Const.NVL(contents, ""));
    } catch (Exception e) {
      LogChannel.UI.logBasic(
          "Error reading contents of file '" + explorerFile.getFilename() + "'", e);
    } finally {
      reloadListener = !binary;
    }
  }

  @Override
  public void selectAll() {
    wText.selectAll();
  }

  @Override
  public void unselectAll() {
    wText.setSelection(0, 0);
  }

  @Override
  public void copySelectedToClipboard() {
    wText.copy();
  }
}
