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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.BinaryDetectionUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;

/**
 * Handler for viewing any file as raw text using the shared content editor
 * (Monaco/RSyntaxTextArea). When content looks like text (no null byte in first 8KB), the file is
 * editable and save/save-as are enabled; when binary, view is read-only and save buttons are
 * disabled.
 */
public class RawExplorerFileTypeHandler extends BaseTextExplorerFileTypeHandler {

  /** True if file was detected as binary (null byte in sample); save is disabled. */
  private boolean binary;

  public RawExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  protected String getLanguageId() {
    return "plaintext";
  }

  @Override
  public void renderFile(Composite composite) {
    binary = detectBinary();

    editorWidget = ContentEditorFacade.createContentEditor(composite, getLanguageId());

    reload();

    if (binary) {
      editorWidget.setReadOnly(true);
      reloadListener = false;
    } else {
      reloadListener = true;
      editorWidget.addModifyListener(
          e -> {
            if (reloadListener) {
              this.setChanged();
              perspective.updateGui();
            }
          });
    }
  }

  private boolean detectBinary() {
    try {
      String path = getFilename();
      FileObject file = HopVfs.getFileObject(path, getVariables());
      if (!file.exists() || !file.isFile()) {
        return false;
      }
      try (InputStream in = HopVfs.getInputStream(file)) {
        return !BinaryDetectionUtil.looksLikeText(in);
      }
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error sampling file for binary detection, treating as text: " + getFilename(), e);
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
        outputStream.write(editorWidget.getText().getBytes(StandardCharsets.UTF_8));
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
    try {
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
  public boolean hasChanged() {
    return !binary && super.hasChanged();
  }

  @Override
  public void reload() {
    try {
      reloadListener = false;
      String contents = readTextFileContent("UTF-8");
      editorWidget.setTextSuppressModify(Const.NVL(contents, ""));
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error reading contents of file '" + explorerFile.getFilename() + "'", e);
    } finally {
      reloadListener = !binary;
    }
  }

  @Override
  public void selectAll() {
    editorWidget.selectAll();
  }

  @Override
  public void unselectAll() {
    editorWidget.unselectAll();
  }

  @Override
  public void copySelectedToClipboard() {
    editorWidget.copy();
  }
}
