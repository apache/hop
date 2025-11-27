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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.eclipse.swt.SWT;

public abstract class BaseExplorerFileTypeHandler implements IExplorerFileTypeHandler {

  @Getter protected final HopGui hopGui;
  @Getter protected final ExplorerPerspective perspective;
  @Getter @Setter protected ExplorerFile explorerFile;
  private boolean changed;
  protected Set<IContentChangedListener> contentChangedListeners = new CopyOnWriteArraySet<>();

  public BaseExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    this.hopGui = hopGui;
    this.perspective = perspective;
    this.explorerFile = explorerFile;
  }

  protected String readTextFileContent(String encoding) throws HopException {
    try {
      FileObject file = HopVfs.getFileObject(getFilename());
      if (file.exists()) {
        try (InputStream inputStream = HopVfs.getInputStream(file)) {
          StringWriter writer = new StringWriter();
          IOUtils.copy(inputStream, writer, encoding);
          return writer.toString();
        }
      } else {
        throw new HopException("File '" + getFilename() + "' doesn't exist");
      }
    } catch (IOException e) {
      throw new HopException(
          "I/O exception while reading contents of file '" + getFilename() + "'", e);
    }
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  @Override
  public Object getSubject() {
    return explorerFile;
  }

  @Override
  public String getName() {
    return explorerFile.getName();
  }

  @Override
  public void setName(String name) {
    explorerFile.setName(name);
  }

  @Override
  public IHopFileType getFileType() {
    return explorerFile.getFileType();
  }

  @Override
  public String getFilename() {
    return explorerFile.getFilename();
  }

  @Override
  public void setFilename(String filename) {
    explorerFile.setFilename(filename);
    // Update name based on filename
    try {
      String name = HopVfs.getFileObject(filename).getName().getBaseName();
      explorerFile.setName(name);
    } catch (HopFileException e) {
      // Ignore
    }
  }

  @Override
  public void save() throws HopException {
    throw new HopException("Saving file '" + getFilename() + " is not implemented.");
  }

  @Override
  public void saveAs(String filename) throws HopException {
    throw new HopException("Saving file '" + getFilename() + " is not implemented.");
  }

  @Override
  public void start() {
    // Nothing to start
  }

  @Override
  public void stop() {
    // Nothing to stop
  }

  @Override
  public void pause() {
    // Nothing to pause
  }

  @Override
  public void resume() {
    // Nothing to resume
  }

  @Override
  public void preview() {
    // Nothing to preview
  }

  @Override
  public void debug() {
    // Nothing to debug
  }

  @Override
  public void redraw() {}

  @Override
  public void updateGui() {
    hopGui
        .getDisplay()
        .asyncExec(
            () ->
                hopGui.handleFileCapabilities(this.getFileType(), this.hasChanged(), false, false));
  }

  @Override
  public void selectAll() {}

  @Override
  public void unselectAll() {}

  @Override
  public void copySelectedToClipboard() {}

  @Override
  public void cutSelectedToClipboard() {}

  @Override
  public void deleteSelected() {}

  @Override
  public void pasteFromClipboard() {}

  @Override
  public boolean isCloseable() {
    try {
      if (changed) {
        MessageBox messageDialog =
            new MessageBox(hopGui.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText("Save file?");
        messageDialog.setMessage("Do you want to save file '" + getName() + "' before closing?");
        int answer = messageDialog.open();
        if ((answer & SWT.YES) != 0) {
          save();
          return true;
        }
        if ((answer & SWT.NO) != 0) {
          // User doesn't want to save but close
          return true;
        }
        return false;
      }
      return true;
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error preparing file close of '" + getName() + "'", e);
      return false;
    }
  }

  @Override
  public boolean hasChanged() {
    return changed;
  }

  //  /** Flag the file as changed */
  public void setChanged() {
    if (!changed) {
      this.changed = true;
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentChanged(this);
      }
    }
  }

  public void clearChanged() {
    if (changed) {
      this.changed = false;
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentSafe(this);
      }
    }
  }

  @Override
  public void undo() {}

  @Override
  public void redo() {}

  @Override
  public Map<String, Object> getStateProperties() {
    // Return an empty editable map because HopGuiAuditDelegate tries to add properties
    return new HashMap<>();
  }

  @Override
  public void close() {
    perspective.remove(this);
  }

  @Override
  public void applyStateProperties(Map<String, Object> stateProperties) {}

  @Override
  public IVariables getVariables() {
    return hopGui.getVariables();
  }

  @Override
  public void addContentChangedListener(IContentChangedListener listener) {
    if (listener != null) {
      contentChangedListeners.add(listener);
    }
  }

  @Override
  public void removeContentChangedListener(IContentChangedListener listener) {
    if (listener != null) {
      contentChangedListeners.remove(listener);
    }
  }

  protected void fireContentChangedListeners(boolean ch) {
    if (ch) {
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentChanged(this);
      }
    } else {
      for (IContentChangedListener listener : contentChangedListeners) {
        listener.contentSafe(this);
      }
    }
  }
}
