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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class BaseExplorerFileTypeHandler implements IHopFileTypeHandler {

  protected HopGui hopGui;
  protected ExplorerPerspective perspective;
  protected ExplorerFile explorerFile;

  public BaseExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    this.hopGui = hopGui;
    this.perspective = perspective;
    this.explorerFile = explorerFile;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return Collections.emptyList();
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
  public void updateGui() {}

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
      if (explorerFile.isChanged()) {
        MessageBox messageDialog =
            new MessageBox(hopGui.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText("Save file?");
        messageDialog.setMessage(
            "Do you want to save file '" + explorerFile.getName() + "' before closing?");
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
      new ErrorDialog(hopGui.getShell(), "Error", "Error preparing file close of '"+explorerFile.getName()+"'", e);
      return false;
    }
  }

  @Override
  public boolean hasChanged() {
    return false;
  }

  @Override
  public void undo() {}

  @Override
  public void redo() {}

  @Override
  public Map<String, Object> getStateProperties() {
    return Collections.emptyMap();
  }

  @Override
  public void close() {
    perspective.closeFile(explorerFile);
  }

  @Override
  public void applyStateProperties(Map<String, Object> stateProperties) {}

  @Override
  public IVariables getVariables() {
    return hopGui.getVariables();
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /** @param hopGui The hopGui to set */
  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  /**
   * Gets perspective
   *
   * @return value of perspective
   */
  public ExplorerPerspective getPerspective() {
    return perspective;
  }

  /** @param perspective The perspective to set */
  public void setPerspective(ExplorerPerspective perspective) {
    this.perspective = perspective;
  }

  /**
   * Gets explorerFile
   *
   * @return value of explorerFile
   */
  public ExplorerFile getExplorerFile() {
    return explorerFile;
  }

  /** @param explorerFile The explorerFile to set */
  public void setExplorerFile(ExplorerFile explorerFile) {
    this.explorerFile = explorerFile;
  }
}
