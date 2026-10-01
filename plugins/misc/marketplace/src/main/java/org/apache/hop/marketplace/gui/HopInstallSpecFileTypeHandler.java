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

package org.apache.hop.marketplace.gui;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.widgets.Composite;

public class HopInstallSpecFileTypeHandler extends BaseExplorerFileTypeHandler {

  private HopInstallSpecEditor editor;

  public HopInstallSpecFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    editor =
        HopInstallSpecEditor.embed(
            composite, explorerFile.getFilename(), message -> hopGui.getLog().logBasic(message));
    editor.setExplorerMode(true);
    editor.setDirtyListener(
        () -> {
          setChanged();
          updateGui();
        });
  }

  @Override
  public void save() throws HopException {
    if (editor == null) {
      return;
    }
    if (!editor.saveCurrent()) {
      return;
    }
    if (editor.getCurrentFilename() != null) {
      setFilename(editor.getCurrentFilename());
    }
    clearChanged();
    updateGui();
  }

  @Override
  public void saveAs(String filename) throws HopException {
    if (editor == null) {
      return;
    }
    editor.setCurrentFilename(filename);
    if (!editor.saveCurrent()) {
      return;
    }
    setFilename(editor.getCurrentFilename());
    hopGui.fileRefreshDelegate.register(getFilename(), this);
    clearChanged();
    updateGui();
    perspective.refresh();
  }

  @Override
  public void reload() {
    if (editor != null) {
      editor.reloadFromDisk();
    }
  }
}
