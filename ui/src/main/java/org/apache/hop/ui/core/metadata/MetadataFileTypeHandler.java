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

package org.apache.hop.ui.core.metadata;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataFileTypeHandler<T extends IHopMetadata> implements IHopFileTypeHandler {

  private static final IHopFileType fileType = new MetadataFileType();

  private T metadata;

  public MetadataFileTypeHandler(T metadata) {
    this.metadata = metadata;
  }

  @Override
  public Object getSubject() {
    return null;
  }

  @Override
  public String getName() {
    if (metadata == null) {
      return null;
    }
    return metadata.getName();
  }

  @Override
  public void setName(String name) {
    if (metadata == null) {
      return;
    }
    metadata.setName(name);
  }

  @Override
  public IHopFileType getFileType() {
    return fileType;
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public void setFilename(String filename) {}

  @Override
  public void save() throws HopException {}

  @Override
  public void saveAs(String filename) throws HopException {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void pause() {}

  @Override
  public void resume() {}

  @Override
  public void preview() {}

  @Override
  public void debug() {}

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

  private TabItemHandler findTabItemHandler() {
    if (metadata == null) {
      return null;
    }
    MetadataPerspective perspective = MetadataPerspective.getInstance();
    for (TabItemHandler tabItemHandler : perspective.getItems()) {
      CTabItem tabItem = tabItemHandler.getTabItem();
      MetadataEditor editor = (MetadataEditor) tabItem.getData();
      IHopMetadata other = editor.getMetadata();
      if (other.equals(metadata)) {
        return tabItemHandler;
      }
    }
    return null;
  }

  @Override
  public boolean isCloseable() {
    TabItemHandler tabItemHandler = findTabItemHandler();
    if (tabItemHandler == null) {
      return true;
    }
    IHopFileTypeHandler typeHandler = tabItemHandler.getTypeHandler();
    return typeHandler.isCloseable();
  }

  @Override
  public void close() {
    TabItemHandler tabItemHandler = findTabItemHandler();
    if (tabItemHandler == null) {
      return;
    }
    // Simply close the associated tab
    //
    CTabItem tabItem = tabItemHandler.getTabItem();
    tabItem.dispose();
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
  public void applyStateProperties(Map<String, Object> stateProperties) {}

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

  /**
   * Metadata doesn't have its own variables. It should take it elsewhere.
   *
   * @return An empty variables set
   */
  @Override
  public IVariables getVariables() {
    return new Variables();
  }
}
