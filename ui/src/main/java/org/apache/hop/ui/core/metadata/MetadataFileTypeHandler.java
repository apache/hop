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

package org.apache.hop.ui.core.metadata;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataFileTypeHandler implements IHopFileTypeHandler {

 private static final IHopFileType<?> fileType = new MetadataFileType();

  public MetadataFileTypeHandler() {

  }

  @Override public Object getSubject() {
    return null;
  }

  @Override public String getName() {
    return null;
  }

  @Override public void setName( String name ) {
  }

  @Override public IHopFileType<?> getFileType() {
    return fileType;
  }

  @Override public String getFilename() {
    return "meta";
  }

  @Override public void setFilename( String filename ) {
  }

  @Override public void save() throws HopException {
  }

  @Override public void saveAs( String filename ) throws HopException {
  }

  @Override public void start() {
  }

  @Override public void stop() {
  }

  @Override public void pause() {
  }

  @Override public void resume() {
  }

  @Override public void preview() {
  }

  @Override public void debug() {
  }

  @Override public void redraw() {
  }

  @Override public void updateGui() {
  }

  @Override public void selectAll() {
  }

  @Override public void unselectAll() {
  }

  @Override public void copySelectedToClipboard() {
  }

  @Override public void cutSelectedToClipboard() {
  }

  @Override public void deleteSelected() {
  }

  @Override public void pasteFromClipboard() {
  }

  @Override public boolean isCloseable() {
    return true;
  }

  @Override public void close() {

  }

  @Override public boolean hasChanged() {
    return false;
  }

  @Override public void undo() {
  }

  @Override public void redo() {
  }

  @Override public Map<String, Object> getStateProperties() {
    return Collections.emptyMap();
  }

  @Override public void applyStateProperties( Map<String, Object> stateProperties ) {
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

 /**
  * Metadata doesn't have it's own variables.  It should take it elsewhere.
  *
  * @return An empty variables set
  */
 @Override public IVariables getVariables() {
  return new Variables();
 }
}
