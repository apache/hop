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

package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.core.search.ISearchable;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmptyHopPerspective implements IHopPerspective {

  private IHopFileTypeHandler emptyHandler;

  public EmptyHopPerspective() {
    emptyHandler = new EmptyHopFileTypeHandler();
  }

  @Override public String getId() {
    return "empty";
  }

  @Override public IHopFileTypeHandler getActiveFileTypeHandler() {
    return emptyHandler;
  }

  @Override public void setActiveFileTypeHandler( IHopFileTypeHandler activeFileTypeHandler ) {
  }

  @Override public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  @Override public void activate() {
  }
  
  @Override public void perspectiveActivated() {
  }
  
  @Override public void navigateToPreviousFile() {
  }

  @Override public void navigateToNextFile() {
  }

  @Override public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override public boolean hasNavigationNextFile() {
    return false;
  }

  @Override public boolean isActive() {
    return false;
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
  }

  @Override public Control getControl() {
    return null;
  }

  @Override public boolean remove( IHopFileTypeHandler typeHandler ) {
    return true;
  }

  @Override public List<TabItemHandler> getItems() {
    return null;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

  @Override public List<ISearchable> getSearchables() {
    List<ISearchable> searchables = new ArrayList<>();
    return searchables;
  } 
}
