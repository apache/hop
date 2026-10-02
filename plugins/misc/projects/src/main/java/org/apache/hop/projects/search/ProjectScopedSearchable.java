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

package org.apache.hop.projects.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.gui.ProjectsGuiPlugin;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

/**
 * Searchable that belongs to one project. Opening a hit from another project asks to switch there
 * first and does not open it in the project that is active now.
 */
final class ProjectScopedSearchable implements ISearchable<Object> {

  private static final Class<?> PKG = ProjectScopedSearchable.class;

  private final ISearchable<?> delegate;
  private final String projectName;
  private final ProjectSearchableOpen.Actions actions;

  private ProjectScopedSearchable(
      ISearchable<?> delegate, String projectName, ProjectSearchableOpen.Actions actions) {
    this.delegate = delegate;
    this.projectName = projectName;
    this.actions = actions;
  }

  static ISearchable<?> wrap(ISearchable<?> searchable, String projectName) {
    if (searchable == null || StringUtils.isEmpty(projectName)) {
      return searchable;
    }
    return forProject(searchable, projectName, HOP_GUI);
  }

  static ISearchable<?> forProject(
      ISearchable<?> searchable, String projectName, ProjectSearchableOpen.Actions actions) {
    return new ProjectScopedSearchable(searchable, projectName, actions);
  }

  @Override
  public String getLocation() {
    return delegate.getLocation();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getType() {
    return delegate.getType();
  }

  @Override
  public String getFilename() {
    return delegate.getFilename();
  }

  @Override
  public Object getSearchableObject() {
    return delegate.getSearchableObject();
  }

  @Override
  public ISearchableCallback getSearchCallback() {
    return (searchable, searchResult) ->
        ProjectSearchableOpen.prepare(
            projectName,
            actions.activeProjectName(),
            () -> actions.confirmSwitch(projectName),
            actions::switchToProject,
            () -> {
              ISearchableCallback callback = delegate.getSearchCallback();
              callback.callback(delegate, searchResult);
            });
  }

  /** Question box and project switch used by the GUI. */
  static final ProjectSearchableOpen.Actions HOP_GUI =
      new ProjectSearchableOpen.Actions() {
        @Override
        public String activeProjectName() {
          try {
            return HopNamespace.getNamespace();
          } catch (RuntimeException e) {
            return null;
          }
        }

        @Override
        public boolean confirmSwitch(String projectName) {
          HopGui hopGui = HopGui.getInstance();
          MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
          box.setText(BaseMessages.getString(PKG, "ProjectSearch.SwitchProject.Title"));
          box.setMessage(
              BaseMessages.getString(PKG, "ProjectSearch.SwitchProject.Message", projectName));
          return (box.open() & SWT.YES) != 0;
        }

        @Override
        public boolean switchToProject(String projectName) {
          try {
            new ProjectsGuiPlugin().selectProject(projectName);
          } catch (Exception e) {
            LogChannel.GENERAL.logError("Error switching to project '" + projectName + "'", e);
            return false;
          }
          String active = activeProjectName();
          if (active == null || !active.equalsIgnoreCase(projectName)) {
            return false;
          }
          // selectProjectInUiOnly sets the namespace when the project could not be loaded. That is
          // not an enabled project: its variables still belong to the project that was active.
          HopGui hopGui = HopGui.getInstance();
          if (hopGui == null || hopGui.getVariables() == null) {
            return false;
          }
          String enabled = hopGui.getVariables().getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME);
          return projectName.equalsIgnoreCase(StringUtils.defaultString(enabled));
        }
      };
}
