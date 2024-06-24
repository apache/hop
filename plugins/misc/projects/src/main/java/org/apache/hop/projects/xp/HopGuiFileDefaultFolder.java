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

package org.apache.hop.projects.xp;

import java.util.ArrayList;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.IFileDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;

public class HopGuiFileDefaultFolder implements IExtensionPoint<HopGuiFileDialogExtension> {

  public static final String BOOKMARKS_AUDIT_TYPE = "vfs-bookmarks";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiFileDialogExtension ext) {

    // return if no projectname
    String projectName = HopNamespace.getNamespace();
    if (StringUtil.isEmpty(projectName)) {
      return;
    }

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    // return if no projectConfig is found
    if (projectConfig == null) {
      return;
    }

    // Get last known location from audit history
    PropsUi props = PropsUi.getInstance();
    String usedNamespace;
    java.util.List<String> navigationHistory;
    int navigationIndex;
    String filterPath;

    if (props.useGlobalFileBookmarks()) {
      usedNamespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
    } else {
      usedNamespace = HopNamespace.getNamespace();
    }

    try {
      AuditList auditList =
          AuditManager.getActive().retrieveList(usedNamespace, BOOKMARKS_AUDIT_TYPE);
      navigationHistory = auditList.getNames();
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading navigation history", e);
      navigationHistory = new ArrayList<>();
    }

    if (!navigationHistory.isEmpty()) {
      navigationIndex = navigationHistory.size() - 1;
      filterPath = navigationHistory.get(navigationIndex);
    } else {
      filterPath = projectConfig.getProjectHome();
    }

    // maybe we should clean this up and in the audit split folder and filename
    // check if path ends with slash else remove filename
    int dotFound = filterPath.lastIndexOf(".");
    int slashFound = filterPath.lastIndexOf(Const.FILE_SEPARATOR);
    filterPath =
        dotFound > slashFound && slashFound > 0 ? filterPath.substring(0, slashFound) : filterPath;

    IFileDialog dialog = ext.getFileDialog();
    dialog.setFilterPath(filterPath);
  }
}
