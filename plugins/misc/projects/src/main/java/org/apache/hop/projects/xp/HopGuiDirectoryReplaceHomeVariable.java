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

package org.apache.hop.projects.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectorySelectedExtension;

@ExtensionPoint(
    id = "HopGuiDirectoryReplaceHomeVariable",
    extensionPointId = "HopGuiDirectorySelected",
    description = "Replace ${PROJECT_HOME} in selected directory as a best practice aid")
public class HopGuiDirectoryReplaceHomeVariable
    implements IExtensionPoint<HopGuiDirectorySelectedExtension> {

  // TODO make this optional

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiDirectorySelectedExtension ext) {

    // Is there an active project?
    //
    String projectName = HopNamespace.getNamespace();
    if (StringUtil.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      return;
    }
    String homeFolder;

    if (variables != null) {
      homeFolder = variables.resolve(projectConfig.getProjectHome());
    } else {
      homeFolder = projectConfig.getProjectHome();
    }

    try {
      if (StringUtils.isNotEmpty(homeFolder)) {

        FileObject file = HopVfs.getFileObject(ext.folderName);
        String absoluteFile = file.getName().getPath();

        FileObject home = HopVfs.getFileObject(homeFolder);
        String absoluteHome = home.getName().getPath();
        // Make the URI always end with a /
        if (!absoluteHome.endsWith("/")) {
          absoluteHome += "/";
        }

        // Replace the project home variable in the filename
        //
        if (absoluteFile.startsWith(absoluteHome)) {
          ext.folderName =
              "${"
                  + ProjectsUtil.VARIABLE_PROJECT_HOME
                  + "}/"
                  + absoluteFile.substring(absoluteHome.length());
        }
      }
    } catch (Exception e) {
      log.logError("Error setting default folder for project " + projectName, e);
    }
  }
}
