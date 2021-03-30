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

package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

@ExtensionPoint(
    id = "ExplorerPerspectiveRoot",
    description = "Set the root folder and name of the current project in the explorer perspective",
    extensionPointId = "HopGuiDetermineExplorerRoot")
public class ExplorerPerspectiveRoot
    implements IExtensionPoint<ExplorerPerspective.DetermineRootFolderExtension> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, ExplorerPerspective.DetermineRootFolderExtension ext)
      throws HopException {

    // Get the current project...
    //
    String projectName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig( projectName );
    if (projectConfig==null) {
      return;
    }
    ext.rootFolder = variables.resolve(projectConfig.getProjectHome());
    ext.rootName = projectName;
  }
}
