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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;

import java.io.File;

@ExtensionPoint( id = "HopGuiFileReplaceHomeVariable",
extensionPointId = "HopGuiFileOpenedDialog",
description = "Replace ${PROJECT_HOME} in selected filenames as a best practice aid"
)
public class HopGuiFileReplaceHomeVariable implements IExtensionPoint<HopGuiFileOpenedExtension> {

  // TODO make this optional

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, HopGuiFileOpenedExtension ext ) {

    // Is there an active project?
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
    String homeFolder = projectConfig.getProjectHome();
    try {
      if ( StringUtils.isNotEmpty(homeFolder)) {

        File file = new File(ext.filename);
        String absoluteFile = file.getAbsolutePath();

        File home = new File(homeFolder);
        String absoluteHome = home.getAbsolutePath();
        // Make it always end with a / or \
        if (!absoluteHome.endsWith( Const.FILE_SEPARATOR )) {
          absoluteHome+=Const.FILE_SEPARATOR;
        }

        // Replace the project home variable in the filename
        //
        if (absoluteFile.startsWith( absoluteHome )) {
          ext.filename = "${"+ ProjectsUtil.VARIABLE_PROJECT_HOME +"}/"+absoluteFile.substring( absoluteHome.length() );
        }
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for project "+projectName, e );
    }
  }
}
