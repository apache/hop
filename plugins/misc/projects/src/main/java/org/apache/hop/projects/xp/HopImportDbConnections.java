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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;

@ExtensionPoint(
    id = "HopImportConnections",
    description = "Import relational database connections into a Hop project",
    extensionPointId = "HopImportConnections")
public class HopImportDbConnections implements IExtensionPoint<Object[]> {

  @Override
  public void callExtensionPoint(
      ILogChannel iLogChannel, IVariables variables, Object[] connectionObject)
      throws HopException {
    String projectName = (String) connectionObject[0];
    List<DatabaseMeta> connectionList = (List<DatabaseMeta>) connectionObject[1];
    TreeMap<String, String> connectionFileMap = (TreeMap<String, String>) connectionObject[2];

    ILogChannel log = iLogChannel != null ? iLogChannel : LogChannel.GENERAL;

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    if (projectConfig == null) {
      throw new HopException("Unable to find project '" + projectName + "'");
    }

    // Apply the target project on a throwaway variable space so import does not switch the GUI
    // session (PROJECT_HOME, metadata provider, namespace). See issue #2865.
    IVariables importVars = new Variables();
    importVars.initializeFrom(variables);
    Project project = projectConfig.loadProject(importVars);
    project.modifyVariables(importVars, projectConfig, Collections.emptyList(), null);

    IHopMetadataProvider metadataProvider =
        HopMetadataUtil.getStandardHopMetadataProvider(importVars);
    IHopMetadataSerializer<DatabaseMeta> databaseSerializer =
        metadataProvider.getSerializer(DatabaseMeta.class);

    for (DatabaseMeta databaseMeta : connectionList) {
      try {
        if (databaseSerializer.exists(databaseMeta.getName())) {
          log.logBasic(
              "Skipped: a connection with name '" + databaseMeta.getName() + "' already exists.");
        } else {
          databaseSerializer.save(databaseMeta);
        }
      } catch (HopException e) {
        throw new HopException(
            "Error importing database metadata [" + databaseMeta.getName() + "]", e);
      }
    }

    if (connectionList.isEmpty()) {
      return;
    }

    String eol = System.getProperty("line.separator");
    String projectHome = importVars.resolve(projectConfig.getProjectHome());
    try (FileObject home = HopVfs.getFileObject(projectHome)) {
      FileObject connectionsFile = home.resolveFile("connections.csv");
      try (OutputStream outputStream = HopVfs.getOutputStream(connectionsFile, false)) {
        for (Map.Entry<String, String> entry : connectionFileMap.entrySet()) {
          outputStream.write(entry.getKey().getBytes(StandardCharsets.UTF_8));
          outputStream.write(",".getBytes(StandardCharsets.UTF_8));
          outputStream.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
          outputStream.write(eol.getBytes(StandardCharsets.UTF_8));
        }
      }
    } catch (Exception e) {
      throw new HopException("Error writing connections file to project " + projectName, e);
    }
  }
}
