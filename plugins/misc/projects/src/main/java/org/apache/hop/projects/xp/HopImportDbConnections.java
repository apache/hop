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

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.hopgui.HopGui;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

@ExtensionPoint(
        id = "HopImportConnections",
        description = "Import relational database connections into a Hop project",
        extensionPointId = "HopImportConnections"
)
public class HopImportDbConnections implements IExtensionPoint<Object[]> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, Object[] connectionObject) throws HopException {
        String projectName = (String)connectionObject[0];
        List<DatabaseMeta> connectionList = (List<DatabaseMeta>)connectionObject[1];
        TreeMap<String, String> connectionFileMap = (TreeMap<String, String>)connectionObject[2];

        HopGui hopGui = HopGui.getInstance();
        ILogChannel log = hopGui.getLog();

        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        Project project = projectConfig.loadProject( hopGui.getVariables() );
//        project.setMetadataBaseFolder(projectConfig.getProjectHome() + System.getProperty("file.separator") + "metadata");
        ProjectsUtil.enableProject(hopGui.getLog(), projectName, project, variables, Collections.emptyList(), null, hopGui);
        IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
        IHopMetadataSerializer<DatabaseMeta> databaseSerializer = metadataProvider.getSerializer(DatabaseMeta.class);
        projectConfig.getProjectHome();

        Iterator<DatabaseMeta> connectionIterator = connectionList.iterator();
        while(connectionIterator.hasNext()){
            DatabaseMeta databaseMeta = connectionIterator.next();
            try {
                if(databaseSerializer.exists(databaseMeta.getName())){
                    log.logBasic("Skipped: a connection with name '" + databaseMeta.getName() + "' already exists.");
                }else{
                    databaseSerializer.save(databaseMeta);
                }
            } catch (HopException e) {
                e.printStackTrace();
            }
        }

        String eol = System.getProperty("line.separator");

        // only create connections csv if we have connections
        if(connectionList.size() > 0){
            try (Writer writer = new FileWriter(projectConfig.getProjectHome() + System.getProperty("file.separator") + "connections.csv")) {
                for (Map.Entry<String, String> entry : connectionFileMap.entrySet()) {
                    writer.append(entry.getKey())
                            .append(',')
                            .append(entry.getValue())
                            .append(eol);
                }
            } catch (IOException e) {
                log.logError("Error writing connections file to project : ");
                e.printStackTrace();
            }
        }
    }
}
