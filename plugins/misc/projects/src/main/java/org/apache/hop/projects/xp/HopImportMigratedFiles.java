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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.hopgui.HopGui;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Iterator;

@ExtensionPoint(
        id = "HopImportMigratedFiles",
        description = "Imports variables into a Hop project",
        extensionPointId = "HopImportMigratedFiles"
)
public class HopImportMigratedFiles implements IExtensionPoint<Object[]> {

    @Override
    public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, Object[] migrationObject) throws HopException {
        String projectName = (String)migrationObject[0];
        HashMap<String, DOMSource> filesMap = (HashMap<String, DOMSource>)migrationObject[1];
        String inputFolder = (String)migrationObject[2];

        HopGui hopGui = HopGui.getInstance();
        ProjectsConfig config = ProjectsConfigSingleton.getConfig();

        ProjectConfig projectConfig = config.findProjectConfig(projectName);
        Project project = projectConfig.loadProject( hopGui.getVariables() );
        projectConfig.getProjectHome();

        try {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            /**
             * TODO: check if no import path is provided (import into selected project).
             */
            Iterator<String> filesIterator = filesMap.keySet().iterator();
            while(filesIterator.hasNext()) {
                String filename = filesIterator.next();
                DOMSource domSource = filesMap.get(filename);

                // copy any non-Hop files as is
                if(domSource == null){
                    InputStream is = null;
                    OutputStream os = null;
                    try{
                        File sourceFile = new File(filename);
                        if(!sourceFile.isDirectory()){
                            String outFilename = "";
                            if(System.getProperty("os.name").contains("Windows")){
                                outFilename = filename.replaceAll("\\\\", "/")
                                        .replaceAll(inputFolder.replaceAll("\\\\", "/"), projectConfig.getProjectHome().replaceAll("\\\\", "/"));
                            }else{
                                outFilename = filename.replaceAll(inputFolder, projectConfig.getProjectHome());
                            }
                            File projectFile = new File(outFilename);
                            String folderName = projectFile.getParent();
                            Files.createDirectories(Paths.get(folderName));
                            Files.copy(Paths.get(sourceFile.getAbsolutePath()), Paths.get(projectFile.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }catch(IOException e) {
                        iLogChannel.logError("Error copying file '" + filename + " to Hop.");
                        e.printStackTrace();
                    }
                }else{
                    String outFilename = "";
                    if(filename.indexOf(System.getProperty("user.dir")) > -1){
                        outFilename = filename.replaceAll(System.getProperty("user.dir"), "");
                        outFilename = projectConfig.getProjectHome() + outFilename;
                    }else{
                        outFilename = filename;
                    }
                    File outFile = new File(outFilename);
                    String folderName = outFile.getParent();
                    Files.createDirectories(Paths.get(folderName));
                    StreamResult streamResult = new StreamResult(new File(outFilename));
                    transformer.transform(domSource, streamResult);
                }
            }
        }catch(TransformerConfigurationException e) {
            e.printStackTrace();
        }catch(TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
