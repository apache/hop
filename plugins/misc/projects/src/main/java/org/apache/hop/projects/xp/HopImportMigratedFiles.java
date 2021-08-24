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

import org.apache.commons.vfs2.FileFilterSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.filter.NameFileFilter;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.hopgui.HopGui;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

@ExtensionPoint(
    id = "HopImportMigratedFiles",
    description = "Imports variables into a Hop project",
    extensionPointId = "HopImportMigratedFiles")
public class HopImportMigratedFiles implements IExtensionPoint<Object[]> {

  @Override
  public void callExtensionPoint(
      ILogChannel iLogChannel, IVariables variables, Object[] migrationObject) throws HopException {
    String projectName = (String) migrationObject[0];
    HashMap<String, DOMSource> filesMap = (HashMap<String, DOMSource>) migrationObject[1];
    FileObject inputFolder = (FileObject) migrationObject[2];
    boolean skipExitingFiles = (boolean) migrationObject[3];

    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    ProjectConfig projectConfig = config.findProjectConfig(projectName);
    String projectHome = HopVfs.getFileObject(projectConfig.getProjectHome()).getName().getURI();

    try {
      ProgressMonitorDialog monitorDialog =
          new ProgressMonitorDialog(HopGui.getInstance().getShell());
      monitorDialog.run(
          true,
          monitor -> {
            try {
              monitor.beginTask("Importing Kettle files...", filesMap.size());
              importFiles(filesMap, inputFolder, projectHome, skipExitingFiles, monitor);
              monitor.done();
            } catch (InterruptedException e) {
              throw e;
            } catch (Exception e) {
              throw new InvocationTargetException(e, "Error importing files");
            }
          });

    } catch (Exception e) {
      throw new HopException("Error migrating file to project '" + projectName + "'", e);
    }
  }

  private void importFiles(
      HashMap<String, DOMSource> filesMap,
      FileObject inputFolder,
      String projectHome,
      boolean skipExitingFiles,
      IProgressMonitor monitor)
      throws TransformerConfigurationException, HopException, FileSystemException,
          InterruptedException {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

    int processed = 0;
    Iterator<String> filesIterator = filesMap.keySet().iterator();
    while (filesIterator.hasNext() && !monitor.isCanceled()) {

      String filename = filesIterator.next();

      FileObject sourceFile = HopVfs.getFileObject(filename);
      if (sourceFile.isFolder()) {
        monitor.worked(++processed);
        continue;
      }

      String targetFilename = filename.replaceAll(inputFolder.getName().getURI(), projectHome);
      monitor.subTask("Importing file to: " + targetFilename);

      FileObject targetFile = HopVfs.getFileObject(targetFilename);
      if (skipExitingFiles && targetFile.exists()) {
        monitor.worked(++processed);
        continue;
      }

      // Make sure the parent folder(s) exist...
      //
      if (!targetFile.getParent().exists()) {
        targetFile.getParent().createFolder();
      }

      DOMSource domSource = filesMap.get(filename);
      if (domSource == null) {
        // copy any non-Hop files as is
        //
        try {
          NameFileFilter filter =
              new NameFileFilter(Collections.singletonList(sourceFile.getName().getBaseName()));
          targetFile.getParent().copyFrom(sourceFile.getParent(), new FileFilterSelector(filter));
          monitor.worked(++processed);
        } catch (IOException e) {
          throw new HopException("Error copying file '" + filename, e);
        }
      } else {
        // Convert Kettle XML metadata to Hop
        //
        StreamResult streamResult = new StreamResult(HopVfs.getOutputStream(targetFilename, false));
        try {
          transformer.transform(domSource, streamResult);
        } catch (TransformerException e) {
          throw new HopException("Error importing file " + filename, e);
        } finally {
          monitor.worked(++processed);
        }
      }
    }
  }
}
