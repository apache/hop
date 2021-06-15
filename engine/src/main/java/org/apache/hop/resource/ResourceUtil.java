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

package org.apache.hop.resource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ResourceUtil {

  private static final Class<?> PKG = ResourceUtil.class; // For Translator

  /**
   * Serializes the referenced resource export interface (Workflow, Pipeline, Mapping, Transform,
   * Action, etc) to a ZIP file.
   *
   * @param zipFilename The ZIP file to put the content in
   * @param resourceExportInterface the interface to serialize
   * @param variables the variables to use for variable replacement
   * @param metadataProvider The metadata for which we want to include the metadata.json file
   * @param executionConfiguration The XML interface to inject into the resulting ZIP archive
   *     (optional, can be null)
   * @param injectFilename The name of the file for the XML to inject in the ZIP archive (optional,
   *     can be null)
   * @param sourceResourceFolderMapping The source folder to use as a reference for named resources,
   *     typically something like ${PROJECT}
   * @param targetResourceFolderMapping the target folder of named resources to translate to.
   * @param variablesMap The variables map of the execution configuration
   * @return The full VFS filename reference to the serialized export interface XML file in the ZIP
   *     archive.
   * @throws HopException in case anything goes wrong during serialization
   */
  public static final TopLevelResource serializeResourceExportInterface(
      String zipFilename,
      IResourceExport resourceExportInterface,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      IXml executionConfiguration,
      String injectFilename,
      String sourceResourceFolderMapping,
      String targetResourceFolderMapping,
      Map<String, String> variablesMap)
      throws HopException {

    ZipOutputStream out = null;

    try {
      Map<String, ResourceDefinition> definitions = new HashMap<>();

      IResourceNaming namingInterface = new SequenceResourceNaming();

      String topLevelResource =
          resourceExportInterface.exportResources(
              variables, definitions, namingInterface, metadataProvider);

      if (topLevelResource != null && !definitions.isEmpty()) {

        // See if we need to rename folders and if we need to expose those as variables in the
        // execution configuration...
        //
        // See if we need to rename named resource folders...
        // We have a list of these in the naming interface
        //
        Map<String, String> dirMap = namingInterface.getDirectoryMap();
        if (StringUtils.isNotEmpty(sourceResourceFolderMapping)
            && StringUtils.isNotEmpty(targetResourceFolderMapping)) {
          FileObject sourceReference =
              HopVfs.getFileObject(variables.resolve(sourceResourceFolderMapping));

          for (String sourceDirectory : dirMap.keySet()) {
            // Calculate the relative path compared to the source resource folder
            // From: ${PROJECT_HOME}/files
            // To:   files
            //
            FileObject source = HopVfs.getFileObject(variables.resolve(sourceDirectory));
            String relativePath = sourceReference.getName().getRelativeName(source.getName());

            // So now simply add the relative path to the target folder
            //
            String targetDirectory = targetResourceFolderMapping;
            if (!targetDirectory.endsWith("/")) {
              targetDirectory += "/";
            }
            targetDirectory += relativePath;
            String parameter = dirMap.get(sourceDirectory);

            // Set the resulting variable in the variables map of the execution configuration
            //
            variablesMap.put(parameter, targetDirectory);
          }
        }

        // In case we want to add an extra pay-load to the exported ZIP file.
        // We add an extra file definition which gets picked up below and zipped up.
        //
        if (executionConfiguration != null) {
          ResourceDefinition resourceDefinition =
              new ResourceDefinition(injectFilename, executionConfiguration.getXml(variables));
          definitions.put(injectFilename, resourceDefinition);
        }

        // Create the ZIP file...
        //
        FileObject fileObject = HopVfs.getFileObject(zipFilename);

        // Store the XML in the definitions in a ZIP file...
        //
        out = new ZipOutputStream(HopVfs.getOutputStream(fileObject, false));

        for (String filename : definitions.keySet()) {
          ResourceDefinition resourceDefinition = definitions.get(filename);

          ZipEntry zipEntry = new ZipEntry(resourceDefinition.getFilename());

          String comment =
              BaseMessages.getString(
                  PKG,
                  "ResourceUtil.SerializeResourceExportInterface.ZipEntryComment.OriginatingFile",
                  filename,
                  Const.NVL(resourceDefinition.getOrigin(), "-"));
          zipEntry.setComment(comment);
          out.putNextEntry(zipEntry);

          out.write(resourceDefinition.getContent().getBytes());
          out.closeEntry();
        }

        // Add the metadata JSON file
        //
        ZipEntry jsonEntry = new ZipEntry("metadata.json");
        jsonEntry.setComment("Export of the client metadata");
        out.putNextEntry(jsonEntry);
        out.write(new SerializableMetadataProvider(metadataProvider).toJson().getBytes("UTF-8"));
        out.closeEntry();

        String zipURL = fileObject.getName().toString();
        return new TopLevelResource(
            topLevelResource, zipURL, "zip:" + zipURL + "!" + topLevelResource);
      } else {
        throw new HopException(
            BaseMessages.getString(PKG, "ResourceUtil.Exception.NoResourcesFoundToExport"));
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "ResourceUtil.Exception.ErrorSerializingExportInterface",
              resourceExportInterface.toString()),
          e);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ResourceUtil.Exception.ErrorClosingZipStream", zipFilename));
        }
      }
    }
  }

  public static String getExplanation(
      String zipFilename, String launchFile, IResourceExport resourceExportInterface) {

    String commandString = "";
    if (Const.isWindows()) {
      if (resourceExportInterface instanceof PipelineMeta) {
        commandString += "Pan.bat /file:\"";
      } else {
        commandString += "Kitchen.bat /file:\"";
      }
    } else {
      if (resourceExportInterface instanceof PipelineMeta) {
        commandString += "sh pan.sh -file='";
      } else {
        commandString += "sh kitchen.sh -file='";
      }
    }
    commandString += launchFile;
    if (Const.isWindows()) {
      commandString += "\"";
    } else {
      commandString += "'";
    }

    String message =
        BaseMessages.getString(
            PKG,
            "ResourceUtil.ExportResourcesExplanation",
            zipFilename,
            commandString,
            launchFile,
            Const.CR);
    return message;
  }
}
