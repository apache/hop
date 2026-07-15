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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;

public class ResourceUtil {

  private static final Class<?> PKG = ResourceUtil.class;

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
        // Bundle the pipelines/workflows that are referenced from metadata objects (Pipeline Log,
        // Workflow Log, Pipeline Probe, variable resolvers, web services, ...) and rewrite those
        // references to point at the copies inside the ZIP archive. Otherwise those pipelines are
        // missing on the remote server (see #3368).
        //
        // This is done first because bundling a referenced pipeline can add new named-resource
        // folders (DATA_PATH_n) which then need a value assigned below.
        //
        SerializableMetadataProvider exportMetadataProvider =
            exportMetadataReferencedResources(
                variables, definitions, namingInterface, metadataProvider);

        // See if we need to rename named resource folders...
        // We have a list of these in the naming interface
        //
        // Give every generated folder variable (DATA_PATH_1, ...) a value in the execution
        // configuration. Otherwise the rewritten filenames (${DATA_PATH_1}/file.txt) stay
        // unresolved on the remote server (see #7209).
        //
        assignNamedResourceDirectoryVariables(
            variables,
            namingInterface.getDirectoryMap(),
            sourceResourceFolderMapping,
            targetResourceFolderMapping,
            variablesMap);

        // In case we want to add an extra pay-load to the exported ZIP file.
        // We add an extra file definition which gets picked up below and zipped up.
        //
        if (executionConfiguration != null) {
          String encoding = Const.UTF_8;
          ResourceDefinition resourceDefinition =
              new ResourceDefinition(
                  injectFilename,
                  XmlHandler.getXmlHeader(encoding) + executionConfiguration.getXml(variables));
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
        out.write(exportMetadataProvider.toJson().getBytes(StandardCharsets.UTF_8));
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

  /**
   * Assign a value to every generated named-resource folder variable (e.g. {@code DATA_PATH_1})
   * that was created while renaming referenced files during a resource export.
   *
   * <p>When both a source and target resource folder are configured, the referenced folders are
   * mapped from the source folder onto the target folder on the server (e.g. {@code
   * ${PROJECT_HOME}/files} becomes {@code <target>/files}). Otherwise each variable defaults to the
   * same folder as on the executing (local) machine, so the rewritten {@code ${DATA_PATH_n}/file}
   * references still resolve. Leaving these variables unset resulted in unresolved {@code
   * ${DATA_PATH_n}} paths on remote Hop servers (#7209).
   *
   * @param variables the variable space used to resolve folders
   * @param directoryMap map of referenced source folder to generated variable name
   * @param sourceResourceFolderMapping optional source folder to map from (e.g. ${PROJECT_HOME})
   * @param targetResourceFolderMapping optional target folder on the server (e.g. /server/)
   * @param variablesMap the execution configuration variables map to populate
   */
  static void assignNamedResourceDirectoryVariables(
      IVariables variables,
      Map<String, String> directoryMap,
      String sourceResourceFolderMapping,
      String targetResourceFolderMapping,
      Map<String, String> variablesMap)
      throws HopException, FileSystemException {

    boolean mapToTargetFolder =
        StringUtils.isNotEmpty(sourceResourceFolderMapping)
            && StringUtils.isNotEmpty(targetResourceFolderMapping);

    FileObject sourceReference = null;
    if (mapToTargetFolder) {
      sourceReference = HopVfs.getFileObject(variables.resolve(sourceResourceFolderMapping));
    }

    for (Map.Entry<String, String> entry : directoryMap.entrySet()) {
      String sourceDirectory = entry.getKey();
      String parameter = entry.getValue();
      String targetDirectory;

      if (mapToTargetFolder) {
        // Calculate the relative path compared to the source resource folder and add it to the
        // target folder.
        // From: ${PROJECT_HOME}/files
        // To:   <target folder>/files
        //
        FileObject source = HopVfs.getFileObject(variables.resolve(sourceDirectory));
        String relativePath = sourceReference.getName().getRelativeName(source.getName());
        targetDirectory = targetResourceFolderMapping;
        if (!targetDirectory.endsWith("/")) {
          targetDirectory += "/";
        }
        targetDirectory += relativePath;
      } else {
        // Default: use the same folder as on the executing (local) machine.
        //
        targetDirectory = variables.resolve(sourceDirectory);
      }

      // Set the resulting variable in the variables map of the execution configuration
      //
      variablesMap.put(parameter, targetDirectory);
    }
  }

  /**
   * Bundle the pipelines and workflows that are referenced from metadata objects (e.g. Pipeline
   * Log, Workflow Log, Pipeline Probe, variable resolvers, web services) into the export {@code
   * definitions} and rewrite those references so they resolve inside the ZIP archive on the remote
   * server. Metadata objects are only serialized to {@code metadata.json}; unlike transforms and
   * actions they are never walked by the normal resource export, so their referenced pipelines went
   * missing on the server (#3368).
   *
   * <p>The rewriting is done on an independent deep copy of the metadata so the caller's live
   * metadata objects are never modified.
   *
   * @return a metadata provider holding the rewritten copy, to be serialized into the ZIP archive.
   */
  static SerializableMetadataProvider exportMetadataReferencedResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    // Deep copy through a JSON round-trip so we never mutate the caller's live metadata objects
    // (for in-memory providers the objects would otherwise be shared by reference).
    //
    SerializableMetadataProvider exportMetadataProvider =
        new SerializableMetadataProvider(
            new SerializableMetadataProvider(metadataProvider).toJson());

    Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());

    for (Class<IHopMetadata> metadataClass : exportMetadataProvider.getMetadataClasses()) {
      IHopMetadataSerializer<IHopMetadata> serializer =
          exportMetadataProvider.getSerializer(metadataClass);
      for (String name : serializer.listObjectNames()) {
        IHopMetadata object = serializer.load(name);
        if (rewriteMetadataFileReferences(
            object, visited, variables, definitions, namingInterface, metadataProvider)) {
          serializer.save(object);
        }
      }
    }
    return exportMetadataProvider;
  }

  /**
   * Recursively walk a metadata object, bundling and rewriting every {@link
   * HopMetadataPropertyType#PIPELINE_FILE} / {@link HopMetadataPropertyType#WORKFLOW_FILE} String
   * reference it (or a nested metadata object / list) holds.
   *
   * @return true if anything was rewritten.
   */
  private static boolean rewriteMetadataFileReferences(
      Object object,
      Set<Object> visited,
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    if (object == null || !visited.add(object)) {
      return false;
    }
    boolean changed = false;

    for (Field field : ReflectionUtil.findAllFields(object.getClass())) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property == null) {
        continue;
      }
      HopMetadataPropertyType type = property.hopMetadataPropertyType();
      boolean fileReference =
          type == HopMetadataPropertyType.PIPELINE_FILE
              || type == HopMetadataPropertyType.WORKFLOW_FILE
              || type == HopMetadataPropertyType.HOP_FILE;

      Object value = getMetadataFieldValue(object, field);
      if (value == null) {
        continue;
      }

      if (value instanceof String stringValue) {
        if (fileReference && StringUtils.isNotEmpty(stringValue)) {
          String rewritten =
              exportReferencedFile(
                  stringValue, type, variables, definitions, namingInterface, metadataProvider);
          if (rewritten != null && !rewritten.equals(stringValue)) {
            changed |= setMetadataFieldValue(object, field, rewritten);
          }
        }
      } else if (value instanceof List<?> list) {
        for (int i = 0; i < list.size(); i++) {
          Object item = list.get(i);
          if (item instanceof String stringItem) {
            if (fileReference && StringUtils.isNotEmpty(stringItem)) {
              String rewritten =
                  exportReferencedFile(
                      stringItem, type, variables, definitions, namingInterface, metadataProvider);
              if (rewritten != null && !rewritten.equals(stringItem)) {
                @SuppressWarnings("unchecked")
                List<Object> objectList = (List<Object>) list;
                objectList.set(i, rewritten);
                changed = true;
              }
            }
          } else if (isRecursableMetadataObject(item)) {
            changed |=
                rewriteMetadataFileReferences(
                    item, visited, variables, definitions, namingInterface, metadataProvider);
          }
        }
      } else if (isRecursableMetadataObject(value)) {
        changed |=
            rewriteMetadataFileReferences(
                value, visited, variables, definitions, namingInterface, metadataProvider);
      }
    }
    return changed;
  }

  private static Object getMetadataFieldValue(Object object, Field field) {
    try {
      return ReflectionUtil.getFieldValue(
          object, field.getName(), field.getType() == boolean.class);
    } catch (Exception e) {
      // A metadata property without a conventional getter must never break the whole export;
      // just skip that field (its reference, if any, is left unchanged).
      return null;
    }
  }

  private static boolean setMetadataFieldValue(Object object, Field field, String value) {
    try {
      ReflectionUtil.setFieldValue(object, field.getName(), String.class, value);
      return true;
    } catch (Exception e) {
      // No usable setter: leave the reference unchanged rather than break the export.
      return false;
    }
  }

  /** Only recurse into Hop metadata objects, not JDK types, enums or primitives. */
  private static boolean isRecursableMetadataObject(Object value) {
    if (value == null) {
      return false;
    }
    Class<?> clazz = value.getClass();
    return !clazz.isEnum() && !clazz.isPrimitive() && clazz.getName().startsWith("org.apache.hop.");
  }

  /**
   * Load the referenced pipeline or workflow, export its resources into {@code definitions} and
   * return a reference that resolves against the ZIP archive on the server ({@code
   * ${Internal.Entry.Current.Folder}/<bundled name>}). For an ambiguous {@link
   * HopMetadataPropertyType#HOP_FILE} reference the type is decided from the {@code .hwf}/{@code
   * .hpl} extension (defaulting to a pipeline). A reference that cannot be resolved to an existing
   * file, or that fails to load/export, is left unchanged: the export must not fail on a peripheral
   * or stale metadata reference (the server logs and skips it).
   */
  private static String exportReferencedFile(
      String filename,
      HopMetadataPropertyType type,
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider) {

    String realFilename = variables.resolve(filename);
    try {
      if (!HopVfs.getFileObject(realFilename).exists()) {
        return filename;
      }
    } catch (Exception e) {
      // Can't tell whether it exists; leave the reference untouched rather than break the export.
      return filename;
    }

    boolean workflowFile =
        type == HopMetadataPropertyType.WORKFLOW_FILE
            || (type == HopMetadataPropertyType.HOP_FILE
                && realFilename.toLowerCase().endsWith(".hwf"));

    try {
      String bundledName;
      if (workflowFile) {
        WorkflowMeta workflowMeta = new WorkflowMeta(variables, realFilename, metadataProvider);
        bundledName =
            workflowMeta.exportResources(variables, definitions, namingInterface, metadataProvider);
      } else {
        PipelineMeta pipelineMeta = new PipelineMeta(realFilename, metadataProvider, variables);
        bundledName =
            pipelineMeta.exportResources(variables, definitions, namingInterface, metadataProvider);
      }
      return "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + bundledName;
    } catch (Exception e) {
      // A peripheral metadata reference must not break the whole export; leave it unchanged.
      return filename;
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

    return BaseMessages.getString(
        PKG,
        "ResourceUtil.ExportResourcesExplanation",
        zipFilename,
        commandString,
        launchFile,
        Const.CR);
  }
}
