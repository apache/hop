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
 *
 */

package org.apache.hop.execution.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.*;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@GuiPlugin(description = "File execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "local-folder",
    name = "File location",
    description = "Stores execution information in a folder structure")
public class FileExecutionInfoLocation implements IExecutionInfoLocation {

  public static final String FILENAME_EXECUTION_JSON = "execution.json";
  @HopMetadataProperty protected String pluginId;

  @HopMetadataProperty protected String pluginName;

  @GuiWidgetElement(
      id = "rootFolder",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      toolTip = "i18n::LocalExecutionInfoLocation.RootFolder.Tooltip",
      label = "i18n::LocalExecutionInfoLocation.RootFolder.Label")
  @HopMetadataProperty
  protected String rootFolder;

  public FileExecutionInfoLocation() {}

  public FileExecutionInfoLocation(FileExecutionInfoLocation location) {
    this.pluginId = location.pluginId;
    this.pluginName = location.pluginName;
    this.rootFolder = location.rootFolder;
  }

  public FileExecutionInfoLocation clone() {
    return new FileExecutionInfoLocation(this);
  }

  @Override
  public void registerExecution(Execution execution) throws HopException {
    try {
      // Register this execution with the
      //
      String folderName = getSubFolder(execution);

      // We can write out a single simple JSON file with execution details
      //
      String registrationFileName = folderName + "/" + FILENAME_EXECUTION_JSON;

      // Create the folder(s) of the parent:
      //
      HopVfs.getFileObject(registrationFileName).getParent().createFolder();

      // Write the execution information to disk...
      //
      try (OutputStream outputStream = HopVfs.getOutputStream(registrationFileName, false)) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, execution);
      }
    } catch (Exception e) {
      throw new HopException("Error registering execution information", e);
    }
  }

  @Override
  public Execution findLastExecution(ExecutionType executionType, String name) throws HopException {
    try {
      List<String> ids =
          getExecutionIds(
              executionType == ExecutionType.Pipeline,
              executionType == ExecutionType.Workflow,
              100);
      for (String id : ids) {
        Execution execution = getExecution(id);
        if (name.equals(execution.getName())) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(
          "Error looking up the last execution of type " + executionType + " and name " + name, e);
    }
  }

  @Override
  public void updateExecutionState(ExecutionState executionState) throws HopException {
    try {
      // We need to add the logging text incrementally.
      // This means: read the previous value first and then add the new lines here...
      //
      ExecutionState oldState =
          getExecutionState(executionState.getExecutionType(), executionState.getId());
      if (oldState != null) {
        executionState.setLoggingText(oldState.getLoggingText() + executionState.getLoggingText());
      }

      // We'll store the execution updates for transforms and actions in the same folder as the
      // corresponding pipeline or workflow.
      // It will be easier and faster to get information from smaller folders.
      //
      // Let's store the information update in a file called Pipeline-update.json
      // or Transform-update-<UUID>.json and similar for Workflows and actions
      //
      String updateFilename = getUpdateFilename(executionState);

      // Create the folder(s) of the parent if needed:
      //
      HopVfs.getFileObject(updateFilename).getParent().createFolder();

      try (OutputStream outputStream = HopVfs.getOutputStream(updateFilename, false)) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, executionState);
      }

      // Also append to a log file...
      //
      String logFilename = getLogFilename(executionState);
      try (OutputStream outputStream = HopVfs.getOutputStream(logFilename, true)) {
        outputStream.write(executionState.getLoggingText().getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      throw new HopException("Error updating execution information", e);
    }
  }

  @Override
  public ExecutionState getExecutionState(ExecutionType type, String executionId)
      throws HopException {
    try {
      String updateFilename = getUpdateFilename(type, executionId);
      if (!HopVfs.fileExists(updateFilename)) {
        return null;
      }
      try (InputStream inputStream = HopVfs.getInputStream(updateFilename)) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(inputStream, ExecutionState.class);
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to get the execution status for type " + type + " and ID " + executionId, e);
    }
  }

  /**
   * register output data for a given transform
   *
   * @param data
   * @throws HopException
   */
  public void registerData(ExecutionData data) throws HopException {
    try {
      // We simply store the data in a file with the ID of the transform in the name
      //
      String dataFilename = getDataFilename(data);

      // Create the folder(s) of the parent if needed:
      //
      HopVfs.getFileObject(dataFilename).getParent().createFolder();

      try (OutputStream outputStream = HopVfs.getOutputStream(dataFilename, false)) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, data);
      }
    } catch (Exception e) {
      throw new HopException("Error storing execution data", e);
    }
  }

  @Override
  public List<String> getExecutionIds(boolean pipelines, boolean workflows, int limit)
      throws HopException {
    try {
      // The list of IDs is simply the content of the pipelines and workflows folders
      //
      List<ExecutionIdAndDate> list = new ArrayList<>();

      List<FileObject> subFolders = new ArrayList<>();
      if (pipelines) {
        FileObject folder = HopVfs.getFileObject(rootFolder);
        for (FileObject child : folder.getChildren()) {
          if (child.isFolder()) {
            if (child
                .getName()
                .getBaseName()
                .startsWith(ExecutionType.Pipeline.getFolderPrefix() + "-")) {
              subFolders.add(child);
            }
          }
        }
      }
      if (workflows) {
        FileObject folder = HopVfs.getFileObject(rootFolder);
        for (FileObject child : folder.getChildren()) {
          if (child.isFolder()) {
            if (child
                .getName()
                .getBaseName()
                .startsWith(ExecutionType.Workflow.getFolderPrefix() + "-")) {
              subFolders.add(child);
            }
          }
        }
      }

      // There should be a file called execution.json in the folder, otherwise we ignore it
      //
      for (FileObject subFolder : subFolders) {
        FileObject executionFileObject = subFolder.getChild(FILENAME_EXECUTION_JSON);
        if (executionFileObject.exists()) {
          ObjectMapper objectMapper = new ObjectMapper();
          try (InputStream inputStream = HopVfs.getInputStream(executionFileObject)) {
            Execution execution = objectMapper.readValue(inputStream, Execution.class);
            list.add(new ExecutionIdAndDate(execution.getId(), execution.getExecutionStartDate()));
          }
        }
      }

      // Now reverse sort the list by date (latest updated first)
      //
      Collections.sort(list, Comparator.comparing(one -> one.date, Comparator.reverseOrder()));

      // Collect the IDs
      List<String> ids = new ArrayList<>();
      list.forEach(
          e -> {
            if (limit <= 0 || ids.size() < limit) {
              ids.add(e.id);
            }
          });

      return ids;
    } catch (Exception e) {
      throw new HopException("Error listing execution IDs", e);
    }
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    try {
      // Look in the pipeline executions
      //
      try (FileObject folder = lookupPipelineOrWorkflowFileObject(executionId)) {
        if (!folder.exists()) {
          // No Execution info to be found
          return null;
        }
        FileObject executionFileObject = folder.getChild(FILENAME_EXECUTION_JSON);
        if (!executionFileObject.exists()) {
          // No information for this ID
          return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream inputStream = HopVfs.getInputStream(executionFileObject)) {
          return objectMapper.readValue(inputStream, Execution.class);
        }
      }
    } catch (Exception e) {
      throw new HopException("Error getting execution information for ID " + executionId, e);
    }
  }

  private FileObject lookupPipelineOrWorkflowFileObject(String executionId)
      throws HopFileException, FileSystemException {
    FileObject fileObject =
        HopVfs.getFileObject(
            rootFolder + "/" + ExecutionType.Pipeline.getFolderPrefix() + "-" + executionId);
    if (!fileObject.exists()) {
      fileObject =
          HopVfs.getFileObject(
              rootFolder + "/" + ExecutionType.Workflow.getFolderPrefix() + "-" + executionId);
    }
    return fileObject;
  }

  public ExecutionData getExecutionData(String parentExecutionId) throws HopException {
    try {
      try (FileObject folder = lookupPipelineOrWorkflowFileObject(parentExecutionId)) {
        if (!folder.exists()) {
          return null;
        }

        FileObject dataFileObject = folder.getChild("transform-data-all-transforms.json");
        if (!dataFileObject.exists()) {
          return null;
        }
        try (InputStream inputStream = HopVfs.getInputStream(dataFileObject)) {
          ObjectMapper objectMapper = new ObjectMapper();
          return objectMapper.readValue(inputStream, ExecutionData.class);
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error looking up execution data for parent execution ID " + parentExecutionId, e);
    }
  }

  private String getSubFolder(Execution registration) {
    return rootFolder
        + "/"
        + registration.getExecutionType().getFolderPrefix()
        + "-"
        + registration.getId();
  }

  private String getSubFolder(ExecutionState update) {
    return getSubFolder(update.getExecutionType(), update.getId());
  }

  private String getSubFolder(ExecutionType executionType, String executionId) {
    switch (executionType) {
      case Pipeline:
      case Transform:
        return rootFolder + "/" + ExecutionType.Pipeline.getFolderPrefix() + "-" + executionId;
      case Workflow:
      case Action:
        return rootFolder + "/" + ExecutionType.Workflow.getFolderPrefix() + "-" + executionId;
      default:
        throw new RuntimeException("Unhandled execution type: " + executionType);
    }
  }

  private String getSubFolder(ExecutionData data) {
    return rootFolder + "/" + ExecutionType.Pipeline.getFolderPrefix() + "-" + data.getParentId();
  }

  private String getUpdateFilename(ExecutionState update) {
    return getUpdateFilename(update.getExecutionType(), update.getId());
  }

  private String getUpdateFilename(ExecutionType type, String id) {
    String filename = getSubFolder(type, id);
    filename += "/" + type.getFilePrefix() + "-state.json";
    return filename;
  }

  private String getLogFilename(ExecutionState update) {
    String filename = getSubFolder(update);
    filename += "/" + update.getExecutionType().getFilePrefix() + "-" + "log.txt";
    return filename;
  }

  private String getDataFilename(ExecutionData data) {
    String filename = getSubFolder(data);
    filename +=
        "/" + ExecutionType.Transform.getFilePrefix() + "-data-" + data.getOwnerId() + ".json";
    return filename;
  }

  @Override
  public String getPluginId() {
    return pluginId;
  }

  @Override
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  @Override
  public String getPluginName() {
    return pluginName;
  }

  @Override
  public void setPluginName(String pluginName) {
    this.pluginName = pluginName;
  }

  public String getRootFolder() {
    return rootFolder;
  }

  public void setRootFolder(String rootFolder) {
    this.rootFolder = rootFolder;
  }

  private static class ExecutionIdAndDate {
    public String id;
    public Date date;

    public ExecutionIdAndDate(String id, Date date) {
      this.id = id;
      this.date = date;
    }
  }
}
