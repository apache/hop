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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.*;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

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
  public static final String FILENAME_STATE_JSON = "state.json";

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

  public FileExecutionInfoLocation(String rootFolder) {
    this.pluginId = "local-folder";
    this.pluginName = "File location";
    this.rootFolder = rootFolder;
  }

  public FileExecutionInfoLocation(FileExecutionInfoLocation location) {
    this.pluginId = location.pluginId;
    this.pluginName = location.pluginName;
    this.rootFolder = location.rootFolder;
  }

  public FileExecutionInfoLocation clone() {
    return new FileExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    // Nothing to do here really.
  }

  @Override
  public void close() throws HopException {
    // Nothing to close
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
      List<String> ids = getExecutionIds(true, 100);
      for (String id : ids) {
        Execution execution = getExecution(id);
        if (execution.getExecutionType() == executionType && name.equals(execution.getName())) {
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
      ExecutionState oldState = getExecutionState(executionState.getId());
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
      try (OutputStream outputStream = HopVfs.getOutputStream(logFilename, false)) {
        outputStream.write(executionState.getLoggingText().getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      throw new HopException("Error updating execution information", e);
    }
  }

  @Override
  public ExecutionState getExecutionState(String executionId) throws HopException {
    try {
      String updateFilename = getUpdateFilename(executionId);
      if (!HopVfs.fileExists(updateFilename)) {
        return null;
      }
      try (InputStream inputStream = HopVfs.getInputStream(updateFilename)) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(inputStream, ExecutionState.class);
      }
    } catch (Exception e) {
      throw new HopException("Unable to get the execution status for ID " + executionId, e);
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
      // The parent folder(s) should already exist at this time!
      //
      String dataFilename = getDataFilename(data);

      try (OutputStream outputStream = HopVfs.getOutputStream(dataFilename, false)) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, data);
      }
    } catch (Exception e) {
      throw new HopException("Error storing execution data", e);
    }
  }

  @Override
  public List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException {
    try {
      // The list of IDs is simply the content of the pipelines and workflows folders
      //
      List<ExecutionIdAndDate> list = new ArrayList<>();

      List<FileObject> subFolders = new ArrayList<>();

      FileObject folder = HopVfs.getFileObject(rootFolder);
      if (!folder.exists()) {
        return Collections.emptyList();
      }
      for (FileObject child : folder.getChildren()) {
        if (child.isFolder()) {
          subFolders.add(child);
        }
      }

      // There should be a file called execution.json in the folder, otherwise we ignore it
      //
      for (FileObject subFolder : subFolders) {
        FileObject executionFileObject = subFolder.getChild(FILENAME_EXECUTION_JSON);
        if (executionFileObject != null && executionFileObject.exists()) {
          ObjectMapper objectMapper = new ObjectMapper();
          Execution execution;
          ExecutionState state = null;
          try (InputStream inputStream = HopVfs.getInputStream(executionFileObject)) {
            execution = objectMapper.readValue(inputStream, Execution.class);
          }
          try (InputStream inputStream =
              HopVfs.getInputStream(subFolder.getChild(FILENAME_STATE_JSON))) {
            state = objectMapper.readValue(inputStream, ExecutionState.class);
          } catch (Exception e) {
            // Ignore
          }
          String id = execution.getId();
          Date startDate = execution.getExecutionStartDate();
          Date updateDate = state == null ? null : state.getUpdateTime();

          if (includeChildren || StringUtils.isEmpty(execution.getParentId())) {
            list.add(new ExecutionIdAndDate(id, startDate, updateDate));
          }
        }
      }

      // Now reverse sort the list by date (latest updated first)
      //
      Collections.sort(list, ExecutionIdAndDate::compareTo);

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
  public List<String> findChildIds(ExecutionType parentExecutionType, String executionId)
      throws HopException {
    try {
      List<String> ids = new ArrayList<>();

      // For a workflow to find its children.
      // For a Beam pipeline to find child transforms.
      //
      String suffix = "-data.json";
      FileObject folderObject = HopVfs.getFileObject(getSubFolder(executionId));

      // In this folder we have a number of files ending with "-data.json"
      for (FileObject child : folderObject.getChildren()) {
        if (child == null) {
          continue;
        }
        String baseName = child.getName().getBaseName();
        if (baseName.endsWith(suffix)) {
          String id = baseName.substring(0, baseName.length() - suffix.length());
          ids.add(id);
        }
      }

      return ids;
    } catch (Exception e) {
      throw new HopException(
          "Error finding children of " + parentExecutionType.name() + " execution " + executionId,
          e);
    }
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    try {
      // Look in the pipeline executions
      //
      try (FileObject folder = HopVfs.getFileObject(getSubFolder(executionId))) {
        if (folder == null || !folder.exists()) {
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

  @Override
  public List<Execution> findExecutions(String parentExecutionId) throws HopException {
    try {
      List<Execution> executions = new ArrayList<>();

      for (String id : getExecutionIds(true, 10000)) {
        Execution execution = getExecution(id);
        if (parentExecutionId.equals(execution.getParentId())) {
          executions.add(execution);
        }
      }
      return executions;
    } catch (Exception e) {
      throw new HopException(
          "Error finding child executions for parent ID " + parentExecutionId, e);
    }
  }

  @Override
  public List<Execution> findExecutions(IExecutionMatcher matcher) throws HopException {
    try {
      List<Execution> executions = new ArrayList<>();

      for (String id : getExecutionIds(true, 0)) {
        Execution execution = getExecution(id);
        if (matcher.matches(execution)) {
          executions.add(execution);
        }
      }
      return executions;
    } catch (Exception e) {
      throw new HopException("Error finding executions with a matcher", e);
    }
  }

  @Override
  public Execution findPreviousSuccessfulExecution(ExecutionType executionType, String name)
      throws HopException {
    try {
      List<Execution> executions =
          findExecutions(e -> e.getExecutionType() == executionType && name.equals(e.getName()));
      for (Execution execution : executions) {
        ExecutionState executionState = getExecutionState(execution.getId());
        if (executionState != null && !executionState.isFailed()) {
          return execution;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException("Error finding previous successful execution", e);
    }
  }

  @Override
  public String findParentId(String childId) throws HopException {
    try {
      for (String id : getExecutionIds(true, 100)) {
        ExecutionState executionState = getExecutionState(id);
        if (executionState.getChildIds().contains(childId)) {
          return id;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException("Error finding parent execution for child ID " + childId, e);
    }
  }

  @Override
  public ExecutionData getExecutionData(String parentExecutionId, String executionId)
      throws HopException {
    try {
      try (FileObject folder = HopVfs.getFileObject(getSubFolder(parentExecutionId))) {
        if (!folder.exists()) {
          return null;
        }

        FileObject dataFileObject = folder.getChild(executionId + "-data.json");
        if (dataFileObject == null || !dataFileObject.exists()) {
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
    return rootFolder + "/" + registration.getId();
  }

  private String getSubFolder(ExecutionState update) {
    return getSubFolder(update.getId());
  }

  private String getSubFolder(String executionId) {
    return rootFolder + "/" + executionId;
  }

  private String getSubFolder(ExecutionData data) {
    return getSubFolder(data.getParentId());
  }

  private String getUpdateFilename(ExecutionState update) {
    return getUpdateFilename(update.getId());
  }

  private String getUpdateFilename(String id) {
    String filename = getSubFolder(id);
    filename += "/" + FILENAME_STATE_JSON;
    return filename;
  }

  private String getLogFilename(ExecutionState update) {
    String filename = getSubFolder(update);
    filename += "/" + update.getExecutionType().getFilePrefix() + "-" + "log.txt";
    return filename;
  }

  private String getDataFilename(ExecutionData data) {
    String filename = getSubFolder(data);
    filename += "/" + data.getOwnerId() + "-data.json";
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
    public Date startDate;
    public Date updateDate;

    public ExecutionIdAndDate(String id, Date startDate, Date updateDate) {
      this.id = id;
      this.startDate = startDate;
      this.updateDate = updateDate;
    }

    public int compareTo(ExecutionIdAndDate e) {
      if (e.updateDate == null || updateDate == null) {
        return -startDate.compareTo(e.startDate);
      }
      return -updateDate.compareTo(e.updateDate);
    }
  }
}
