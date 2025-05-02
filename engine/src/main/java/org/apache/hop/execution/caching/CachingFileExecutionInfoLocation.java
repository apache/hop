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

package org.apache.hop.execution.caching;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionMatcher;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@GuiPlugin(description = "Caching File execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "caching-file-location",
    name = "Caching File location",
    description = "Aggregates and caches execution information before storing in files")
@Getter
@Setter
public class CachingFileExecutionInfoLocation implements IExecutionInfoLocation {
  @GuiWidgetElement(
      id = "rootFolder",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      toolTip = "i18n::CachingFileExecutionInfoLocation.RootFolder.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.RootFolder.Label")
  @HopMetadataProperty
  protected String rootFolder;

  @GuiWidgetElement(
      id = "persistenceDelay",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingFileExecutionInfoLocation.PersistenceDelay.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.PersistenceDelay.Label")
  @HopMetadataProperty
  protected String persistenceDelay = "5000";

  @GuiWidgetElement(
      id = "maxCacheAge",
      order = "030",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingFileExecutionInfoLocation.MaxCacheAge.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.MaxCacheAge.Label")
  @HopMetadataProperty
  protected String maxCacheAge = "86400000";

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;
  protected String actualRootFolder;

  // This is the main cache
  protected Map<String, CacheEntry> cache;

  protected Timer cacheTimer;

  public CachingFileExecutionInfoLocation() {
    cache = new HashMap<>();
    this.cacheTimer = null;
    this.actualRootFolder = null;
    this.locked = new AtomicBoolean(false);
  }

  public CachingFileExecutionInfoLocation(String rootFolder, String maxCacheAge) {
    this();
    this.rootFolder = rootFolder;
    this.maxCacheAge = maxCacheAge;
  }

  public CachingFileExecutionInfoLocation(CachingFileExecutionInfoLocation location) {
    this();
    this.rootFolder = location.rootFolder;
    this.maxCacheAge = location.maxCacheAge;
    this.persistenceDelay = location.persistenceDelay;
    this.variables = location.variables;
    this.metadataProvider = location.metadataProvider;
  }

  @Override
  public IExecutionInfoLocation clone() {
    return new CachingFileExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    // The actual root folder
    //
    actualRootFolder = variables.resolve(rootFolder);

    // The default persistence delay is 1 minute
    //
    final int delay = Const.toInt(variables.resolve(persistenceDelay), 60000);

    // The default maximum cache age is 1 day
    //
    final int maxAge = Const.toInt(variables.resolve(maxCacheAge), 86400000);

    // Let's start a timer to manage the cache every second or so
    //
    cacheTimer = new Timer("Caching file execution location timer");
    TimerTask cacheManageTask =
        new TimerTask() {
          @Override
          public void run() {
            manageCache(actualRootFolder, delay, maxAge);
          }
        };
    cacheTimer.schedule(cacheManageTask, 1000L, 1000L);
  }

  /** */
  AtomicBoolean locked;

  private synchronized void manageCache(String actualRootFolder, int delay, int maxAge) {
    try {
      // Let's make sure we never run this method in parallel
      //
      if (locked.get()) {
        return;
      }
      locked.set(true);

      // See which dirty cache entries haven't been saved in a while.
      //
      for (CacheEntry cacheEntry : cache.values()) {
        if (cacheEntry.needsWriting(delay)) {
          cacheEntry.writeToDisk(actualRootFolder);
        }
      }

      // Perhaps there are cache entries which are getting too old?
      //
      List<String> tooOld = new ArrayList<>();
      for (CacheEntry cacheEntry : cache.values()) {
        if (cacheEntry.isTooOld(maxAge)) {
          tooOld.add(cacheEntry.getId());
        }
      }
      // Remove these entries.
      //
      tooOld.forEach(id -> cache.remove(id));
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error managing file execution information location cache", e);
    } finally {
      locked.set(false);
    }
  }

  @Override
  public synchronized void close() throws HopException {
    try {
      ExecutorUtil.cleanup(cacheTimer);
      for (CacheEntry cacheEntry : cache.values()) {
        if (cacheEntry.isDirty()) {
          cacheEntry.writeToDisk(actualRootFolder);
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error closing caching file execution information location in folder: "
              + actualRootFolder,
          e);
    }
  }

  @Override
  public synchronized void registerExecution(Execution execution) throws HopException {
    /*
     We're going to collect execution information of actions along with the parent workflow.
     Similarly, we're doing to add transform execution information under its parent pipeline.
     This way, we'll always just have one simple file to deal with.
    */
    ExecutionType type = execution.getExecutionType();
    if (type == ExecutionType.Pipeline || type == ExecutionType.Workflow) {
      addExecutionToCache(execution);
    } else {
      addChildExecutionToCache(execution);
    }
  }

  /**
   * Add the execution to the cache as a top level object.
   *
   * @param execution The execution to add to the cache
   */
  private synchronized void addExecutionToCache(Execution execution) {
    // Check if the execution is already in the cache...
    CacheEntry entry = cache.get(execution.getId());
    if (entry == null) {
      entry = new CacheEntry();
      entry.setId(execution.getId());
    }
    entry.setExecution(execution);
    entry.setName(execution.getName());
    entry.setDirty(true);
    entry.setLastWritten(null);

    cache.put(execution.getId(), entry);
  }

  private synchronized void addChildExecutionToCache(Execution execution) {
    // Find the parent in the cache.
    // We'll assume that the parent cache entry isn't removed while children are still executing.
    //
    CacheEntry entry = cache.get(execution.getParentId());
    entry.addChildExecution(execution);
  }

  @Override
  public synchronized void updateExecutionState(ExecutionState executionState) throws HopException {
    ExecutionType type = executionState.getExecutionType();
    if (type == ExecutionType.Pipeline || type == ExecutionType.Workflow) {
      addStateToCache(executionState);
    } else {
      addChildStateToCache(executionState);
    }
  }

  private synchronized void addStateToCache(ExecutionState executionState) {
    CacheEntry entry = cache.get(executionState.getId());
    // This entry should always exist
    entry.setExecutionState(executionState);
  }

  private synchronized void addChildStateToCache(ExecutionState executionState) {
    CacheEntry entry = cache.get(executionState.getParentId());
    // This parent entry should always exit
    entry.addChildExecutionState(executionState);
  }

  @Override
  public synchronized boolean deleteExecution(String executionId) throws HopException {
    CacheEntry removed = cache.remove(executionId);
    removed.deleteFromDisk(rootFolder);
    return true;
  }

  @Override
  public synchronized ExecutionState getExecutionState(String executionId) throws HopException {
    CacheEntry entry = findCacheEntry(executionId);
    if (entry == null) {
      return null;
    }
    if (entry.getId().equals(executionId)) {
      return entry.getExecutionState();
    }
    return entry.getChildExecutionState(executionId);
  }

  private synchronized CacheEntry findCacheEntry(String executionId) throws HopException {
    // Check the cache first...
    for (CacheEntry cacheEntry : cache.values()) {
      // See if this is a parent in the cache.
      //
      if (cacheEntry.getId().equals(executionId)) {
        return cacheEntry;
      }

      // Is it perhaps one of the children?
      //
      Execution childExecution = cacheEntry.getChildExecution(executionId);
      if (childExecution != null) {
        return cacheEntry;
      }
    }

    // We still haven't found anything in the cache.
    // Let's load this from disk.
    //
    CacheEntry entry = loadCacheEntry(executionId);
    if (entry != null) {
      entry.setLastRead(new Date());
      entry.setLastWritten(new Date());
      entry.setDirty(false);

      // Add this to the cache as well
      //
      cache.put(executionId, entry);

      return entry;
    }
    return null;
  }

  private synchronized CacheEntry loadCacheEntry(String executionId) throws HopException {
    try {
      CacheEntry entry = new CacheEntry();
      entry.setId(executionId);
      String filename = entry.calculateFilename(rootFolder);
      if (!HopVfs.fileExists(filename)) {
        return null;
      }
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(HopVfs.getInputStream(filename), CacheEntry.class);
    } catch (Exception e) {
      throw new HopException(
          "Error loading execution information location file for executionId '" + executionId + "'",
          e);
    }
  }

  @Override
  public synchronized ExecutionState getExecutionState(String executionId, boolean includeLogging)
      throws HopException {
    // This is the same as the other method ignoring the logging size, for now.
    //
    return getExecutionState(executionId);
  }

  @Override
  public synchronized String getExecutionStateLoggingText(String executionId, int sizeLimit)
      throws HopException {
    ExecutionState state = getExecutionState(executionId);
    if (state == null) {
      return null;
    }
    String log = state.getLoggingText();
    if (StringUtils.isEmpty(log)) {
      return null;
    }
    if (log.length() < sizeLimit) {
      return log;
    }
    return log.substring(0, sizeLimit);
  }

  /**
   * We need to add execution data to a child execution state.
   *
   * @param data The data to add to a child execution state.
   * @throws HopException In case we couldn't find or load the cache entry to register with
   */
  @Override
  public synchronized void registerData(ExecutionData data) throws HopException {
    // The ownerId in the data refers to the execution ID of the transform or action
    //
    CacheEntry entry = findCacheEntry(data.getParentId());
    if (entry == null) {
      throw new HopException(
          "Error finding execution state to register data, for execution id '"
              + data.getOwnerId()
              + "'");
    }
    entry.addExecutionData(data);
  }

  @Getter
  @Setter
  private static class DatedId {
    private String id;
    private Date date;

    DatedId(String id, Date date) {
      this.id = id;
      this.date = date;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DatedId)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      return id.equals(((DatedId) obj).id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }

  @Override
  public List<String> getExecutionIds(boolean includeChildren, int limit) throws HopException {
    Set<DatedId> ids = new HashSet<>();

    // The data in the cache is the most recent, so we start with that.
    //
    getExecutionIdsFromCache(ids, includeChildren);

    // Get all the IDs from disk if we don't have it in the cache.
    //
    try {
      FileObject[] files = getAllFileObjects(actualRootFolder);

      for (FileObject file : files) {
        String id = getIdFromFileName(file);

        if (!ids.contains(new DatedId(id, null))) {
          ids.add(new DatedId(id, new Date(file.getContent().getLastModifiedTime())));

          // To add child IDs we need to load the file.
          // We won't store these in the cache though.
          //
          if (includeChildren) {
            CacheEntry entry = loadCacheEntry(id);
            if (entry != null) {
              addChildIds(entry, ids);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error finding execution ids in folder '" + actualRootFolder + "'", e);
    }

    // Reverse sort the IDs by date
    //
    List<DatedId> datedIds = new ArrayList<>(ids);
    datedIds.sort(Comparator.comparing(DatedId::getDate));
    Collections.reverse(datedIds); // Newest first

    // Take only the first from the list
    //
    List<String> list = new ArrayList<>();
    for (int i = 0; i < datedIds.size() && i < limit; i++) {
      list.add(datedIds.get(i).getId());
    }
    return list;
  }

  private static void addChildIds(CacheEntry entry, Set<DatedId> ids) {
    for (String childId : entry.getChildIds()) {
      Execution childExecution = entry.getChildExecution(childId);
      ids.add(new DatedId(childExecution.getId(), childExecution.getRegistrationDate()));
    }
  }

  private String getIdFromFileName(FileObject file) {
    String baseName = file.getName().getBaseName();
    String id;
    int index = baseName.lastIndexOf(".json");
    if (index > 0) {
      id = baseName.substring(0, index);
    } else {
      id = baseName;
    }
    return id;
  }

  private FileObject[] getAllFileObjects(String actualRootFolder)
      throws FileSystemException, HopFileException {
    FileObject folder = HopVfs.getFileObject(actualRootFolder);
    return folder.findFiles(
        new AllFileSelector() {
          @Override
          public boolean includeFile(FileSelectInfo fileInfo) {
            return fileInfo.getFile().getName().getExtension().equalsIgnoreCase("json");
          }
        });
  }

  private void getExecutionIdsFromCache(Set<DatedId> ids, boolean includeChildren) {
    for (CacheEntry cacheEntry : cache.values()) {
      ids.add(new DatedId(cacheEntry.getId(), cacheEntry.getExecution().getRegistrationDate()));
      if (includeChildren) {
        addChildIds(cacheEntry, ids);
      }
    }
  }

  @Override
  public Execution getExecution(String executionId) throws HopException {
    CacheEntry entry = findCacheEntry(executionId);
    if (entry == null) {
      return null;
    }
    return entry.getExecution();
  }

  @Override
  public List<Execution> findExecutions(String parentExecutionId) throws HopException {
    try {
      Set<Execution> executions = new HashSet<>();

      for (String id : getExecutionIds(true, 10000)) {
        Execution execution = getExecution(id);
        if (execution != null && parentExecutionId.equals(execution.getParentId())) {
          executions.add(execution);
        }
      }
      return executions.stream().toList();
    } catch (Exception e) {
      throw new HopException(
          "Error finding child executions for parent ID " + parentExecutionId, e);
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
  public ExecutionData getExecutionData(String parentExecutionId, String executionId)
      throws HopException {
    try {
      CacheEntry cacheEntry = findCacheEntry(parentExecutionId);
      if (cacheEntry == null) {
        return null;
      }
      ExecutionData data = cacheEntry.getExecutionData(executionId);
      if (data == null) {
        // Retry for the exception for transforms: "all-transforms" stored together.
        data = cacheEntry.getExecutionData("all-transforms");
      }
      return data;
    } catch (Exception e) {
      throw new HopException(
          "Error finding execution data for parent execution ID " + executionId, e);
    }
  }

  @Override
  public Execution findLastExecution(ExecutionType executionType, String name) throws HopException {
    try {
      List<String> ids = getExecutionIds(true, 100);
      for (String id : ids) {
        Execution execution = getExecution(id);
        if (execution != null
            && execution.getExecutionType() == executionType
            && name.equals(execution.getName())) {
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
  public List<String> findChildIds(ExecutionType parentExecutionType, String parentExecutionId)
      throws HopException {
    CacheEntry cacheEntry = findCacheEntry(parentExecutionId);
    if (cacheEntry == null) {
      return Collections.emptyList();
    }
    return cacheEntry.getChildIds();
  }

  @Override
  public String findParentId(String childId) throws HopException {
    CacheEntry cacheEntry = findCacheEntry(childId);
    if (cacheEntry == null) {
      return null;
    }
    return cacheEntry.getId();
  }

  @Override
  public String getPluginId() {
    return "caching-file-location";
  }

  public void setPluginId(String pluginId) {
    // Don't set anything
  }

  @Override
  public String getPluginName() {
    return "Caching File location";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }
}
