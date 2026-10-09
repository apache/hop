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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.ExecutionDeleter;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionSelector;
import org.apache.hop.execution.LastPeriod;
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
public class CachingFileExecutionInfoLocation extends BaseCachingExecutionInfoLocation
    implements IExecutionInfoLocation {
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
      id = "createParentFolder",
      order = "015",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      toolTip = "i18n::CachingFileExecutionInfoLocation.CreateParentFolder.Tooltip",
      label = "i18n::CachingFileExecutionInfoLocation.CreateParentFolder.Label")
  @HopMetadataProperty
  protected boolean createParentFolder;

  protected String actualRootFolder;

  public CachingFileExecutionInfoLocation() {
    super();
    this.actualRootFolder = null;
    this.createParentFolder = true;
  }

  public CachingFileExecutionInfoLocation(CachingFileExecutionInfoLocation location) {
    super(location);
    this.rootFolder = location.rootFolder;
    this.createParentFolder = location.createParentFolder;
    this.actualRootFolder = location.actualRootFolder;
  }

  @Override
  public CachingFileExecutionInfoLocation clone() {
    return new CachingFileExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    // Resolve root folder with the executor's variables (pipeline or workflow). Nested engines
    // (Workflow Executor, Pipeline Executor) must inherit parent variables so ${HOP_DATA} /
    // ${EXECUTIONS_INFORMATION_FOLDER} resolve the same way as the top-level run.
    actualRootFolder = variables != null ? variables.resolve(rootFolder) : rootFolder;

    if (StringUtils.isEmpty(actualRootFolder)) {
      throw new HopException(
          "Caching file execution information location has an empty root folder"
              + " (configured value was '"
              + Const.NVL(rootFolder, "")
              + "'). Set a path or a variable that resolves on this executor.");
    }
    if (actualRootFolder.contains("${") || actualRootFolder.contains("%%")) {
      throw new HopException(
          "Caching file execution information location root folder still contains unresolved"
              + " variables after resolution: '"
              + actualRootFolder
              + "' (configured '"
              + Const.NVL(rootFolder, "")
              + "'). Ensure variables like EXECUTIONS_INFORMATION_FOLDER / HOP_DATA are set on"
              + " the pipeline or workflow that opens this location (including nested executors).");
    }

    if (createParentFolder) {
      try {
        FileObject folder = HopVfs.getFileObject(actualRootFolder, variables);
        if (!folder.exists()) {
          folder.createFolder();
        }
      } catch (Exception e) {
        throw new HopException("Error creating root folder " + actualRootFolder, e);
      }
    }

    super.initialize(variables, metadataProvider);
    LogChannel.GENERAL.logBasic(
        "Caching file execution info location ready: rootFolder=" + actualRootFolder);
  }

  @Override
  protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // Multi-writer safe: driver and executors each hold a separate CacheEntry in memory.
      // Merge child maps from the on-disk file so samples/children written by other processes
      // are not wiped when this process flushes parent metrics/state.
      mergeChildrenFromDisk(cacheEntry);
      cacheEntry.prepareForPersist();
      // Before writing to disk, we calculate some summaries for convenience of other tools.
      cacheEntry.calculateSummary();
      cacheEntry.writeToDisk(actualRootFolder, variables);
      // Remember when we last wrote to disk
      cacheEntry.setLastWritten(new Date());
    } catch (Exception e) {
      throw new HopException(
          "Error writing caching file entry to disk in folder " + actualRootFolder, e);
    }
  }

  /**
   * Union child maps from the existing on-disk entry into {@code cacheEntry}. In-memory values win
   * on key conflicts ({@code putIfAbsent} from disk).
   */
  private void mergeChildrenFromDisk(CacheEntry cacheEntry) {
    if (cacheEntry == null || StringUtils.isEmpty(cacheEntry.getId())) {
      return;
    }
    try {
      CacheEntry onDisk = loadCacheEntry(cacheEntry.getId());
      if (onDisk == null) {
        return;
      }
      mergeMap(onDisk.getChildExecutions(), cacheEntry.getChildExecutions());
      mergeMap(onDisk.getChildExecutionStates(), cacheEntry.getChildExecutionStates());
      mergeMap(onDisk.getChildExecutionData(), cacheEntry.getChildExecutionData());
      cacheEntry.keepStoredProjectId(onDisk);
    } catch (Exception e) {
      // Best-effort: still write our in-memory view if merge fails
      LogChannel.GENERAL.logError(
          "Unable to merge on-disk cache entry before persist (non-fatal): " + e.getMessage());
    }
  }

  private static <K, V> void mergeMap(Map<K, V> fromDisk, Map<K, V> into) {
    if (fromDisk == null || fromDisk.isEmpty() || into == null) {
      return;
    }
    for (Map.Entry<K, V> e : fromDisk.entrySet()) {
      into.putIfAbsent(e.getKey(), e.getValue());
    }
  }

  @Override
  public void deleteCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      cacheEntry.deleteFromDisk(actualRootFolder, variables);
    } catch (Exception e) {
      throw new HopException(
          "Error deleting caching file entry from folder " + actualRootFolder, e);
    }
  }

  /**
   * Deletes the JSON files themselves. Listing through {@link #retrieveIds} parses every document
   * into the cache, and a location with a lot of sample rows then exhausts the Hop GUI heap.
   */
  @Override
  public synchronized int deleteExecutions(Date olderThan, IProgressMonitor monitor)
      throws HopException {
    int deleted = 0;
    int failed = 0;
    HopException firstFailure = null;
    FileObject[] files;
    try {
      files = getAllFileObjects(actualRootFolder);
    } catch (Exception e) {
      throw new HopException("Error listing executions to delete from " + actualRootFolder, e);
    }
    if (files == null) {
      return 0;
    }
    boolean needRead = olderThan != null || StringUtils.isNotEmpty(activeProjectId);
    for (FileObject file : files) {
      try {
        if (monitor != null && monitor.isCanceled()) {
          break;
        }
        String id = getIdFromFileName(file);
        if (StringUtils.isEmpty(id)) {
          continue;
        }
        if (needRead && !selectedForDelete(file, olderThan)) {
          continue;
        }
        if (monitor != null) {
          monitor.subTask("Deleting execution " + (deleted + 1) + ": " + id);
        }
        cache.remove(id);
        file.delete();
        deleted++;
      } catch (Exception e) {
        failed++;
        if (firstFailure == null) {
          firstFailure =
              new HopException("Error deleting execution file " + file.getName().getURI(), e);
        }
      } finally {
        closeQuietly(file);
      }
    }
    ExecutionDeleter.throwIfFailed(deleted, failed, firstFailure);
    return deleted;
  }

  /**
   * A file is deleted when it belongs to the active project and, if a cutoff is set, its execution
   * started before that cutoff or has no start date. The sample rows after the execution object are
   * not read.
   */
  private boolean selectedForDelete(FileObject file, Date olderThan) throws HopException {
    boolean needStart = olderThan != null;
    try (InputStream inputStream = HopVfs.getInputStream(file)) {
      DeleteMeta meta = readDeleteMeta(inputStream, needStart, activeProjectId);
      if (!matchesActiveProject(meta.projectId)) {
        return false;
      }
      if (!needStart) {
        return true;
      }
      return meta.start == null || meta.start.before(olderThan);
    } catch (Exception e) {
      throw new HopException(
          "Error reading execution '" + file.getName().getBaseName() + "' for delete", e);
    }
  }

  /** Project id and execution start. Sample data and logging are left unread. */
  static final class DeleteMeta {
    String projectId;
    Date start;
  }

  static DeleteMeta readDeleteMeta(
      InputStream inputStream, boolean needStart, String activeProjectId) throws IOException {
    DeleteMeta meta = new DeleteMeta();
    boolean inExecution = false;
    int depth = 0;
    try (JsonParser parser = HopJson.newFactory().createParser(inputStream)) {
      while (parser.nextToken() != null) {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
          depth++;
          continue;
        }
        if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
          // depth is still the object being closed. 2 is the execution object.
          if (inExecution && token == JsonToken.END_OBJECT && depth == 2) {
            return meta;
          }
          depth--;
          continue;
        }
        if (token != JsonToken.FIELD_NAME) {
          continue;
        }
        String field = parser.currentName();
        if (depth == 1 && "projectId".equals(field)) {
          parser.nextToken();
          meta.projectId = parser.getValueAsString();
          if (excludedProject(activeProjectId, meta.projectId)) {
            return meta;
          }
          continue;
        }
        if (depth == 1 && "execution".equals(field)) {
          // The top-level project id is enough to keep or skip the file. Do not open the
          // execution object: its XML and the sample rows after it are the large parts.
          if (!needStart && !excludedProject(activeProjectId, meta.projectId)) {
            if (StringUtils.isNotEmpty(meta.projectId) || StringUtils.isEmpty(activeProjectId)) {
              return meta;
            }
          }
          inExecution = true;
          continue;
        }
        if (inExecution && depth == 2 && "projectId".equals(field)) {
          parser.nextToken();
          if (StringUtils.isEmpty(meta.projectId)) {
            meta.projectId = parser.getValueAsString();
          } else {
            parser.getValueAsString();
          }
          if (excludedProject(activeProjectId, meta.projectId) || !needStart) {
            return meta;
          }
          continue;
        }
        if (inExecution && depth == 2 && "executionStartDate".equals(field)) {
          parser.nextToken();
          meta.start = readDate(parser);
          return meta;
        }
        // A wipe does not need the start date. Stop before the pipeline XML.
        if (!needStart && inExecution && depth == 2 && isHeavyExecutionField(field)) {
          return meta;
        }
        if (parser.nextToken() == null) {
          break;
        }
        parser.skipChildren();
      }
    }
    return meta;
  }

  private static boolean isHeavyExecutionField(String field) {
    return "executorXml".equals(field) || "metadataJson".equals(field);
  }

  private static boolean excludedProject(String activeProjectId, String storedProjectId) {
    return StringUtils.isNotEmpty(activeProjectId)
        && StringUtils.isNotEmpty(storedProjectId)
        && !activeProjectId.equals(storedProjectId);
  }

  private static Date readDate(JsonParser parser) throws IOException {
    if (parser.currentToken() == null || parser.currentToken() == JsonToken.VALUE_NULL) {
      return null;
    }
    if (parser.currentToken().isNumeric()) {
      return new Date(parser.getLongValue());
    }
    String text = parser.getValueAsString();
    if (StringUtils.isEmpty(text)) {
      return null;
    }
    try {
      return new Date(Long.parseLong(text));
    } catch (NumberFormatException e) {
      return Date.from(Instant.parse(text));
    }
  }

  private static void closeQuietly(FileObject file) {
    if (file == null) {
      return;
    }
    try {
      file.close();
    } catch (Exception ignored) {
      // Releasing the content cache must not hide the delete error.
    }
  }

  protected synchronized CacheEntry loadCacheEntry(String executionId) throws HopException {
    try {
      CacheEntry entry = new CacheEntry();
      entry.setId(executionId);
      String filename = entry.calculateFilename(actualRootFolder);
      if (!HopVfs.fileExists(filename, variables)) {
        return null;
      }
      ObjectMapper objectMapper = new ObjectMapper(HopJson.newFactory());
      return objectMapper.readValue(HopVfs.getInputStream(filename, variables), CacheEntry.class);
    } catch (Exception e) {
      throw new HopException(
          "Error loading execution information location file for executionId '" + executionId + "'",
          e);
    }
  }

  @Override
  protected void retrieveIds(
      boolean includeChildren, Set<DatedId> ids, int limit, final IExecutionSelector selector)
      throws HopException {
    final IExecutionSelector activeSelector = selector == null ? IExecutionSelector.ALL : selector;
    try {
      FileObject[] files = getAllFileObjects(actualRootFolder);

      for (FileObject file : files) {
        try {
          String id = getIdFromFileName(file);

          // We do a first filtering on the modification date
          // This is probably the execution end date, so we add an hour.
          //
          LastPeriod dateFilter = activeSelector.startDateFilter();
          if (dateFilter != null) {
            LocalDateTime roughStartDate = dateFilter.calculateStartDate().minusHours(1);
            long startDate =
                ZonedDateTime.of(roughStartDate, ZoneId.systemDefault()).toInstant().toEpochMilli();
            long lastModified = file.getContent().getLastModifiedTime();
            // Some VFS providers (e.g. Databricks Files API before list-cache attach) report
            // lastModified as 0 / unknown. Do not pre-skip those — load and apply content filters.
            if (lastModified > 0 && lastModified < startDate) {
              // Skip for performance
              continue;
            }
          }

          // Apply the other filters by loading the file
          //
          CacheEntry entry = findCacheEntry(id);
          if (entry == null) {
            // Not much loaded from disk or cache
            continue;
          }
          if (!matchesActiveProject(entry)) {
            continue;
          }
          if (!activeSelector.isSelected(entry.getExecution())) {
            continue;
          }
          if (!activeSelector.isSelected(entry.getExecutionState())) {
            continue;
          }

          if (!ids.contains(new DatedId(id, null))) {
            long lastModified = file.getContent().getLastModifiedTime();
            Date dated;
            if (lastModified > 0) {
              dated = new Date(lastModified);
            } else if (entry.getExecution() != null
                && entry.getExecution().getExecutionStartDate() != null) {
              // VFS providers without mtime: use execution start for sorting
              dated = entry.getExecution().getExecutionStartDate();
            } else {
              dated = new Date(0L);
            }
            ids.add(new DatedId(id, dated));

            // To add child IDs we need to load the file.
            // We won't store these in the cache though.
            //
            if (!activeSelector.isSelectingParents()) {
              entry = loadCacheEntry(id);
              if (entry != null) {
                addChildIds(entry, ids);
              }
            }
          }
        } finally {
          // findFiles keeps every file open. Closing releases the document bytes after the id is
          // known, so a large location is not held in the heap all at once.
          closeQuietly(file);
        }
      }
    } catch (Exception e) {
      throw new HopException("Error finding execution ids in folder '" + actualRootFolder + "'", e);
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
    FileObject folder = HopVfs.getFileObject(actualRootFolder, variables);
    return folder.findFiles(
        new AllFileSelector() {
          @Override
          public boolean includeFile(FileSelectInfo fileInfo) {
            return fileInfo.getFile().getName().getExtension().equalsIgnoreCase("json");
          }
        });
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
