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
import java.util.Date;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
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

  protected String actualRootFolder;

  public CachingFileExecutionInfoLocation() {
    super();
    this.actualRootFolder = null;
  }

  public CachingFileExecutionInfoLocation(CachingFileExecutionInfoLocation location) {
    super(location);
    this.rootFolder = location.rootFolder;
    this.actualRootFolder = location.actualRootFolder;
  }

  @Override
  public CachingFileExecutionInfoLocation clone() {
    return new CachingFileExecutionInfoLocation(this);
  }

  @Override
  public void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    // The actual root folder
    //
    actualRootFolder = variables.resolve(rootFolder);

    super.initialize(variables, metadataProvider);
  }

  @Override
  protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      // Before writing to disk, we calculate some summaries for convenience of other tools.
      cacheEntry.calculateSummary();
      cacheEntry.writeToDisk(actualRootFolder);
      // Remember when we last wrote to disk
      cacheEntry.setLastWritten(new Date());
    } catch (Exception e) {
      throw new HopException(
          "Error writing caching file entry to disk in folder " + actualRootFolder, e);
    }
  }

  @Override
  public void deleteCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      cacheEntry.deleteFromDisk(actualRootFolder);
    } catch (Exception e) {
      throw new HopException(
          "Error deleting caching file entry from folder " + actualRootFolder, e);
    }
  }

  protected synchronized CacheEntry loadCacheEntry(String executionId) throws HopException {
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
  protected void retrieveIds(boolean includeChildren, Set<DatedId> ids, int limit)
      throws HopException {
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
