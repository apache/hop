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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionState;

@Setter
@Getter
public class CacheEntry {
  // The log channel ID of the pipeline or workflow
  private String id;

  // The internal ID of the storage where this data resides at rest.
  @JsonIgnore private String internalId;

  // The name of the pipeline of workflow
  private String name;

  // The parent execution: pipeline or workflow
  private Execution execution;

  // The parent execution: pipeline or workflow
  private ExecutionState executionState;

  // All the child transform/action executions
  @JsonSerialize private Map<String, Execution> childExecutions;

  @JsonSerialize private Map<String, ExecutionState> childExecutionStates;

  @JsonSerialize private Map<String, ExecutionData> childExecutionData;

  private EntrySummary summary;

  // When was the cache entry last written to file?
  @JsonIgnore private Date lastWritten;
  // When was this cache entry last read?
  @JsonIgnore private Date lastRead;

  // Was content modified and not yet written to disk?
  @JsonIgnore private boolean dirty;

  public CacheEntry() {
    childExecutions = new HashMap<>();
    childExecutionStates = new HashMap<>();
    childExecutionData = new HashMap<>();
    summary = new EntrySummary();
    lastWritten = new Date();
    dirty = true;
  }

  /**
   * Write this cache entry to a file in a folder
   *
   * @param rootFolder The folder to write the file to.
   * @throws HopException In case there was an error writing.
   */
  public void writeToDisk(String rootFolder) throws HopException {
    String targetFilename = calculateFilename(rootFolder);
    String filename = calculateFilename(rootFolder) + ".new";
    try (FileOutputStream fos = new FileOutputStream(filename)) {
      // Serialize this object to JSON in a file
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.writeValue(fos, this);
    } catch (Exception e) {
      throw new HopException(
          "Error writing cache entry to file '" + calculateFilename(rootFolder) + "'", e);
    }
    // Now delete the old file and rename the new one.
    //
    try {
      FileObject targetFileObject = HopVfs.getFileObject(targetFilename);
      targetFileObject.delete();
      FileObject fileObject = HopVfs.getFileObject(filename);
      fileObject.moveTo(targetFileObject);
    } catch (Exception e) {
      throw new HopException(
          "Error renaming execution information to file '" + targetFilename + "'", e);
    }
    // All went fine so we'll clear the dirty flag and keep track of when we wrote the file.
    this.dirty = false;
    this.lastWritten = new Date();
    this.lastRead = null;
  }

  public void deleteFromDisk(String rootFolder) throws HopException {
    String targetFilename = calculateFilename(rootFolder);
    try {
      FileObject fileObject = HopVfs.getFileObject(targetFilename);
      fileObject.delete();
    } catch (Exception e) {
      throw new HopException(
          "Error deleting execution information file '" + targetFilename + "'", e);
    }
  }

  /**
   * We store the execution information in a folder. We'll have one file per pipeline or workflow.
   * We include the name of the pipeline or workflow in the filename to make it easier to see what's
   * in the files.
   *
   * @param rootFolder the root folder to store the
   */
  public String calculateFilename(String rootFolder) {
    return rootFolder + Const.FILE_SEPARATOR + id + ".json";
  }

  public void addChildExecution(Execution childExecution) {
    childExecutions.put(childExecution.getId(), childExecution);
    flagDirty();
  }

  public void addExecutionData(ExecutionData executionData) {
    childExecutionData.put(executionData.getOwnerId(), executionData);
    flagDirty();
  }

  public ExecutionData getExecutionData(String ownerId) {
    flagRead();
    return childExecutionData.get(ownerId);
  }

  private void flagDirty() {
    if (!dirty) {
      dirty = true;
      lastWritten = new Date();
    }
  }

  private void flagRead() {
    lastRead = new Date();
  }

  public Execution getChildExecution(String id) {
    flagRead();
    return childExecutions.get(id);
  }

  public void addChildExecutionState(ExecutionState executionState) {
    String executionId = executionState.getId();
    if (StringUtils.isEmpty(executionId)) {
      executionId = executionState.getName() + "." + executionState.getCopyNr();
    }
    childExecutionStates.put(executionId, executionState);
    flagDirty();
  }

  public ExecutionState getChildExecutionState(String id) {
    flagRead();
    return childExecutionStates.get(id);
  }

  /**
   * If this entry is dirty and old enough return true. Otherwise, return false as it's not time to
   * write this entry.
   *
   * @param persistenceDelay The maximum time we delay writing the entry to disk.
   * @return True if this entry needs writing to disk.
   */
  public boolean needsWriting(int persistenceDelay) {
    if (!dirty) {
      return false;
    }
    // Write at least once
    //
    if (lastWritten == null) {
      return true;
    }
    return System.currentTimeMillis() - lastWritten.getTime() > persistenceDelay;
  }

  /**
   * This entry is too old if the last time it was written or read exceeds the maximum age.
   *
   * @param maxAge The maximum age of this entry in ms
   * @return true if this entry is too old.
   */
  public boolean isTooOld(int maxAge) {
    if (lastRead != null && System.currentTimeMillis() - lastRead.getTime() > maxAge) {
      return true;
    }
    return lastWritten != null && System.currentTimeMillis() - lastWritten.getTime() > maxAge;
  }

  public List<String> getChildIds() {
    return new ArrayList<>(childExecutions.keySet());
  }

  public void calculateSummary() {
    summary.calculateSummary(this);
  }
}
