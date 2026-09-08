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

package org.apache.hop.testing.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;

/**
 * Tracks in-memory unit-test transform renames that have not been saved yet, so Ctrl+Z can restore
 * the previous location names and a pipeline save can persist them.
 */
public final class UnitTestTransformRenames {

  private UnitTestTransformRenames() {
    // utility
  }

  public static final class Rename {
    private final String oldName;
    private final String newName;

    public Rename(String oldName, String newName) {
      this.oldName = oldName;
      this.newName = newName;
    }

    public String getOldName() {
      return oldName;
    }

    public String getNewName() {
      return newName;
    }
  }

  public static List<Rename> pending(Map<String, Object> stateMap) {
    List<Rename> pending = mutablePending(stateMap);
    return pending != null ? pending : Collections.emptyList();
  }

  public static void record(Map<String, Object> stateMap, String oldName, String newName) {
    if (stateMap == null) {
      return;
    }
    List<Rename> list = mutablePending(stateMap);
    if (list == null) {
      list = new ArrayList<>();
      stateMap.put(DataSetConst.STATE_KEY_PENDING_TRANSFORM_RENAMES, list);
    }
    list.add(new Rename(oldName, newName));
  }

  public static void clear(Map<String, Object> stateMap) {
    if (stateMap != null) {
      stateMap.remove(DataSetConst.STATE_KEY_PENDING_TRANSFORM_RENAMES);
    }
  }

  public static boolean hasPending(Map<String, Object> stateMap) {
    List<Rename> pending = mutablePending(stateMap);
    return pending != null && !pending.isEmpty();
  }

  @SuppressWarnings("unchecked")
  private static List<Rename> mutablePending(Map<String, Object> stateMap) {
    if (stateMap == null) {
      return null;
    }
    Object value = stateMap.get(DataSetConst.STATE_KEY_PENDING_TRANSFORM_RENAMES);
    if (value instanceof List<?> list) {
      return (List<Rename>) list;
    }
    return null;
  }

  /**
   * If undo restored the old transform name, roll the unit test locations back. Walks newest-first
   * so chained renames unwind in order.
   *
   * @return true when at least one location was reverted
   */
  public static boolean revertIfUndoRestoredOldNames(
      PipelineMeta pipelineMeta, PipelineUnitTest unitTest, Map<String, Object> stateMap) {
    if (pipelineMeta == null || unitTest == null || stateMap == null) {
      return false;
    }
    List<Rename> pending = pending(stateMap);
    if (pending.isEmpty()) {
      return false;
    }
    boolean changed = false;
    for (int i = pending.size() - 1; i >= 0; i--) {
      Rename rename = pending.get(i);
      if (pipelineMeta.findTransform(rename.getNewName()) == null
          && pipelineMeta.findTransform(rename.getOldName()) != null) {
        if (unitTest.renameTransform(rename.getNewName(), rename.getOldName())) {
          changed = true;
        }
        pending.remove(i);
      }
    }
    return changed;
  }

  /**
   * Undo every pending rename on the unit test so a cached metadata object matches what is on disk.
   *
   * @return true when at least one location was reverted
   */
  public static boolean revertAll(PipelineUnitTest unitTest, Map<String, Object> stateMap) {
    if (unitTest == null || stateMap == null) {
      return false;
    }
    List<Rename> pending = pending(stateMap);
    if (pending.isEmpty()) {
      return false;
    }
    boolean changed = false;
    for (int i = pending.size() - 1; i >= 0; i--) {
      Rename rename = pending.get(i);
      if (unitTest.renameTransform(rename.getNewName(), rename.getOldName())) {
        changed = true;
      }
    }
    pending.clear();
    return changed;
  }
}
