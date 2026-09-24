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

package org.apache.hop.metadata.serializer;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * The serializer calls the Hop GUI makes to rename and duplicate metadata objects, without the GUI
 * around them. Keep these in sync with the classes they mirror so the tests using them exercise
 * what users actually do.
 */
public final class MetadataGuiFlows {

  private MetadataGuiFlows() {}

  /**
   * Renaming an element in the metadata perspective tree: {@code MetadataManager.rename()}.
   *
   * @return false if the new name is already taken, like the GUI which refuses the rename
   */
  public static <T extends IHopMetadata> boolean renameInTree(
      IHopMetadataSerializer<T> serializer, String oldName, String newName) throws HopException {
    if (serializer.exists(newName)) {
      return false;
    }
    T metadata = serializer.load(oldName);
    metadata.setName(newName);
    serializer.save(metadata);
    serializer.delete(oldName);
    return true;
  }

  /**
   * Changing the name of an open element in its editor and saving it: {@code
   * MetadataEditor.save()}.
   *
   * @throws HopException if the new name is already taken, like the GUI
   */
  public static <T extends IHopMetadata> void renameInEditor(
      IHopMetadataSerializer<T> serializer, T metadata, String originalName) throws HopException {
    boolean isRename = false;
    if (!originalName.equals(metadata.getName())) {
      if (serializer.exists(metadata.getName())) {
        throw new HopException("Name '" + metadata.getName() + "' already exists");
      }
      isRename = true;
    }
    serializer.save(metadata);
    if (isRename && serializer.exists(originalName)) {
      serializer.delete(originalName);
    }
  }

  /**
   * Duplicating an element in the metadata perspective: {@code
   * MetadataPerspective.duplicateMetadata()}.
   *
   * @return the name of the copy
   */
  public static <T extends IHopMetadata> String duplicate(
      IHopMetadataSerializer<T> serializer, String name) throws HopException {
    T metadata = serializer.load(name);
    int copyNr = 2;
    while (true) {
      String newName = name + " " + copyNr;
      if (!serializer.exists(newName)) {
        metadata.setName(newName);
        serializer.save(metadata);
        return newName;
      }
      copyNr++;
    }
  }
}
