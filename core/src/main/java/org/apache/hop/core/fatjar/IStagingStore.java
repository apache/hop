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

package org.apache.hop.core.fatjar;

import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.exception.HopException;

/**
 * Where {@link ContentStaging} puts files for a cluster: a Hop VFS folder, a Databricks volume, ...
 * Names are flat file names or relative paths under the store's folder.
 *
 * <p>A store that can also list, delete and write small text files supports {@link StagingCleanup}:
 * each run marks the files it uses, and files nobody used for a while are removed.
 */
public interface IStagingStore {

  /**
   * @return the size of the staged file, or -1 when it is not there
   */
  long size(String name) throws HopException;

  void upload(Path localFile, String name) throws HopException;

  /** Where the file is stored, for the log. */
  String location(String name);

  /** How the cluster reads the file. */
  String clusterUrl(String name);

  /** A file directly in the staging folder. */
  record StagedFile(String name, long size, long lastModified) {}

  /**
   * @return true when this store can list, delete, read and write: what marking files as used and
   *     removing unused ones needs
   */
  default boolean supportsCleanup() {
    return false;
  }

  /**
   * @return the files directly in the staging folder, empty when the folder does not exist
   */
  default List<StagedFile> list() throws HopException {
    throw new HopException("Listing is not supported by this staging store");
  }

  default void delete(String name) throws HopException {
    throw new HopException("Deleting is not supported by this staging store");
  }

  default void writeText(String name, String text) throws HopException {
    throw new HopException("Writing text is not supported by this staging store");
  }

  /**
   * @return the text of a small file, or null when it is not there
   */
  default String readText(String name) throws HopException {
    throw new HopException("Reading text is not supported by this staging store");
  }
}
