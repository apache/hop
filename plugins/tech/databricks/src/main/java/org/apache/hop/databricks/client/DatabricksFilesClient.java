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

package org.apache.hop.databricks.client;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.hop.core.exception.HopException;

/**
 * Databricks workspace Files API surface for UC Volumes and Workspace files ({@code /Volumes/…},
 * {@code /Workspace/…}). Not the legacy DBFS block API.
 */
public interface DatabricksFilesClient extends AutoCloseable {

  /** Upload a local file (overwrite). Path must be absolute Files API path. */
  void upload(Path localFile, String workspacePath) throws HopException;

  /** Upload UTF-8 text (overwrite). Convenience for small sidecars. */
  void uploadText(String workspacePath, String text) throws HopException;

  /**
   * Metadata for a remote file. {@link WorkspaceFileMetadata#exists()} is false when missing or not
   * a file.
   */
  WorkspaceFileMetadata getFileMetadata(String workspacePath) throws HopException;

  /** Download a small remote text file; empty when missing. */
  Optional<String> downloadTextIfExists(String workspacePath) throws HopException;

  /**
   * Open a binary download stream for the file. Caller must close the stream. Throws if the path
   * does not exist.
   */
  InputStream openInputStream(String workspacePath) throws HopException;

  /** List directory contents (all pages). Path must be an existing directory. */
  List<DirectoryEntry> listDirectory(String workspacePath) throws HopException;

  /** Create a directory (parents must exist per API rules for the volume). */
  void createDirectory(String workspacePath) throws HopException;

  /** Delete a file. */
  void deleteFile(String workspacePath) throws HopException;

  /** Delete a directory; when {@code recursive}, delete contents first as required by the API. */
  void deleteDirectory(String workspacePath, boolean recursive) throws HopException;

  @Override
  void close();
}
