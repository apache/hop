/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import org.apache.commons.vfs2.impl.DefaultFileSystemManager;

/** Test configuration for a file system. */
public interface ProviderTestConfig {

  /**
   * Returns the base folder for tests. This folder must exist, and contain the following structure:
   *
   * <ul>
   *   <li>"/read-tests"
   *   <li>"/write-tests"
   * </ul>
   */
  FileObject getBaseTestFolder(FileSystemManager manager) throws Exception;

  /** Returns a DefaultFileSystemManager instance (or subclass instance). */
  DefaultFileSystemManager getDefaultFileSystemManager();

  /** Returns the filesCache implementation used for tests. */
  FilesCache getFilesCache();

  /**
   * Tests whether or not the root of test file system is accessible.
   *
   * <p>For example, with the default Jackrabbit (WebDAV) server, the root is not accessible, but
   * deeper paths are OK.
   *
   * @return Whether or not the root of test file system is accessible.
   */
  boolean isFileSystemRootAccessible();

  /** Prepares the file system manager. */
  void prepare(DefaultFileSystemManager manager) throws Exception;
}
