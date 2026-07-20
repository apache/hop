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

/**
 * One entry from a Databricks Files API directory listing ({@code GET
 * /api/2.0/fs/directories{path}}).
 */
public record DirectoryEntry(
    String name, String path, boolean directory, long sizeBytes, long lastModifiedEpochMs) {

  public static DirectoryEntry ofFile(String name, String path, long sizeBytes, long lastModified) {
    return new DirectoryEntry(name, path, false, Math.max(0L, sizeBytes), lastModified);
  }

  public static DirectoryEntry ofDirectory(String name, String path, long lastModified) {
    return new DirectoryEntry(name, path, true, -1L, lastModified);
  }
}
