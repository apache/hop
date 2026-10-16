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

import java.util.HashMap;
import java.util.Map;

/** Info about a file. */
public class FileInfo {

  String baseName;
  FileType type;
  String content;
  Map<String, FileInfo> children = new HashMap<>();
  FileInfo parent;

  public FileInfo(final String name, final FileType type) {
    baseName = name;
    this.type = type;
    content = null;
  }

  public FileInfo(final String name, final FileType type, final String content) {
    baseName = name;
    this.type = type;
    this.content = content;
  }

  /** Adds a child. */
  public void addChild(final FileInfo child) {
    children.put(child.baseName, child);
    child.parent = this;
  }

  /** Adds a child file. */
  public FileInfo addFile(final String baseName, final String content) {
    final FileInfo child = new FileInfo(baseName, FileType.FILE, content);
    addChild(child);
    return child;
  }

  /** Adds a child folder. */
  public FileInfo addFolder(final String baseName) {
    final FileInfo child = new FileInfo(baseName, FileType.FOLDER, null);
    addChild(child);
    return child;
  }

  /**
   * Returns the base name for the file.
   *
   * @return The base name
   */
  public String getBaseName() {
    return baseName;
  }

  /**
   * Returns a {@code Map} of this {@code FileInfo}'s children.
   *
   * @return The {@code FileInfo}'s children
   */
  public Map<String, FileInfo> getChildren() {
    return children;
  }

  /**
   * Returns file's content.
   *
   * @return The content as a {@code String}
   */
  public String getContent() {
    return content;
  }

  public FileInfo getParent() {
    return parent;
  }

  /**
   * Returns the {@link FileType} of the file.
   *
   * @return {@link FileType}
   */
  public FileType getType() {
    return type;
  }
}
