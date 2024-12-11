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

package org.apache.hop.metadata.serializer;

import java.util.ArrayList;
import java.util.List;

// Represents a node in the file system tree
public class FileSystemNode {
  private String name; // Name of the file or folder
  private String path; // Absolute path to the file or folder
  private Type type; // Type (file or folder)
  private FileSystemNode parent; // Parent node
  private List<FileSystemNode> children; // Children nodes (only for folder)

  // Enum for node type
  public enum Type {
    FILE,
    FOLDER
  }

  // Constructor

  public FileSystemNode(String name, String path, Type type, FileSystemNode parent) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.parent = parent;
    this.children = new ArrayList<>();

    if (this.parent != null) {
      this.parent.addChild(this);
    }
  }

  // Add a child node

  public void addChild(FileSystemNode child) {
    if (this.type == Type.FOLDER) {
      this.children.add(child);
    }
  }

  // Getters

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public Type getType() {
    return type;
  }

  public FileSystemNode getParent() {
    return parent;
  }

  public List<FileSystemNode> getChildren() {
    return children;
  }

  public boolean isFolder() {
    return type == Type.FOLDER;
  }

  public boolean isFile() {
    return type == Type.FILE;
  }
}
