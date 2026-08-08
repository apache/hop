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

package org.apache.hop.core.security;

/**
 * Fine-grained permissions used by Hop Web (and potentially other surfaces) for authorization.
 *
 * <p>These map to UI enablement (menus, toolbars, context actions) and, in later phases, to
 * server-side guards on save/execute/metadata mutations.
 */
public enum Permission {
  /** Open and view files, browse explorer, navigate history. */
  FILE_VIEW("file.view"),
  /** Create new files (pipelines, workflows, etc.). */
  FILE_CREATE("file.create"),
  /** Edit graph content (cut/paste/delete elements, undo-affecting changes). */
  FILE_EDIT("file.edit"),
  /** Save and save-as. */
  FILE_SAVE("file.save"),
  /** Delete files from the explorer or similar surfaces. */
  FILE_DELETE("file.delete"),
  /** Export to SVG and similar non-mutating exports. */
  FILE_EXPORT("file.export"),

  /** Start, preview, debug pipelines and workflows. */
  RUN_EXECUTE("run.execute"),
  /** Stop, pause, resume running pipelines and workflows. */
  RUN_STOP("run.stop"),

  /** Read metadata objects. */
  METADATA_READ("metadata.read"),
  /** Create, update, delete metadata objects. */
  METADATA_WRITE("metadata.write"),

  /** Create folders, rename, delete in the file explorer. */
  EXPLORER_WRITE("explorer.write"),

  /** Personal GUI preferences (theme, etc.). */
  CONFIG_GUI("config.gui"),
  /** System-wide Hop configuration. */
  CONFIG_SYSTEM("config.system"),
  /** Manage users, roles, and security settings. */
  SECURITY_MANAGE("security.manage"),

  /**
   * Install/uninstall marketplace plugins, edit marketplace repositories, and apply hop-env plugin
   * sets. Shared process install — Admin-only among built-in roles.
   */
  PLUGIN_MANAGE("plugin.manage");

  private final String id;

  Permission(String id) {
    this.id = id;
  }

  /**
   * Stable string id for config files and annotations (e.g. {@code file.save}).
   *
   * @return permission id
   */
  public String getId() {
    return id;
  }

  /**
   * Resolve a permission by its stable id (case-insensitive).
   *
   * @param id permission id such as {@code file.save}
   * @return the matching permission
   * @throws IllegalArgumentException if unknown
   */
  public static Permission fromId(String id) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("Permission id is empty");
    }
    for (Permission permission : values()) {
      if (permission.id.equalsIgnoreCase(id) || permission.name().equalsIgnoreCase(id)) {
        return permission;
      }
    }
    throw new IllegalArgumentException("Unknown permission id: " + id);
  }
}
