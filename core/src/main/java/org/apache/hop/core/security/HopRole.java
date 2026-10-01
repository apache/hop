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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Built-in Hop Web roles with their default permission sets.
 *
 * <p>Custom roles may be added later; these four are always present and non-deletable.
 */
public enum HopRole {
  /** Full power: configure Hop Web, manage users/roles, and all file/run/metadata operations. */
  ADMIN("admin", EnumSet.allOf(Permission.class)),

  /**
   * Day-to-day developer: CRUD files and metadata, execute, personal GUI config. No system security
   * administration.
   */
  USER(
      "user",
      EnumSet.of(
          Permission.FILE_VIEW,
          Permission.FILE_CREATE,
          Permission.FILE_EDIT,
          Permission.FILE_SAVE,
          Permission.FILE_DELETE,
          Permission.FILE_EXPORT,
          Permission.RUN_EXECUTE,
          Permission.RUN_STOP,
          Permission.METADATA_READ,
          Permission.METADATA_WRITE,
          Permission.EXPLORER_WRITE,
          Permission.CONFIG_GUI)),

  /** Operator: view and execute only. No save, edit, delete, or metadata write. */
  OPERATOR(
      "operator",
      EnumSet.of(
          Permission.FILE_VIEW,
          Permission.FILE_EXPORT,
          Permission.RUN_EXECUTE,
          Permission.RUN_STOP,
          Permission.METADATA_READ,
          Permission.CONFIG_GUI)),

  /** Read-only: inspect files and metadata. No execute and no mutations. */
  READ_ONLY(
      "readonly",
      EnumSet.of(Permission.FILE_VIEW, Permission.METADATA_READ, Permission.CONFIG_GUI));

  private final String id;
  private final Set<Permission> permissions;

  HopRole(String id, Set<Permission> permissions) {
    this.id = id;
    this.permissions = Collections.unmodifiableSet(permissions);
  }

  /**
   * Stable role id used in config and container role mapping (e.g. {@code admin}).
   *
   * @return role id
   */
  public String getId() {
    return id;
  }

  /**
   * Permissions granted by this role.
   *
   * @return unmodifiable set of permissions
   */
  public Set<Permission> getPermissions() {
    return permissions;
  }

  /**
   * Resolve a built-in role by id (case-insensitive). Accepts common aliases used in Tomcat samples
   * and reverse proxies.
   *
   * @param id role name such as {@code hop-admin}, {@code admin}, {@code apachehop}
   * @return matching role, or {@code null} if not a known built-in / alias
   */
  public static HopRole fromIdOrAlias(String id) {
    if (id == null || id.isBlank()) {
      return null;
    }
    String normalized = id.trim().toLowerCase(Locale.ROOT);
    // Strip optional hop- prefix used in container role names
    if (normalized.startsWith("hop-")) {
      normalized = normalized.substring(4);
    }
    return switch (normalized) {
      case "admin", "administrator" -> ADMIN;
      case "user", "developer", "apachehop" -> USER;
      case "operator", "ops" -> OPERATOR;
      case "readonly", "read-only", "read_only", "viewer" -> READ_ONLY;
      default -> {
        for (HopRole role : values()) {
          if (role.id.equals(normalized) || role.name().equalsIgnoreCase(id.trim())) {
            yield role;
          }
        }
        yield null;
      }
    };
  }
}
