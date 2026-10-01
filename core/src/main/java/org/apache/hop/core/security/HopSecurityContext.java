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
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;

/**
 * Resolved security identity for a session: username, roles, and the effective union of
 * permissions.
 *
 * <p>An <em>unrestricted</em> context (no authentication / desktop) allows every permission. A
 * restricted context only allows permissions granted by assigned roles.
 */
@Getter
public final class HopSecurityContext {

  public static final String ANONYMOUS_USERNAME = "anonymous";

  private final String username;
  private final Set<String> roleIds;

  /**
   * Original container / IdP / LDAP group names before mapping to Hop roles. Used for project
   * access rules keyed by group.
   */
  private final Set<String> sourceRoles;

  private final Set<Permission> permissions;
  private final boolean unrestricted;

  private HopSecurityContext(
      String username,
      Set<String> roleIds,
      Set<String> sourceRoles,
      Set<Permission> permissions,
      boolean unrestricted) {
    this.username = username;
    this.roleIds = Collections.unmodifiableSet(new LinkedHashSet<>(roleIds));
    this.sourceRoles =
        sourceRoles == null || sourceRoles.isEmpty()
            ? Set.of()
            : Collections.unmodifiableSet(new LinkedHashSet<>(sourceRoles));
    this.permissions =
        permissions.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(EnumSet.copyOf(permissions));
    this.unrestricted = unrestricted;
  }

  /**
   * Full-privilege context used when authentication is off (Hop Web mode NONE) or on the desktop
   * GUI.
   *
   * @return unrestricted context
   */
  public static HopSecurityContext unrestricted() {
    return new HopSecurityContext(
        ANONYMOUS_USERNAME, Set.of(), Set.of(), EnumSet.allOf(Permission.class), true);
  }

  /**
   * Build a context for an authenticated user from built-in roles. Permissions are the union of all
   * role permission sets.
   *
   * @param username authenticated username (must not be blank)
   * @param roles assigned built-in roles (at least one recommended)
   * @return restricted security context
   */
  public static HopSecurityContext forUser(String username, Set<HopRole> roles) {
    return forUser(username, roles, Set.of());
  }

  /**
   * Build a context for an authenticated user, retaining original container/IdP role names for
   * group-based authorization (e.g. project access by LDAP group).
   *
   * @param username authenticated username (must not be blank)
   * @param roles assigned built-in Hop roles
   * @param sourceRoles original container/IdP/LDAP group names (may be empty)
   * @return restricted security context
   */
  public static HopSecurityContext forUser(
      String username, Set<HopRole> roles, Set<String> sourceRoles) {
    Objects.requireNonNull(username, "username");
    if (username.isBlank()) {
      throw new IllegalArgumentException("username must not be blank");
    }
    Set<String> roleIds = new LinkedHashSet<>();
    EnumSet<Permission> effective = EnumSet.noneOf(Permission.class);
    if (roles != null) {
      for (HopRole role : roles) {
        if (role != null) {
          roleIds.add(role.getId());
          effective.addAll(role.getPermissions());
        }
      }
    }
    return new HopSecurityContext(username.trim(), roleIds, sourceRoles, effective, false);
  }

  /**
   * Build a context from permission ids directly (custom roles / tests).
   *
   * @param username username
   * @param roleIds role id labels for display
   * @param permissions effective permissions
   * @return restricted security context
   */
  public static HopSecurityContext forUserWithPermissions(
      String username, Set<String> roleIds, Set<Permission> permissions) {
    Objects.requireNonNull(username, "username");
    if (username.isBlank()) {
      throw new IllegalArgumentException("username must not be blank");
    }
    Set<String> safeRoles = roleIds != null ? roleIds : Set.of();
    Set<Permission> safePerms =
        permissions != null ? permissions : EnumSet.noneOf(Permission.class);
    return new HopSecurityContext(username.trim(), safeRoles, Set.of(), safePerms, false);
  }

  /**
   * Whether this context grants the given permission.
   *
   * @param permission permission to check
   * @return true if allowed
   */
  public boolean allows(Permission permission) {
    if (permission == null) {
      return false;
    }
    if (unrestricted) {
      return true;
    }
    return permissions.contains(permission);
  }

  /**
   * Whether this context grants every listed permission.
   *
   * @param required permissions that must all be present
   * @return true if all are allowed
   */
  public boolean allowsAll(Permission... required) {
    if (required == null || required.length == 0) {
      return true;
    }
    for (Permission permission : required) {
      if (!allows(permission)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether the user is considered authenticated (not the unrestricted anonymous default).
   *
   * @return true if a real user principal was bound
   */
  public boolean isAuthenticated() {
    return !unrestricted && username != null && !ANONYMOUS_USERNAME.equals(username);
  }

  @Override
  public String toString() {
    return "HopSecurityContext{username='"
        + username
        + "', roles="
        + roleIds
        + ", unrestricted="
        + unrestricted
        + ", permissions="
        + permissions.size()
        + '}';
  }
}
