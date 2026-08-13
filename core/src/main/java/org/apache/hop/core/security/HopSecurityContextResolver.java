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

import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resolves a {@link HopSecurityContext} from an authenticated username and container/IdP role
 * names. Lives in core so it has no servlet dependency; callers extract principal and roles from
 * the request.
 *
 * <p>Default alias mapping (EXTERNAL mode):
 *
 * <ul>
 *   <li>{@code hop-admin}, {@code admin} → {@link HopRole#ADMIN}
 *   <li>{@code hop-user}, {@code user}, {@code apachehop} → {@link HopRole#USER}
 *   <li>{@code hop-operator}, {@code operator} → {@link HopRole#OPERATOR}
 *   <li>{@code hop-readonly}, {@code readonly}, {@code viewer} → {@link HopRole#READ_ONLY}
 * </ul>
 *
 * <p>If the user is authenticated but no known role matches, {@link HopRole#USER} is assigned so
 * existing single-role Tomcat samples keep working with full developer access.
 */
public final class HopSecurityContextResolver {

  private HopSecurityContextResolver() {
    // utility
  }

  /**
   * Resolve security context from identity material using the active {@link HopSecurityConfig} role
   * mappings (if any) and built-in aliases.
   *
   * @param username principal name, or null/blank if unauthenticated
   * @param containerRoles role names from the container ({@code isUserInRole}), headers, or claims
   * @return unrestricted context when username is blank; otherwise a restricted context
   */
  public static HopSecurityContext resolve(String username, Collection<String> containerRoles) {
    return resolve(username, containerRoles, HopSecurityConfig.load());
  }

  /**
   * Resolve security context from identity material.
   *
   * @param username principal name, or null/blank if unauthenticated
   * @param containerRoles role names from the container ({@code isUserInRole}), headers, or claims
   * @param config security config for custom role mappings (null uses built-in aliases only)
   * @return unrestricted context when username is blank; otherwise a restricted context
   */
  public static HopSecurityContext resolve(
      String username, Collection<String> containerRoles, HopSecurityConfig config) {
    if (username == null || username.isBlank()) {
      return HopSecurityContext.unrestricted();
    }

    Set<HopRole> hopRoles = new LinkedHashSet<>();
    if (containerRoles != null) {
      for (String containerRole : containerRoles) {
        HopRole hopRole =
            config != null
                ? config.mapContainerRole(containerRole)
                : HopRole.fromIdOrAlias(containerRole);
        if (hopRole != null) {
          hopRoles.add(hopRole);
        }
      }
    }

    if (hopRoles.isEmpty()) {
      // Authenticated but no mappable role: default to USER (backward compatible with
      // single-role "apachehop" samples that only declare one custom role name).
      hopRoles.add(HopRole.USER);
    }

    Set<String> sourceRoles = new LinkedHashSet<>();
    if (containerRoles != null) {
      for (String containerRole : containerRoles) {
        if (containerRole != null && !containerRole.isBlank()) {
          sourceRoles.add(containerRole.trim());
        }
      }
    }

    return HopSecurityContext.forUser(username.trim(), hopRoles, sourceRoles);
  }

  /**
   * Probe a set of well-known container role names against a role checker (e.g. {@code
   * HttpServletRequest::isUserInRole}).
   *
   * @param roleChecker returns true if the current user has that container role
   * @return role names that matched
   */
  public static Set<String> collectKnownContainerRoles(RoleChecker roleChecker) {
    Set<String> found = new LinkedHashSet<>();
    if (roleChecker == null) {
      return found;
    }
    // Canonical Hop names + legacy apachehop sample role
    String[] candidates = {
      "hop-admin",
      "admin",
      "hop-user",
      "user",
      "apachehop",
      "hop-operator",
      "operator",
      "hop-readonly",
      "readonly",
      "read-only",
      "viewer"
    };
    for (String candidate : candidates) {
      try {
        if (roleChecker.isInRole(candidate)) {
          found.add(candidate);
        }
      } catch (Exception ignored) {
        // Some containers throw for unknown roles; skip
      }
    }
    return found;
  }

  /**
   * Convenience: resolve using a role checker for known role names only.
   *
   * @param username principal name
   * @param roleChecker container role checker
   * @return security context
   */
  public static HopSecurityContext resolve(String username, RoleChecker roleChecker) {
    return resolve(username, collectKnownContainerRoles(roleChecker));
  }

  /** Functional check against the servlet container (or a test double). */
  @FunctionalInterface
  public interface RoleChecker {
    boolean isInRole(String roleName);
  }

  /**
   * Effective permissions for a set of built-in roles (union). Exposed for tests and admin UIs.
   *
   * @param roles roles to union
   * @return permission set
   */
  public static Set<Permission> unionPermissions(Set<HopRole> roles) {
    EnumSet<Permission> effective = EnumSet.noneOf(Permission.class);
    if (roles != null) {
      for (HopRole role : roles) {
        if (role != null) {
          effective.addAll(role.getPermissions());
        }
      }
    }
    return effective;
  }
}
