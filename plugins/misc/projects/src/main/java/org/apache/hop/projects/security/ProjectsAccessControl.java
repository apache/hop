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

package org.apache.hop.projects.security;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.Permission;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;

/**
 * Evaluates whether the current security context may open a given project. Unrestricted sessions
 * and security admins always pass. When access control is disabled, everyone passes.
 */
public final class ProjectsAccessControl {

  private ProjectsAccessControl() {}

  /**
   * Whether project access filtering is active for this session (enabled in config and session is
   * authenticated / restricted).
   */
  public static boolean isEnforced() {
    ProjectsAccessConfig config = ProjectsAccessConfig.load();
    if (!config.isEnabled()) {
      return false;
    }
    HopSecurityContext ctx = HopSecurity.getContext();
    return ctx != null && !ctx.isUnrestricted();
  }

  /**
   * Whether the current user may open the named project.
   *
   * @param projectName project name
   * @return true if allowed
   */
  public static boolean isProjectAllowed(String projectName) {
    if (StringUtils.isEmpty(projectName) || !isEnforced()) {
      return true;
    }
    HopSecurityContext ctx = HopSecurity.getContext();
    if (ctx == null || ctx.isUnrestricted()) {
      return true;
    }
    // Security managers always see every project
    if (ctx.allows(Permission.SECURITY_MANAGE)) {
      return true;
    }

    ProjectsAccessConfig config = ProjectsAccessConfig.load();
    Set<String> allowed = resolveAllowedProjects(ctx, config);
    if (allowed == null) {
      // null means "all projects"
      return true;
    }
    return collectionContainsIgnoreCase(allowed, projectName);
  }

  /**
   * Filter a list of project names to those the current user may open.
   *
   * @param projectNames candidate names
   * @return filtered list (new list)
   */
  public static List<String> filterAllowedProjectNames(List<String> projectNames) {
    if (projectNames == null || projectNames.isEmpty() || !isEnforced()) {
      return projectNames == null ? List.of() : new ArrayList<>(projectNames);
    }
    List<String> result = new ArrayList<>();
    for (String name : projectNames) {
      if (isProjectAllowed(name)) {
        result.add(name);
      }
    }
    return result;
  }

  /**
   * Whether the current user may open at least one registered project (or all, when unrestricted).
   */
  public static boolean hasAnyAllowedProject() {
    if (!isEnforced()) {
      return true;
    }
    ProjectsConfig projectsConfig = ProjectsConfigSingleton.getConfig();
    List<String> names = projectsConfig.listProjectConfigNames();
    if (names.isEmpty()) {
      return true;
    }
    return !filterAllowedProjectNames(names).isEmpty();
  }

  /**
   * Resolve the set of allowed project names for a context.
   *
   * @return {@code null} when all projects are allowed; otherwise a (possibly empty) set of names
   */
  static Set<String> resolveAllowedProjects(HopSecurityContext ctx, ProjectsAccessConfig config) {
    if (ctx == null || ctx.isUnrestricted() || ctx.allows(Permission.SECURITY_MANAGE)) {
      return null;
    }
    if (config == null || !config.isEnabled()) {
      return null;
    }

    boolean anyMatch = false;
    boolean grantAll = false;
    Set<String> granted = new LinkedHashSet<>();

    List<ProjectsAccessRule> rules = config.getRules();
    if (rules != null) {
      for (ProjectsAccessRule rule : rules) {
        if (rule == null || !matches(ctx, rule)) {
          continue;
        }
        anyMatch = true;
        if (rule.isAllProjects()) {
          grantAll = true;
          break;
        }
        if (rule.getProjects() != null) {
          for (String p : rule.getProjects()) {
            if (StringUtils.isNotEmpty(p)) {
              granted.add(p.trim());
            }
          }
        }
      }
    }

    if (grantAll) {
      return null;
    }
    if (anyMatch) {
      return granted;
    }
    // No matching rule
    return config.isDefaultAllowAll() ? null : Set.of();
  }

  private static boolean matches(HopSecurityContext ctx, ProjectsAccessRule rule) {
    String type = rule.normalizedType();
    String subject = rule.normalizedSubject();
    if (subject.isEmpty()) {
      return false;
    }
    return switch (type) {
      case ProjectsAccessRule.TYPE_USER -> equalsIgnoreCase(ctx.getUsername(), subject);
      case ProjectsAccessRule.TYPE_ROLE -> collectionContainsIgnoreCase(ctx.getRoleIds(), subject);
      case ProjectsAccessRule.TYPE_GROUP ->
          collectionContainsIgnoreCase(ctx.getSourceRoles(), subject)
              || collectionContainsIgnoreCase(ctx.getRoleIds(), subject);
      default -> false;
    };
  }

  private static boolean equalsIgnoreCase(String a, String b) {
    if (a == null || b == null) {
      return false;
    }
    return a.trim().equalsIgnoreCase(b.trim());
  }

  private static boolean collectionContainsIgnoreCase(Collection<String> set, String value) {
    if (set == null || set.isEmpty() || value == null) {
      return false;
    }
    String needle = value.trim().toLowerCase(Locale.ROOT);
    for (String s : set) {
      if (s != null && s.trim().toLowerCase(Locale.ROOT).equals(needle)) {
        return true;
      }
    }
    return false;
  }
}
