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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ProjectsAccessControlTest {

  @AfterEach
  void reset() {
    HopSecurity.reset();
    ProjectsAccessConfig.clearCache();
  }

  @Test
  void unrestrictedAlwaysAllowsAll() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(false);
    assertNull(
        ProjectsAccessControl.resolveAllowedProjects(HopSecurityContext.unrestricted(), config));
  }

  @Test
  void disabledConfigAllowsAll() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(false);
    HopSecurityContext viewer = HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY));
    assertNull(ProjectsAccessControl.resolveAllowedProjects(viewer, config));
  }

  @Test
  void userRuleGrantsSpecificProjects() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(false);
    config.setRules(
        List.of(
            new ProjectsAccessRule(
                ProjectsAccessRule.TYPE_USER, "viewer", false, List.of("samples", "demo"))));

    HopSecurityContext viewer = HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY));
    Set<String> allowed = ProjectsAccessControl.resolveAllowedProjects(viewer, config);
    assertTrue(allowed != null && allowed.contains("samples"));
    assertTrue(allowed.contains("demo"));
    assertFalse(allowed.contains("secret"));
  }

  @Test
  void roleRuleGrantsAllProjects() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(false);
    config.setRules(
        List.of(new ProjectsAccessRule(ProjectsAccessRule.TYPE_ROLE, "user", true, List.of())));

    HopSecurityContext dev = HopSecurityContext.forUser("dev", Set.of(HopRole.USER));
    assertNull(ProjectsAccessControl.resolveAllowedProjects(dev, config));
  }

  @Test
  void groupRuleMatchesSourceRoles() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(false);
    config.setRules(
        List.of(
            new ProjectsAccessRule(
                ProjectsAccessRule.TYPE_GROUP, "ldap-finance", false, List.of("finance-dw"))));

    HopSecurityContext ctx =
        HopSecurityContext.forUser(
            "alice", Set.of(HopRole.USER), Set.of("ldap-finance", "ldap-other"));
    Set<String> allowed = ProjectsAccessControl.resolveAllowedProjects(ctx, config);
    assertTrue(allowed != null && allowed.contains("finance-dw"));
  }

  @Test
  void adminAlwaysAllowsAll() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(false);
    config.setRules(List.of());

    HopSecurityContext admin = HopSecurityContext.forUser("root", Set.of(HopRole.ADMIN));
    assertNull(ProjectsAccessControl.resolveAllowedProjects(admin, config));
  }

  @Test
  void noMatchUsesDefaultAllowAll() {
    ProjectsAccessConfig config = new ProjectsAccessConfig();
    config.setEnabled(true);
    config.setDefaultAllowAll(true);
    config.setRules(List.of());

    HopSecurityContext viewer = HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY));
    assertNull(ProjectsAccessControl.resolveAllowedProjects(viewer, config));

    config.setDefaultAllowAll(false);
    Set<String> denied = ProjectsAccessControl.resolveAllowedProjects(viewer, config);
    assertTrue(denied != null && denied.isEmpty());
  }
}
