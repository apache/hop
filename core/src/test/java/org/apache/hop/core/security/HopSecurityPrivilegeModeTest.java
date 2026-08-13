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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HopSecurityPrivilegeModeTest {

  @Test
  void adminCanAssumeOperatorAndReadOnly() {
    HopSecurityContext admin = HopSecurityContext.forUser("a", Set.of(HopRole.ADMIN));
    assertTrue(HopSecurityPrivilegeMode.canAssume(admin, HopRole.OPERATOR));
    assertTrue(HopSecurityPrivilegeMode.canAssume(admin, HopRole.READ_ONLY));
    assertTrue(HopSecurityPrivilegeMode.canAssume(admin, HopRole.USER));
    assertTrue(HopSecurityPrivilegeMode.canAssume(admin, HopRole.ADMIN));
  }

  @Test
  void userCanAssumeReadOnlyAndOperatorButNotAdmin() {
    HopSecurityContext user = HopSecurityContext.forUser("u", Set.of(HopRole.USER));
    assertTrue(HopSecurityPrivilegeMode.canAssume(user, HopRole.READ_ONLY));
    assertTrue(HopSecurityPrivilegeMode.canAssume(user, HopRole.OPERATOR));
    assertTrue(HopSecurityPrivilegeMode.canAssume(user, HopRole.USER));
    assertFalse(HopSecurityPrivilegeMode.canAssume(user, HopRole.ADMIN));
  }

  @Test
  void readOnlyCannotAssumeOperator() {
    HopSecurityContext ro = HopSecurityContext.forUser("r", Set.of(HopRole.READ_ONLY));
    assertTrue(HopSecurityPrivilegeMode.canAssume(ro, HopRole.READ_ONLY));
    assertFalse(HopSecurityPrivilegeMode.canAssume(ro, HopRole.OPERATOR));
    assertFalse(HopSecurityPrivilegeMode.canAssume(ro, HopRole.USER));
  }

  @Test
  void unrestrictedCanAssumeAny() {
    HopSecurityContext base = HopSecurityContext.unrestricted();
    assertTrue(HopSecurityPrivilegeMode.canAssume(base, HopRole.READ_ONLY));
    assertTrue(HopSecurityPrivilegeMode.canAssume(base, HopRole.ADMIN));
  }

  @Test
  void createEffectiveIsRestricted() {
    HopSecurityContext admin = HopSecurityContext.forUser("a", Set.of(HopRole.ADMIN));
    HopSecurityContext effective =
        HopSecurityPrivilegeMode.createEffective(admin, HopRole.READ_ONLY);
    assertFalse(effective.isUnrestricted());
    assertTrue(effective.allows(Permission.FILE_VIEW));
    assertFalse(effective.allows(Permission.FILE_EDIT));
    assertFalse(effective.allows(Permission.SECURITY_MANAGE));
    assertEquals("a", effective.getUsername());
  }

  @Test
  void createEffectiveRejectsElevation() {
    HopSecurityContext user = HopSecurityContext.forUser("u", Set.of(HopRole.USER));
    assertThrows(
        IllegalArgumentException.class,
        () -> HopSecurityPrivilegeMode.createEffective(user, HopRole.ADMIN));
  }

  @Test
  void isDowngradedDetectsSubset() {
    HopSecurityContext admin = HopSecurityContext.forUser("a", Set.of(HopRole.ADMIN));
    HopSecurityContext ro = HopSecurityPrivilegeMode.createEffective(admin, HopRole.READ_ONLY);
    assertTrue(HopSecurityPrivilegeMode.isDowngraded(admin, ro));
    assertFalse(HopSecurityPrivilegeMode.isDowngraded(admin, admin));
  }

  @Test
  void assumableRolesForUser() {
    HopSecurityContext user = HopSecurityContext.forUser("u", Set.of(HopRole.USER));
    List<HopRole> roles = HopSecurityPrivilegeMode.assumableRoles(user);
    assertTrue(roles.contains(HopRole.USER));
    assertTrue(roles.contains(HopRole.OPERATOR));
    assertTrue(roles.contains(HopRole.READ_ONLY));
    assertFalse(roles.contains(HopRole.ADMIN));
  }
}
