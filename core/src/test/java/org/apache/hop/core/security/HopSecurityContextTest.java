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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HopSecurityContextTest {

  @AfterEach
  void resetProvider() {
    HopSecurity.reset();
  }

  @Test
  void unrestrictedAllowsEverything() {
    HopSecurityContext ctx = HopSecurityContext.unrestricted();
    assertTrue(ctx.isUnrestricted());
    assertFalse(ctx.isAuthenticated());
    for (Permission permission : Permission.values()) {
      assertTrue(ctx.allows(permission), "unrestricted should allow " + permission);
    }
  }

  @Test
  void adminHasAllPermissions() {
    HopSecurityContext ctx = HopSecurityContext.forUser("alice", Set.of(HopRole.ADMIN));
    assertTrue(ctx.isAuthenticated());
    assertEquals("alice", ctx.getUsername());
    assertTrue(ctx.getRoleIds().contains("admin"));
    for (Permission permission : Permission.values()) {
      assertTrue(ctx.allows(permission), "admin should allow " + permission);
    }
  }

  @Test
  void userCanCrudAndExecuteButNotManageSecurity() {
    HopSecurityContext ctx = HopSecurityContext.forUser("bob", Set.of(HopRole.USER));
    assertTrue(ctx.allows(Permission.FILE_SAVE));
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertTrue(ctx.allows(Permission.METADATA_WRITE));
    assertTrue(ctx.allows(Permission.EXPLORER_WRITE));
    assertFalse(ctx.allows(Permission.SECURITY_MANAGE));
    assertFalse(ctx.allows(Permission.CONFIG_SYSTEM));
    assertFalse(ctx.allows(Permission.PLUGIN_MANAGE));
  }

  @Test
  void operatorCanExecuteButNotSaveOrEdit() {
    HopSecurityContext ctx = HopSecurityContext.forUser("ops", Set.of(HopRole.OPERATOR));
    assertTrue(ctx.allows(Permission.FILE_VIEW));
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertTrue(ctx.allows(Permission.RUN_STOP));
    assertTrue(ctx.allows(Permission.METADATA_READ));
    assertFalse(ctx.allows(Permission.FILE_SAVE));
    assertFalse(ctx.allows(Permission.FILE_EDIT));
    assertFalse(ctx.allows(Permission.FILE_CREATE));
    assertFalse(ctx.allows(Permission.FILE_DELETE));
    assertFalse(ctx.allows(Permission.METADATA_WRITE));
    assertFalse(ctx.allows(Permission.EXPLORER_WRITE));
    assertFalse(ctx.allows(Permission.PLUGIN_MANAGE));
  }

  @Test
  void readOnlyCanOnlyView() {
    HopSecurityContext ctx = HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY));
    assertTrue(ctx.allows(Permission.FILE_VIEW));
    assertTrue(ctx.allows(Permission.METADATA_READ));
    assertTrue(ctx.allows(Permission.CONFIG_GUI));
    assertFalse(ctx.allows(Permission.RUN_EXECUTE));
    assertFalse(ctx.allows(Permission.FILE_SAVE));
    assertFalse(ctx.allows(Permission.FILE_EDIT));
    assertFalse(ctx.allows(Permission.FILE_EXPORT));
    assertFalse(ctx.allows(Permission.PLUGIN_MANAGE));
  }

  @Test
  void onlyAdminMayManagePlugins() {
    assertTrue(
        HopSecurityContext.forUser("a", Set.of(HopRole.ADMIN)).allows(Permission.PLUGIN_MANAGE));
    assertFalse(
        HopSecurityContext.forUser("u", Set.of(HopRole.USER)).allows(Permission.PLUGIN_MANAGE));
    assertFalse(
        HopSecurityContext.forUser("o", Set.of(HopRole.OPERATOR)).allows(Permission.PLUGIN_MANAGE));
    assertFalse(
        HopSecurityContext.forUser("r", Set.of(HopRole.READ_ONLY))
            .allows(Permission.PLUGIN_MANAGE));
    assertTrue(HopSecurityContext.unrestricted().allows(Permission.PLUGIN_MANAGE));
  }

  @Test
  void roleUnionCombinesPermissions() {
    HopSecurityContext ctx =
        HopSecurityContext.forUser("multi", EnumSet.of(HopRole.OPERATOR, HopRole.READ_ONLY));
    // Operator already supersets read-only for view; still no save
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertFalse(ctx.allows(Permission.FILE_SAVE));
    assertEquals(2, ctx.getRoleIds().size());
  }

  @Test
  void resolverMapsContainerRoles() {
    HopSecurityContext admin = HopSecurityContextResolver.resolve("admin1", List.of("hop-admin"));
    assertTrue(admin.allows(Permission.SECURITY_MANAGE));

    HopSecurityContext user = HopSecurityContextResolver.resolve("u1", List.of("apachehop"));
    assertTrue(user.allows(Permission.FILE_SAVE));
    assertFalse(user.allows(Permission.SECURITY_MANAGE));

    HopSecurityContext operator = HopSecurityContextResolver.resolve("o1", List.of("hop-operator"));
    assertTrue(operator.allows(Permission.RUN_EXECUTE));
    assertFalse(operator.allows(Permission.FILE_SAVE));

    HopSecurityContext readonly = HopSecurityContextResolver.resolve("r1", List.of("hop-readonly"));
    assertTrue(readonly.allows(Permission.FILE_VIEW));
    assertFalse(readonly.allows(Permission.RUN_EXECUTE));
  }

  @Test
  void resolverDefaultsAuthenticatedWithoutKnownRoleToUser() {
    HopSecurityContext ctx =
        HopSecurityContextResolver.resolve("legacy", List.of("some-custom-role"));
    assertTrue(ctx.allows(Permission.FILE_SAVE));
    assertFalse(ctx.allows(Permission.SECURITY_MANAGE));
  }

  @Test
  void resolverUnauthenticatedIsUnrestricted() {
    HopSecurityContext ctx = HopSecurityContextResolver.resolve(null, List.of());
    assertTrue(ctx.isUnrestricted());
    assertTrue(
        HopSecurityContextResolver.resolve("  ", (Collection<String>) null).isUnrestricted());
  }

  @Test
  void hopSecurityProviderIsConsulted() {
    HopSecurityContext readonly = HopSecurityContext.forUser("r", Set.of(HopRole.READ_ONLY));
    HopSecurity.setProvider(() -> readonly);
    assertFalse(HopSecurity.allows(Permission.FILE_SAVE));
    assertTrue(HopSecurity.allows(Permission.FILE_VIEW));
    assertFalse(HopSecurity.allowsCapability(CapabilityPermissionMapper.CAPABILITY_SAVE));
    assertTrue(HopSecurity.allowsCapability(CapabilityPermissionMapper.CAPABILITY_SELECT));
    HopSecurity.reset();
    assertTrue(HopSecurity.allows(Permission.FILE_SAVE));
  }

  @Test
  void capabilityMapperCoversRunAndEdit() {
    HopSecurityContext operator = HopSecurityContext.forUser("o", Set.of(HopRole.OPERATOR));
    assertTrue(
        CapabilityPermissionMapper.allows(operator, CapabilityPermissionMapper.CAPABILITY_START));
    assertFalse(
        CapabilityPermissionMapper.allows(operator, CapabilityPermissionMapper.CAPABILITY_SAVE));
    assertFalse(
        CapabilityPermissionMapper.allows(operator, CapabilityPermissionMapper.CAPABILITY_DELETE));
    // Unmapped capability is not gated
    assertTrue(CapabilityPermissionMapper.allows(operator, "Close"));
  }

  @Test
  void roleAliasesAreRecognized() {
    assertEquals(HopRole.ADMIN, HopRole.fromIdOrAlias("hop-admin"));
    assertEquals(HopRole.USER, HopRole.fromIdOrAlias("apachehop"));
    assertEquals(HopRole.OPERATOR, HopRole.fromIdOrAlias("operator"));
    assertEquals(HopRole.READ_ONLY, HopRole.fromIdOrAlias("read-only"));
    assertEquals(HopRole.READ_ONLY, HopRole.fromIdOrAlias("viewer"));
  }

  @Test
  void actionTypeMapping() {
    assertTrue(ActionPermissionMapper.allowsActionType(null));
    HopSecurityContext readonly = HopSecurityContext.forUser("r", Set.of(HopRole.READ_ONLY));
    HopSecurity.setProvider(() -> readonly);
    assertFalse(
        ActionPermissionMapper.allowsActionType(
            org.apache.hop.core.gui.plugin.action.GuiActionType.Create));
    assertFalse(
        ActionPermissionMapper.allowsActionType(
            org.apache.hop.core.gui.plugin.action.GuiActionType.Modify));
    assertTrue(
        ActionPermissionMapper.allowsActionType(
            org.apache.hop.core.gui.plugin.action.GuiActionType.Info));
    HopSecurity.reset();
  }

  @Test
  void methodNameMapping() {
    assertEquals(
        Optional.of(Permission.FILE_SAVE), ActionPermissionMapper.forMethodName("menuFileSave"));
    assertEquals(
        Optional.of(Permission.RUN_EXECUTE), ActionPermissionMapper.forMethodName("menuRunStart"));
    assertEquals(
        Optional.of(Permission.EXPLORER_WRITE), ActionPermissionMapper.forMethodName("deleteFile"));
    assertTrue(ActionPermissionMapper.forMethodName("unknownMethod").isEmpty());
  }

  @Test
  void customRoleMappingsFromConfig() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.getRoleMappings().put("my-ops-role", "operator");
    HopSecurityContext ctx =
        HopSecurityContextResolver.resolve("ops", List.of("my-ops-role"), config);
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertFalse(ctx.allows(Permission.FILE_SAVE));
  }
}
