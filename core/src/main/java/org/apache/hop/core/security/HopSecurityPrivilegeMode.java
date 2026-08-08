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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Pure helpers for temporary session privilege downgrade (“act as” Operator / Read-only). Effective
 * permissions never exceed the login-time base context.
 */
public final class HopSecurityPrivilegeMode {

  /** Sentinel for “use full base privileges” (not a HopRole id). */
  public static final String MODE_FULL = "full";

  private HopSecurityPrivilegeMode() {}

  /**
   * Whether {@code base} may temporarily act as {@code target}. Unrestricted base may assume any
   * role (desktop simulation). Otherwise every permission of {@code target} must already be granted
   * by {@code base} (no elevation).
   */
  public static boolean canAssume(HopSecurityContext base, HopRole target) {
    if (base == null || target == null) {
      return false;
    }
    if (base.isUnrestricted()) {
      return true;
    }
    return base.getPermissions().containsAll(target.getPermissions());
  }

  /**
   * Built-in roles the base context may assume (typically Operator and Read-only for admins/users;
   * also the user's own strongest roles).
   */
  public static List<HopRole> assumableRoles(HopSecurityContext base) {
    List<HopRole> list = new ArrayList<>();
    if (base == null) {
      return list;
    }
    for (HopRole role : HopRole.values()) {
      if (canAssume(base, role)) {
        list.add(role);
      }
    }
    return list;
  }

  /**
   * Build a restricted effective context for the given role, preserving username and source roles.
   *
   * @param base login-time context
   * @param target role to act as
   * @return effective context
   * @throws IllegalArgumentException if elevation would result
   */
  public static HopSecurityContext createEffective(HopSecurityContext base, HopRole target) {
    if (!canAssume(base, target)) {
      throw new IllegalArgumentException(
          "Cannot assume role "
              + (target != null ? target.getId() : null)
              + " with base context "
              + base);
    }
    String username = base.getUsername();
    if (username == null
        || username.isBlank()
        || HopSecurityContext.ANONYMOUS_USERNAME.equals(username)) {
      username = "session";
    }
    return HopSecurityContext.forUser(username, EnumSet.of(target), base.getSourceRoles());
  }

  /**
   * Parse a UI/session mode id: {@link #MODE_FULL} or a {@link HopRole#getId()}.
   *
   * @return role or null when full / unknown
   */
  public static HopRole parseModeRole(String modeId) {
    if (modeId == null || modeId.isBlank()) {
      return null;
    }
    String n = modeId.trim().toLowerCase(Locale.ROOT);
    if (MODE_FULL.equals(n) || "full".equals(n) || "base".equals(n)) {
      return null;
    }
    return HopRole.fromIdOrAlias(n);
  }

  /**
   * Whether the effective context is a temporary downgrade relative to base (not unrestricted full
   * access).
   */
  public static boolean isDowngraded(HopSecurityContext base, HopSecurityContext effective) {
    if (base == null || effective == null) {
      return false;
    }
    if (base.isUnrestricted() && !effective.isUnrestricted()) {
      return true;
    }
    if (base.isUnrestricted() || effective.isUnrestricted()) {
      return false;
    }
    // Downgraded if effective has strictly fewer permissions
    Set<Permission> bp = base.getPermissions();
    Set<Permission> ep = effective.getPermissions();
    return bp.containsAll(ep) && !ep.containsAll(bp);
  }
}
