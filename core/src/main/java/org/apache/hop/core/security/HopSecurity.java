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
 * Global entry point for authorization checks. Defaults to an unrestricted context (desktop / no
 * auth). Hop Web installs a session-aware {@link ISecurityContextProvider} so concurrent users each
 * see their own roles and permissions.
 *
 * <p>UI code should prefer {@link #allows(Permission)} and {@link #allowsCapability(String)} over
 * reading the context directly, so checks stay consistent.
 */
public final class HopSecurity {

  /**
   * UISession / request attribute key used by Hop Web to store the <em>effective</em> {@link
   * HopSecurityContext} (after any temporary privilege downgrade).
   */
  public static final String SESSION_CONTEXT_ATTRIBUTE = "hop.security.context";

  /**
   * UISession attribute for the login-time base context (never reduced by temporary privilege
   * mode).
   */
  public static final String SESSION_BASE_CONTEXT_ATTRIBUTE = "hop.security.context.base";

  /**
   * UISession attribute: temporary mode id ({@link HopSecurityPrivilegeMode#MODE_FULL} or a Hop
   * role id such as {@code readonly}).
   */
  public static final String SESSION_PRIVILEGE_MODE_ATTRIBUTE = "hop.security.privilege.mode";

  private static final ISecurityContextProvider UNRESTRICTED = HopSecurityContext::unrestricted;

  private static volatile ISecurityContextProvider provider = UNRESTRICTED;

  private HopSecurity() {
    // utility
  }

  /**
   * Install the active security context provider (typically once at Hop Web startup).
   *
   * @param securityContextProvider provider, or null to restore unrestricted default
   */
  public static void setProvider(ISecurityContextProvider securityContextProvider) {
    provider = securityContextProvider != null ? securityContextProvider : UNRESTRICTED;
  }

  /**
   * @return the current security context (never null)
   */
  public static HopSecurityContext getContext() {
    HopSecurityContext context = provider.getContext();
    return context != null ? context : HopSecurityContext.unrestricted();
  }

  /**
   * @param permission permission to check
   * @return true if the current context allows it
   */
  public static boolean allows(Permission permission) {
    return getContext().allows(permission);
  }

  /**
   * Whether the current context allows the security permission mapped from a file-type capability.
   * Unmapped capabilities are allowed (file-type capability alone applies).
   *
   * @param capability {@code IHopFileType} capability name
   * @return true if allowed
   */
  public static boolean allowsCapability(String capability) {
    return CapabilityPermissionMapper.allows(getContext(), capability);
  }

  /**
   * Whether the current context allows invoking a UI method (keyboard shortcut / menu handler).
   * Unknown method names are allowed.
   *
   * @param methodName simple method name
   * @return true if allowed
   */
  public static boolean allowsMethod(String methodName) {
    return ActionPermissionMapper.allowsMethod(methodName);
  }

  /**
   * Whether the current context allows a context-dialog action type.
   *
   * @param actionTypeName name of {@code GuiActionType} enum constant
   * @return true if allowed
   */
  public static boolean allowsActionTypeName(String actionTypeName) {
    if (actionTypeName == null) {
      return true;
    }
    try {
      return ActionPermissionMapper.allowsActionType(
          org.apache.hop.core.gui.plugin.action.GuiActionType.valueOf(actionTypeName));
    } catch (IllegalArgumentException e) {
      return true;
    }
  }

  /** Restore the unrestricted default provider (tests / desktop). */
  public static void reset() {
    provider = UNRESTRICTED;
  }
}
