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

package org.apache.hop.ui.hopgui;

import jakarta.servlet.http.HttpServletRequest;
import java.security.Principal;
import java.util.Set;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityContextResolver;
import org.apache.hop.core.security.HopSecurityPrivilegeMode;
import org.apache.hop.core.security.ISecurityContextProvider;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/**
 * RAP session-aware {@link ISecurityContextProvider}. Returns the <em>effective</em> session
 * context (after optional temporary privilege downgrade). Login-time base context is stored
 * separately so privileges can be restored without re-authentication.
 */
public class RapSecurityContextProvider implements ISecurityContextProvider {

  @Override
  public HopSecurityContext getContext() {
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return HopSecurityContext.unrestricted();
      }
      Object attr = session.getAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE);
      if (attr instanceof HopSecurityContext hopSecurityContext) {
        return hopSecurityContext;
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug("Hop security: unable to read UISession context", e);
    }
    return HopSecurityContext.unrestricted();
  }

  /**
   * Resolve identity from the current HTTP request and store base + effective contexts on the
   * UISession. Preserves an active temporary privilege mode when the authentic base is refreshed.
   *
   * @return the <em>effective</em> context after re-applying privilege mode (never null)
   */
  public static HopSecurityContext bindFromCurrentRequest() {
    HopSecurityContext authentic = resolveFromCurrentRequest();
    return bindBaseAndApplyMode(authentic);
  }

  /**
   * Store the login-time base context and set effective = base (or re-apply current mode).
   *
   * @param authentic login-time context
   * @return effective context
   */
  public static HopSecurityContext bindBaseAndApplyMode(HopSecurityContext authentic) {
    if (authentic == null) {
      authentic = HopSecurityContext.unrestricted();
    }
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return authentic;
      }
      session.setAttribute(HopSecurity.SESSION_BASE_CONTEXT_ATTRIBUTE, authentic);
      String mode = (String) session.getAttribute(HopSecurity.SESSION_PRIVILEGE_MODE_ATTRIBUTE);
      HopRole assumed = HopSecurityPrivilegeMode.parseModeRole(mode);
      HopSecurityContext effective = authentic;
      if (assumed != null && HopSecurityPrivilegeMode.canAssume(authentic, assumed)) {
        effective = HopSecurityPrivilegeMode.createEffective(authentic, assumed);
      } else {
        session.setAttribute(
            HopSecurity.SESSION_PRIVILEGE_MODE_ATTRIBUTE, HopSecurityPrivilegeMode.MODE_FULL);
      }
      session.setAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE, effective);
      return effective;
    } catch (Exception e) {
      LogChannel.UI.logDebug("Hop security: unable to store context on UISession", e);
      return authentic;
    }
  }

  /** Login-time base context for the current UI session (or effective if base missing). */
  public static HopSecurityContext getBaseContext() {
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return HopSecurity.getContext();
      }
      Object base = session.getAttribute(HopSecurity.SESSION_BASE_CONTEXT_ATTRIBUTE);
      if (base instanceof HopSecurityContext hopSecurityContext) {
        return hopSecurityContext;
      }
      Object effective = session.getAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE);
      if (effective instanceof HopSecurityContext hopSecurityContext) {
        return hopSecurityContext;
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug("Hop security: unable to read base context", e);
    }
    return HopSecurity.getContext();
  }

  /**
   * Temporarily act as {@code role} (downgrade only). Returns false if not allowed.
   *
   * @param role target built-in role
   * @return true if applied
   */
  public static boolean assumeRole(HopRole role) {
    if (role == null) {
      return restoreFullPrivileges();
    }
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return false;
      }
      HopSecurityContext base = getBaseContext();
      if (!HopSecurityPrivilegeMode.canAssume(base, role)) {
        LogChannel.UI.logBasic(
            "Privilege mode refused: cannot assume ''{0}'' from base {1}",
            role.getId(), base.getRoleIds());
        return false;
      }
      HopSecurityContext effective = HopSecurityPrivilegeMode.createEffective(base, role);
      session.setAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE, effective);
      session.setAttribute(HopSecurity.SESSION_PRIVILEGE_MODE_ATTRIBUTE, role.getId());
      LogChannel.UI.logBasic(
          "Privilege mode: user ''{0}'' base={1} → effective={2}",
          base.getUsername(), base.getRoleIds(), effective.getRoleIds());
      return true;
    } catch (Exception e) {
      LogChannel.UI.logError("Hop security: assumeRole failed", e);
      return false;
    }
  }

  /** Restore login-time privileges. */
  public static boolean restoreFullPrivileges() {
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return false;
      }
      HopSecurityContext base = getBaseContext();
      session.setAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE, base);
      session.setAttribute(
          HopSecurity.SESSION_PRIVILEGE_MODE_ATTRIBUTE, HopSecurityPrivilegeMode.MODE_FULL);
      LogChannel.UI.logBasic(
          "Privilege mode restored for user ''{0}'' ({1})", base.getUsername(), base.getRoleIds());
      return true;
    } catch (Exception e) {
      LogChannel.UI.logError("Hop security: restoreFullPrivileges failed", e);
      return false;
    }
  }

  /** Current temporary mode id ({@link HopSecurityPrivilegeMode#MODE_FULL} or role id). */
  public static String getPrivilegeModeId() {
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        return HopSecurityPrivilegeMode.MODE_FULL;
      }
      Object mode = session.getAttribute(HopSecurity.SESSION_PRIVILEGE_MODE_ATTRIBUTE);
      if (mode instanceof String s && !s.isBlank()) {
        return s;
      }
    } catch (Exception ignored) {
      // fall through
    }
    return HopSecurityPrivilegeMode.MODE_FULL;
  }

  /**
   * Build a security context from {@link HttpServletRequest#getUserPrincipal()} and known container
   * roles via {@link HttpServletRequest#isUserInRole(String)}.
   *
   * @return unrestricted when no principal is present
   */
  public static HopSecurityContext resolveFromCurrentRequest() {
    try {
      HttpServletRequest request = RWT.getRequest();
      if (request == null) {
        return HopSecurityContext.unrestricted();
      }
      Principal principal;
      try {
        principal = request.getUserPrincipal();
      } catch (UnsupportedOperationException e) {
        return HopSecurityContext.unrestricted();
      }
      if (principal == null || principal.getName() == null || principal.getName().isBlank()) {
        return HopSecurityContext.unrestricted();
      }
      String username = principal.getName().trim();
      Set<String> roles =
          HopSecurityContextResolver.collectKnownContainerRoles(request::isUserInRole);
      return HopSecurityContextResolver.resolve(username, roles);
    } catch (Exception e) {
      LogChannel.UI.logDebug("Hop security: failed to resolve context from request", e);
      return HopSecurityContext.unrestricted();
    }
  }
}
