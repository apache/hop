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
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityContextResolver;
import org.apache.hop.core.security.ISecurityContextProvider;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/**
 * RAP session-aware {@link ISecurityContextProvider}. Reads the {@link HopSecurityContext} bound to
 * the UISession at entry-point start; falls back to unrestricted when no session is active.
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
   * Resolve identity from the current HTTP request and store it on the UISession so subsequent
   * authorization checks (menus, toolbars) use the same context for the life of the session.
   *
   * @return the resolved context (never null)
   */
  public static HopSecurityContext bindFromCurrentRequest() {
    HopSecurityContext context = resolveFromCurrentRequest();
    try {
      UISession session = RWT.getUISession();
      if (session != null) {
        session.setAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE, context);
      }
    } catch (Exception e) {
      LogChannel.UI.logDebug("Hop security: unable to store context on UISession", e);
    }
    return context;
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
