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
import jakarta.servlet.http.HttpSession;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.ui.hopgui.security.HopBasicAuthFilter;
import org.apache.hop.ui.hopgui.security.HopLoginPage;
import org.apache.hop.ui.hopgui.security.HopOidcAuthFilter;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.service.JavaScriptExecutor;
import org.eclipse.rap.rwt.service.UISession;

/**
 * Hop Web log-off: clear session identity and send the browser to the Hop login page (BASIC) or
 * reload the UI for EXTERNAL re-challenge.
 */
public class HopWebLogoutFacadeImpl extends HopWebLogoutFacade {

  @Override
  void logOffInternal() {
    try {
      HttpServletRequest request = RWT.getRequest();
      if (request != null) {
        HttpSession session = request.getSession(false);
        if (session != null) {
          session.removeAttribute(HopBasicAuthFilter.SESSION_PRINCIPAL);
          session.removeAttribute(HopBasicAuthFilter.SESSION_REJECT_AUTH);
          session.removeAttribute(HopBasicAuthFilter.SESSION_FORCE_REAUTH);
          try {
            session.invalidate();
          } catch (IllegalStateException e) {
            LogChannel.UI.logDebug("Session already invalidated", e);
          }
        }
        try {
          request.logout();
        } catch (Exception e) {
          LogChannel.UI.logDebug("request.logout() not available or failed", e);
        }
      }

      UISession uiSession = RWT.getUISession();
      if (uiSession != null) {
        uiSession.removeAttribute(HopSecurity.SESSION_CONTEXT_ATTRIBUTE);
      }

      String contextPath = "";
      try {
        if (request != null && request.getContextPath() != null) {
          contextPath = HopLoginPage.normalizeContext(request.getContextPath());
        }
      } catch (Exception ignored) {
        // ignore
      }

      HopSecurityConfig.AuthMode mode = HopSecurityConfig.load().getAuthMode();
      String target;
      if (mode == HopSecurityConfig.AuthMode.BASIC) {
        target = contextPath + HopLoginPage.PATH_LOGIN + "?logout=1";
      } else if (mode == HopSecurityConfig.AuthMode.OAUTH2) {
        // Use server-side logout for optional IdP end-session
        target = contextPath + HopOidcAuthFilter.PATH_OAUTH_LOGOUT;
      } else {
        target = contextPath + "/ui";
      }
      String escaped = target.replace("\\", "\\\\").replace("'", "\\'");
      JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
      if (executor != null) {
        executor.execute("window.location.replace('" + escaped + "');");
      }
      LogChannel.UI.logBasic("Hop Web: user logged off, redirecting to sign-in");
    } catch (Exception e) {
      LogChannel.UI.logError("Hop Web log-off failed", e);
    }
  }
}
