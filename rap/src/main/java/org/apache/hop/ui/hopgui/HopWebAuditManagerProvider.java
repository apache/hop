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
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.history.IAuditManagerProvider;
import org.apache.hop.history.local.LocalAuditManager;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;

/**
 * Provides a per-user audit manager in Hop Web when the user is authenticated. Audit data (open
 * tabs, history, etc.) is stored under {@code HOP_AUDIT_FOLDER/users/<username>/}.
 */
public class HopWebAuditManagerProvider implements IAuditManagerProvider {

  private static final String SESSION_ATTR_AUDIT_MANAGER = "hop.audit.manager";

  @Override
  public IAuditManager getActiveAuditManager() {
    try {
      UISession session = RWT.getUISession();
      if (session == null) {
        LogChannel.UI.logBasic("Hop Web audit: no UISession, using default audit");
        return null;
      }
      // Use cached manager first so we work during session destroy (no request available)
      IAuditManager manager = (IAuditManager) session.getAttribute(SESSION_ATTR_AUDIT_MANAGER);
      if (manager != null) {
        return manager;
      }
      HttpServletRequest request = RWT.getRequest();
      if (request == null) {
        LogChannel.UI.logBasic(
            "Hop Web audit: no HTTP request (getActiveAuditManager), using default audit");
        return null;
      }
      Principal principal;
      try {
        principal = request.getUserPrincipal();
      } catch (UnsupportedOperationException e) {
        // During session destroy RWT request proxy may throw when calling getUserPrincipal()
        LogChannel.UI.logDebug("Hop Web audit: getUserPrincipal not available", e);
        return null;
      }
      if (principal == null || principal.getName() == null || principal.getName().isEmpty()) {
        LogChannel.UI.logDebug(
            "Hop Web audit: no authenticated user (getUserPrincipal is null or empty), "
                + "using default audit");
        return null;
      }
      String username = sanitizeUsername(principal.getName());
      if (username.isEmpty()) {
        LogChannel.UI.logBasic("Hop Web audit: username empty after sanitize, using default audit");
        return null;
      }
      String userAuditRoot =
          Const.HOP_AUDIT_FOLDER + Const.FILE_SEPARATOR + "users" + Const.FILE_SEPARATOR + username;
      manager = new LocalAuditManager(userAuditRoot);
      session.setAttribute(SESSION_ATTR_AUDIT_MANAGER, manager);
      LogChannel.UI.logBasic(
          "Hop Web audit: using per-user audit folder for user ''{0}'' at ''{1}''",
          username, userAuditRoot);
      return manager;
    } catch (Exception e) {
      LogChannel.UI.logError("Hop Web audit: failed to get per-user audit manager", e);
      return null;
    }
  }

  /**
   * Sanitizes a username for use as a directory name (alphanumeric, dash, underscore only).
   *
   * @param username raw username from principal
   * @return safe folder name, or empty string if invalid
   */
  static String sanitizeUsername(String username) {
    if (username == null || username.isEmpty()) {
      return "";
    }
    return username.replaceAll("[^a-zA-Z0-9_\\-.]", "_").trim();
  }
}
