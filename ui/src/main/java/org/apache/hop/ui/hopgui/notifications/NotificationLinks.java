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
package org.apache.hop.ui.hopgui.notifications;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Policy for the links carried by a notification.
 *
 * <p>A link arrives from a remote feed and ends up at {@code
 * org.apache.hop.ui.util.EnvironmentUtils#openUrl(String)}, which hands it to {@code
 * org.eclipse.swt.program.Program#launch(String)} on the desktop. That launches whatever the
 * operating system associates with the string, so a feed could otherwise get a {@code file:},
 * {@code smb:} or executable path opened by a single click on a notification. Only absolute {@code
 * http} and {@code https} URLs with a host are accepted.
 */
final class NotificationLinks {

  private NotificationLinks() {
    // Utility class
  }

  /**
   * Whether this link may be handed to the browser.
   *
   * @param link The link to check, may be null or empty
   * @return true if the link is an absolute http(s) URL naming a host
   */
  static boolean isSafe(String link) {
    if (link == null) {
      return false;
    }
    String trimmed = link.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    try {
      URI uri = new URI(trimmed);
      if (!uri.isAbsolute() || uri.getScheme() == null) {
        return false;
      }
      String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
      if (!"http".equals(scheme) && !"https".equals(scheme)) {
        return false;
      }
      String host = uri.getHost();
      return host != null && !host.isEmpty();
    } catch (URISyntaxException e) {
      return false;
    }
  }
}
