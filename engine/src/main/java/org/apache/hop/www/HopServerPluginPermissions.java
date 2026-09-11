/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.www;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.security.HopServerEndpointPermissionMapper;
import org.apache.hop.core.util.Utils;

/**
 * Registers plugin {@link IHopServerPlugin} context paths with {@link
 * HopServerEndpointPermissionMapper} so authenticated Hop Web does not default-deny them.
 */
public final class HopServerPluginPermissions {

  private HopServerPluginPermissions() {}

  /**
   * Register the servlet's {@link IHopServerPlugin#getRequiredPermissionId()} if it is set.
   *
   * @param servlet plugin servlet
   * @param log log channel for a bad permission id
   */
  public static void register(IHopServerPlugin servlet, ILogChannel log) {
    if (servlet == null) {
      return;
    }
    String permissionId = servlet.getRequiredPermissionId();
    if (Utils.isEmpty(permissionId)) {
      return;
    }
    String path = servlet.getContextPath();
    try {
      HopServerEndpointPermissionMapper.register(path, permissionId);
    } catch (IllegalArgumentException e) {
      if (log != null) {
        log.logError(
            "Cannot register Hop Web permission '"
                + permissionId
                + "' for servlet path '"
                + path
                + "'",
            e);
      }
    }
  }

  /**
   * Drop the overlay entry for this servlet path.
   *
   * @param servlet plugin servlet
   */
  public static void unregister(IHopServerPlugin servlet) {
    if (servlet == null || Utils.isEmpty(servlet.getContextPath())) {
      return;
    }
    HopServerEndpointPermissionMapper.unregister(servlet.getContextPath());
  }
}
