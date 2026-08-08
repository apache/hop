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

package org.apache.hop.marketplace.gui;

import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.Permission;
import org.apache.hop.ui.core.security.HopSecurityUi;

/**
 * Authorization helpers for marketplace mutations (install, uninstall, repository config, hop-env
 * apply). Browse/validate remain open to all roles.
 */
public final class MarketplaceSecurity {

  private MarketplaceSecurity() {}

  /**
   * Whether the effective session may mutate the shared Hop install via the marketplace.
   *
   * @return true if {@link Permission#PLUGIN_MANAGE} is allowed (Admin among built-in roles;
   *     unrestricted desktop sessions always allow)
   */
  public static boolean canManagePlugins() {
    return HopSecurity.allows(Permission.PLUGIN_MANAGE);
  }

  /**
   * Check manage permission; show the standard access-denied dialog when denied.
   *
   * @return true if allowed
   */
  public static boolean checkManagePlugins() {
    return HopSecurityUi.check(Permission.PLUGIN_MANAGE);
  }
}
