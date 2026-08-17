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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs.security;

import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUserStore;

/**
 * Optional contract for Security configuration sub-tabs (built-in and plugin-contributed).
 *
 * <p>Plugins add a tab by declaring a {@code @GuiPlugin} with a {@code @GuiTab} whose {@code
 * parentId} is {@link
 * org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab#SECURITY_CONFIG_TABS},
 * and implementing this interface so Save / Reload include their settings.
 *
 * <p>Example (Projects plugin, future): map users/groups or LDAP attributes to allowed project
 * names in a dedicated tab under Security.
 */
public interface ISecurityConfigSection {

  /**
   * Load widgets from the given config / user store.
   *
   * @param config security config (never null)
   * @param store BASIC user store (never null; may be empty when mode is not BASIC)
   */
  void loadFrom(HopSecurityConfig config, HopUserStore store);

  /**
   * Copy widget values into the in-memory config before it is written to disk.
   *
   * @param config mutable security config
   * @throws Exception if validation fails
   */
  void applyTo(HopSecurityConfig config) throws Exception;

  /**
   * Optional second-phase persist (e.g. users.json). Called after {@link HopSecurityConfig} is
   * saved.
   *
   * @param config the config just saved (mode is final)
   * @throws Exception if validation or write fails
   */
  default void persistSecondary(HopSecurityConfig config) throws Exception {
    // no-op
  }
}
