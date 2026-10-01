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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityPrivilegeMode;
import org.apache.hop.core.security.ISecurityContextProvider;

/**
 * Desktop: optional session privilege simulation for unrestricted sessions (explore as Operator /
 * Read-only without re-login).
 */
public class HopWebPrivilegeFacadeImpl extends HopWebPrivilegeFacade {

  private static volatile HopSecurityContext base = HopSecurityContext.unrestricted();
  private static volatile HopSecurityContext effective = HopSecurityContext.unrestricted();
  private static volatile String modeId = HopSecurityPrivilegeMode.MODE_FULL;
  private static volatile boolean installed;

  private static synchronized void ensureProviderInstalled() {
    if (installed) {
      return;
    }
    HopSecurity.setProvider(
        new ISecurityContextProvider() {
          @Override
          public HopSecurityContext getContext() {
            return effective;
          }
        });
    installed = true;
  }

  @Override
  boolean isAvailableInternal() {
    // Desktop: allow simulation for unrestricted sessions
    return base.isUnrestricted() || base.isAuthenticated();
  }

  @Override
  HopSecurityContext getBaseContextInternal() {
    return base;
  }

  @Override
  String getModeIdInternal() {
    return modeId;
  }

  @Override
  boolean setModeInternal(String modeIdParam) {
    ensureProviderInstalled();
    HopRole role = HopSecurityPrivilegeMode.parseModeRole(modeIdParam);
    if (role == null) {
      effective = base;
      modeId = HopSecurityPrivilegeMode.MODE_FULL;
      LogChannel.UI.logBasic("Desktop privilege mode restored (full)");
      return true;
    }
    if (!HopSecurityPrivilegeMode.canAssume(base, role)) {
      return false;
    }
    effective = HopSecurityPrivilegeMode.createEffective(base, role);
    modeId = role.getId();
    LogChannel.UI.logBasic("Desktop privilege mode → {0}", role.getId());
    return true;
  }

  @Override
  String[] getModeComboLabelsInternal() {
    return buildComboLabels(base);
  }
}
