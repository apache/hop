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

import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityPrivilegeMode;

/** Hop Web: temporary privilege mode stored on the RAP UISession. */
public class HopWebPrivilegeFacadeImpl extends HopWebPrivilegeFacade {

  @Override
  boolean isAvailableInternal() {
    HopSecurityContext base = RapSecurityContextProvider.getBaseContext();
    // Show when authenticated, or when unrestricted with multiple assume options (rare on web)
    if (base.isAuthenticated()) {
      return HopSecurityPrivilegeMode.assumableRoles(base).size() > 1
          || HopSecurityPrivilegeMode.isDowngraded(base, HopSecurity.getContext());
    }
    return false;
  }

  @Override
  HopSecurityContext getBaseContextInternal() {
    return RapSecurityContextProvider.getBaseContext();
  }

  @Override
  String getModeIdInternal() {
    return RapSecurityContextProvider.getPrivilegeModeId();
  }

  @Override
  boolean setModeInternal(String modeId) {
    HopRole role = HopSecurityPrivilegeMode.parseModeRole(modeId);
    if (role == null) {
      return RapSecurityContextProvider.restoreFullPrivileges();
    }
    return RapSecurityContextProvider.assumeRole(role);
  }

  @Override
  String[] getModeComboLabelsInternal() {
    return buildComboLabels(RapSecurityContextProvider.getBaseContext());
  }
}
