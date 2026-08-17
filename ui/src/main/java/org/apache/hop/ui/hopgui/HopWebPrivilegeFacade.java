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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityPrivilegeMode;
import org.apache.hop.i18n.BaseMessages;

/**
 * Temporary session privilege downgrade (act as Operator / Read-only). Hop Web uses the RAP
 * session; desktop uses an in-process overlay for simulation.
 */
public abstract class HopWebPrivilegeFacade {

  private static final Class<?> PKG = HopGui.class;
  private static final HopWebPrivilegeFacade IMPL;

  static {
    IMPL = (HopWebPrivilegeFacade) ImplementationLoader.newInstance(HopWebPrivilegeFacade.class);
  }

  /** Whether a privilege-mode control should be shown for the current session. */
  public static boolean isAvailable() {
    return IMPL.isAvailableInternal();
  }

  public static HopSecurityContext getBaseContext() {
    return IMPL.getBaseContextInternal();
  }

  public static String getModeId() {
    return IMPL.getModeIdInternal();
  }

  /**
   * Apply mode: {@link HopSecurityPrivilegeMode#MODE_FULL} or a Hop role id.
   *
   * @return true if applied
   */
  public static boolean setMode(String modeId) {
    return IMPL.setModeInternal(modeId);
  }

  /**
   * Combo labels for the toolbar (localized). First entry is always full access when multiple
   * options exist.
   */
  public static String[] getModeComboLabels() {
    return IMPL.getModeComboLabelsInternal();
  }

  /** Map a combo label back to a mode id. */
  public static String labelToModeId(String label) {
    if (label == null) {
      return HopSecurityPrivilegeMode.MODE_FULL;
    }
    String full = BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Full");
    if (label.equals(full) || label.toLowerCase().contains("full")) {
      return HopSecurityPrivilegeMode.MODE_FULL;
    }
    for (HopRole role : HopRole.values()) {
      String roleLabel =
          BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Role." + role.name());
      if (label.equals(roleLabel) || label.equalsIgnoreCase(role.getId())) {
        return role.getId();
      }
    }
    HopRole parsed = HopSecurityPrivilegeMode.parseModeRole(label);
    return parsed != null ? parsed.getId() : HopSecurityPrivilegeMode.MODE_FULL;
  }

  public static String modeIdToLabel(String modeId) {
    HopRole role = HopSecurityPrivilegeMode.parseModeRole(modeId);
    if (role == null) {
      return BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Full");
    }
    return BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Role." + role.name());
  }

  /** Shared label list from a base context (used by RAP and desktop impls). */
  protected static String[] buildComboLabels(HopSecurityContext base) {
    List<String> labels = new ArrayList<>();
    labels.add(BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Full"));
    for (HopRole role : HopSecurityPrivilegeMode.assumableRoles(base)) {
      // Skip listing ADMIN as a separate "downgrade" when base is already only admin — still useful
      // for unrestricted simulation
      labels.add(BaseMessages.getString(PKG, "HopGui.Toolbar.Privilege.Role." + role.name()));
    }
    return labels.toArray(new String[0]);
  }

  protected static boolean setModeOnContexts(
      String modeId,
      HopSecurityContext base,
      java.util.function.Consumer<HopSecurityContext> setEffective,
      java.util.function.Consumer<String> setModeAttr) {
    HopRole role = HopSecurityPrivilegeMode.parseModeRole(modeId);
    if (role == null) {
      setEffective.accept(base);
      setModeAttr.accept(HopSecurityPrivilegeMode.MODE_FULL);
      return true;
    }
    if (!HopSecurityPrivilegeMode.canAssume(base, role)) {
      return false;
    }
    setEffective.accept(HopSecurityPrivilegeMode.createEffective(base, role));
    setModeAttr.accept(role.getId());
    return true;
  }

  abstract boolean isAvailableInternal();

  abstract HopSecurityContext getBaseContextInternal();

  abstract String getModeIdInternal();

  abstract boolean setModeInternal(String modeId);

  abstract String[] getModeComboLabelsInternal();
}
