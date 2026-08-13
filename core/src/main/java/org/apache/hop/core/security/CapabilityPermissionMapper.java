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

package org.apache.hop.core.security;

import java.util.Map;
import java.util.Optional;

/**
 * Maps {@code IHopFileType} capability strings (e.g. {@code Save}, {@code Start}) to {@link
 * Permission} values so menu/toolbar enablement can enforce RBAC at a single choke point.
 *
 * <p>Capabilities without a mapping do not add an extra security check (file-type capability alone
 * decides). View-oriented capabilities such as Close remain unrestricted for any authenticated
 * session.
 */
public final class CapabilityPermissionMapper {

  /**
   * Capability names mirror {@code org.apache.hop.ui.hopgui.file.IHopFileType} constants. Kept as
   * strings here so {@code hop-core} does not depend on the UI module.
   */
  public static final String CAPABILITY_NEW = "New";

  public static final String CAPABILITY_SAVE = "Save";
  public static final String CAPABILITY_SAVE_AS = "SaveAs";
  public static final String CAPABILITY_EXPORT_TO_SVG = "ExportToSvg";
  public static final String CAPABILITY_START = "Start";
  public static final String CAPABILITY_STOP = "Stop";
  public static final String CAPABILITY_PAUSE = "Pause";
  public static final String CAPABILITY_PREVIEW = "Preview";
  public static final String CAPABILITY_DEBUG = "Debug";
  public static final String CAPABILITY_COPY = "Copy";
  public static final String CAPABILITY_PASTE = "Paste";
  public static final String CAPABILITY_CUT = "Cut";
  public static final String CAPABILITY_DELETE = "Delete";
  public static final String CAPABILITY_SELECT = "Select";
  public static final String CAPABILITY_SNAP_TO_GRID = "SnapToGrid";
  public static final String CAPABILITY_ALIGN_LEFT = "AlignLeft";
  public static final String CAPABILITY_ALIGN_RIGHT = "AlignRight";
  public static final String CAPABILITY_ALIGN_TOP = "AlignTop";
  public static final String CAPABILITY_ALIGN_BOTTOM = "AlignBottom";
  public static final String CAPABILITY_DISTRIBUTE_HORIZONTAL = "DistributeHorizontal";
  public static final String CAPABILITY_DISTRIBUTE_VERTICAL = "DistributeVertical";
  public static final String CAPABILITY_HANDLE_METADATA = "HandleMetadata";

  private static final Map<String, Permission> CAPABILITY_TO_PERMISSION =
      Map.ofEntries(
          Map.entry(CAPABILITY_NEW, Permission.FILE_CREATE),
          Map.entry(CAPABILITY_SAVE, Permission.FILE_SAVE),
          Map.entry(CAPABILITY_SAVE_AS, Permission.FILE_SAVE),
          Map.entry(CAPABILITY_EXPORT_TO_SVG, Permission.FILE_EXPORT),
          Map.entry(CAPABILITY_START, Permission.RUN_EXECUTE),
          Map.entry(CAPABILITY_PREVIEW, Permission.RUN_EXECUTE),
          Map.entry(CAPABILITY_DEBUG, Permission.RUN_EXECUTE),
          Map.entry(CAPABILITY_STOP, Permission.RUN_STOP),
          Map.entry(CAPABILITY_PAUSE, Permission.RUN_STOP),
          Map.entry(CAPABILITY_COPY, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_PASTE, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_CUT, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_DELETE, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_SELECT, Permission.FILE_VIEW),
          Map.entry(CAPABILITY_SNAP_TO_GRID, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_ALIGN_LEFT, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_ALIGN_RIGHT, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_ALIGN_TOP, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_ALIGN_BOTTOM, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_DISTRIBUTE_HORIZONTAL, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_DISTRIBUTE_VERTICAL, Permission.FILE_EDIT),
          Map.entry(CAPABILITY_HANDLE_METADATA, Permission.METADATA_WRITE));

  private CapabilityPermissionMapper() {
    // utility
  }

  /**
   * Map a file-type capability name to a security permission, if any.
   *
   * @param capability capability string from {@code IHopFileType}
   * @return permission, or empty if the capability is not security-gated
   */
  public static Optional<Permission> toPermission(String capability) {
    if (capability == null || capability.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(CAPABILITY_TO_PERMISSION.get(capability));
  }

  /**
   * Whether the security context allows the permission associated with the capability. If the
   * capability has no mapping, returns {@code true} (no extra gate).
   *
   * @param context security context (null treated as unrestricted)
   * @param capability file-type capability name
   * @return true if allowed
   */
  public static boolean allows(HopSecurityContext context, String capability) {
    if (context == null || context.isUnrestricted()) {
      return true;
    }
    Optional<Permission> permission = toPermission(capability);
    return permission.map(context::allows).orElse(true);
  }
}
