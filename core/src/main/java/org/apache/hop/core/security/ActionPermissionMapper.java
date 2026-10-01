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

import java.util.Locale;
import java.util.Optional;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;

/**
 * Maps context-dialog {@link GuiActionType} values and known keyboard-shortcut method names to
 * {@link Permission} for RBAC checks.
 */
public final class ActionPermissionMapper {

  private ActionPermissionMapper() {
    // utility
  }

  /**
   * Permission required for a context-action type. {@link GuiActionType#Info} only needs view.
   * {@link GuiActionType#Custom} is not gated here (callers may refine by action id).
   *
   * @param actionType action type
   * @return permission, or empty if no gate for this type
   */
  public static Optional<Permission> forActionType(GuiActionType actionType) {
    if (actionType == null) {
      return Optional.empty();
    }
    return switch (actionType) {
      case Create -> Optional.of(Permission.FILE_CREATE);
      case Modify, Duplicate -> Optional.of(Permission.FILE_EDIT);
      case Delete -> Optional.of(Permission.FILE_DELETE);
      case Info -> Optional.of(Permission.FILE_VIEW);
      case Custom -> Optional.empty();
    };
  }

  /**
   * Permission for a concrete {@link GuiAction}, including heuristics for {@link
   * GuiActionType#Custom} run/start actions.
   *
   * @param action action
   * @return permission if gated
   */
  public static Optional<Permission> forGuiAction(GuiAction action) {
    if (action == null) {
      return Optional.empty();
    }
    Optional<Permission> byType = forActionType(action.getType());
    if (byType.isPresent()) {
      return byType;
    }
    if (action.getType() == GuiActionType.Custom) {
      String blob =
          (Const.NVL(action.getId(), "")
                  + " "
                  + Const.NVL(action.getName(), "")
                  + " "
                  + Const.NVL(action.getTooltip(), ""))
              .toLowerCase(Locale.ROOT);
      if (blob.contains("start")
          || blob.contains("run")
          || blob.contains("execute")
          || blob.contains("preview")
          || blob.contains("debug")) {
        if (blob.contains("stop") || blob.contains("pause") || blob.contains("resume")) {
          return Optional.of(Permission.RUN_STOP);
        }
        return Optional.of(Permission.RUN_EXECUTE);
      }
      if (blob.contains("delete") || blob.contains("remove")) {
        return Optional.of(Permission.FILE_DELETE);
      }
    }
    return Optional.empty();
  }

  /**
   * Whether the current security context allows a context action of the given type.
   *
   * @param actionType action type
   * @return true if allowed
   */
  public static boolean allowsActionType(GuiActionType actionType) {
    Optional<Permission> permission = forActionType(actionType);
    return permission.map(HopSecurity::allows).orElse(true);
  }

  /**
   * Whether the current context allows the given GUI action.
   *
   * @param action action
   * @return true if allowed
   */
  public static boolean allowsGuiAction(GuiAction action) {
    return forGuiAction(action).map(HopSecurity::allows).orElse(true);
  }

  /**
   * Best-effort mapping of UI handler method names (keyboard shortcuts, menu methods) to
   * permissions. Unknown methods are not gated (return empty).
   *
   * @param methodName simple method name (e.g. {@code menuFileSave})
   * @return permission if known
   */
  public static Optional<Permission> forMethodName(String methodName) {
    if (methodName == null || methodName.isEmpty()) {
      return Optional.empty();
    }
    String name = methodName.toLowerCase(Locale.ROOT);

    // Save
    if (name.contains("saveas") || name.equals("filesaveas") || name.contains("filesave")) {
      return Optional.of(Permission.FILE_SAVE);
    }
    if (name.contains("menusave") || name.endsWith("filesave") || name.equals("menufilesave")) {
      return Optional.of(Permission.FILE_SAVE);
    }
    if (name.contains("save") && !name.contains("guard")) {
      // menuFileSave, menuFileSaveAs, fileSave, ...
      if (name.contains("filesave") || name.contains("menusave") || name.equals("save")) {
        return Optional.of(Permission.FILE_SAVE);
      }
    }

    // Run / stop
    if (name.contains("runstart")
        || name.contains("startexecution")
        || name.equals("start")
        || name.contains("menurunstart")
        || name.contains("execute")
        || name.contains("preview")
        || name.contains("debug")) {
      if (name.contains("stop") || name.contains("pause") || name.contains("resume")) {
        return Optional.of(Permission.RUN_STOP);
      }
      return Optional.of(Permission.RUN_EXECUTE);
    }
    if (name.contains("runstop")
        || name.contains("runpause")
        || name.contains("runresume")
        || name.equals("stop")
        || name.equals("pause")
        || name.equals("resume")) {
      return Optional.of(Permission.RUN_STOP);
    }

    // Edit operations (menus, graph shortcuts: deleteSelected, cutSelectedToClipboard, …)
    if (name.contains("menueditdelete")
        || name.contains("menueditcut")
        || name.contains("menueditpaste")
        || name.equals("delete")
        || name.equals("cut")
        || name.equals("paste")
        || name.contains("deleteselected")
        || name.equals("delselected")
        || name.contains("cutselected")
        || name.contains("pastefromclipboard")
        || name.contains("pastexml")) {
      return Optional.of(Permission.FILE_EDIT);
    }

    // New file
    if (name.contains("menufilenew") || name.equals("filenew") || name.equals("newfile")) {
      return Optional.of(Permission.FILE_CREATE);
    }

    // Explorer
    if (name.contains("createfolder")
        || name.contains("deletefile")
        || name.contains("renamefile")) {
      return Optional.of(Permission.EXPLORER_WRITE);
    }

    // Export
    if (name.contains("exporttosvg") || name.contains("export")) {
      return Optional.of(Permission.FILE_EXPORT);
    }

    // Explicit common HopGui method names
    return switch (methodName) {
      case "menuFileSave", "menuFileSaveAs", "fileSave", "fileSaveAs" ->
          Optional.of(Permission.FILE_SAVE);
      case "menuRunStart", "menuRunPreview", "menuRunDebug" -> Optional.of(Permission.RUN_EXECUTE);
      case "menuRunStop", "menuRunPause", "menuRunResume" -> Optional.of(Permission.RUN_STOP);
      case "menuEditDelete",
              "menuEditCut",
              "menuEditPaste",
              "menuEditCopy",
              "deleteSelected",
              "delSelected",
              "cutSelectedToClipboard",
              "pasteFromClipboard" ->
          Optional.of(Permission.FILE_EDIT);
      case "menuFileNew", "menuFileNewPipeline", "menuFileNewWorkflow" ->
          Optional.of(Permission.FILE_CREATE);
      case "menuFileExportToSvg" -> Optional.of(Permission.FILE_EXPORT);
      case "createFolder", "deleteFile", "renameFile" -> Optional.of(Permission.EXPLORER_WRITE);
      default -> Optional.empty();
    };
  }

  /**
   * Whether the current context allows invoking the given UI method (keyboard shortcut / menu).
   * Unknown methods are allowed.
   *
   * @param methodName method name
   * @return true if allowed
   */
  public static boolean allowsMethod(String methodName) {
    return forMethodName(methodName).map(HopSecurity::allows).orElse(true);
  }
}
