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

package org.apache.hop.ui.hopgui.context;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.ui.core.dialog.ContextDialog;

/**
 * Shared payload helpers for dragging a placeable create action from {@link ContextDialog} onto a
 * pipeline/workflow canvas (issue #3111), especially for Hop Web where HTML5/SWT DnD is used.
 */
public final class ContextDialogPlacement {

  /** TextTransfer payload prefix so drop targets ignore unrelated text. */
  public static final String TRANSFER_PREFIX = "hop-context-placement:";

  private ContextDialogPlacement() {
    // utility
  }

  public static String encode(GuiAction action) {
    if (action == null || StringUtils.isEmpty(action.getId())) {
      return null;
    }
    return TRANSFER_PREFIX + action.getId();
  }

  public static boolean isPlacementPayload(Object data) {
    return data instanceof String s && s.startsWith(TRANSFER_PREFIX);
  }

  /**
   * @return the GuiAction id embedded in a placement payload, or null if not a placement payload
   */
  public static String decodeActionId(Object data) {
    if (!isPlacementPayload(data)) {
      return null;
    }
    return ((String) data).substring(TRANSFER_PREFIX.length());
  }

  /**
   * Notify the active context dialog that a canvas drop already created the transform/action, so
   * {@link org.apache.hop.ui.hopgui.context.GuiContextUtil} must not start a second placement
   * gesture when the dialog closes.
   */
  public static void markDropCompletedOnActiveDialog() {
    ContextDialog dialog = ContextDialog.getInstance();
    if (dialog != null) {
      dialog.markPlacementCompletedByDrop();
    }
  }
}
