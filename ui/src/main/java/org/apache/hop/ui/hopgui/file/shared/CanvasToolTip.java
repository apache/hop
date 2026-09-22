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

package org.apache.hop.ui.hopgui.file.shared;

import java.util.EnumSet;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.gui.AreaOwner.AreaType;

/**
 * The kinds of tooltip the pipeline and workflow canvas can show. Each one can be switched off on
 * its own in the Look &amp; Feel options; the global "Show tool tips" option still switches all of
 * them off at once.
 */
@Getter
public enum CanvasToolTip {
  /** "Click to edit" on a transform or action name. */
  EDIT_HINT("EditHint"),
  /**
   * The description of a transform or action on its icon and info badge, along with the transform
   * partitioning and the "can start a pipeline" note.
   */
  DESCRIPTION("Description"),
  /** The warning on the icon of a deprecated transform or action. */
  DEPRECATION("Deprecation"),
  /** Hop lines and every badge on a hop: info, error, copies, row distribution, targets. */
  HOP("Hop"),
  /** What a run left behind: failure logs, output row buffers, action results and checkpoints. */
  EXECUTION_RESULT("ExecutionResult"),
  /** The link target of a hyperlink in a note. */
  NOTE_LINK("NoteLink"),
  /** Tooltips that plugins add through the area-hover extension points. */
  PLUGIN("Plugin"),
  /** The "Selection cleared" notice after a click on the empty canvas. */
  NOTICE("Notice");

  private final String code;

  CanvasToolTip(String code) {
    this.code = code;
  }

  /** The key of the checkbox label in the options dialog bundle. */
  public String getLabelKey() {
    return "EnterOptionsDialog.CanvasToolTip." + code + ".Label";
  }

  /** The key of the checkbox tooltip in the options dialog bundle. */
  public String getToolTipKey() {
    return "EnterOptionsDialog.CanvasToolTip." + code + ".ToolTip";
  }

  /**
   * The kinds of tooltip an area of the canvas can put up. Usually one; an icon can carry either a
   * deprecation warning or a description, so it lists both.
   */
  public static Set<CanvasToolTip> forAreaType(AreaType areaType) {
    if (areaType == null) {
      return EnumSet.noneOf(CanvasToolTip.class);
    }
    return switch (areaType) {
      case TRANSFORM_NAME, ACTION_NAME -> EnumSet.of(EDIT_HINT);
      case TRANSFORM_ICON, TRANSFORM_INFO_ICON, ACTION_ICON, ACTION_INFO_ICON ->
          EnumSet.of(DESCRIPTION, DEPRECATION);
      case TRANSFORM_PARTITIONING -> EnumSet.of(DESCRIPTION);
      case HOP_COPY_ICON,
              HOP_ERROR_ICON,
              HOP_INFO_ICON,
              HOP_INFO_TRANSFORM_COPIES_ERROR,
              HOP_INFO_TRANSFORMS_PARTITIONED,
              TRANSFORM_TARGET_HOP_ICON,
              ROW_DISTRIBUTION_ICON,
              WORKFLOW_HOP_ICON,
              WORKFLOW_HOP_PARALLEL_ICON ->
          EnumSet.of(HOP);
      case TRANSFORM_FAILURE_ICON,
              TRANSFORM_OUTPUT_DATA,
              HOP_OUTPUT_DATA,
              ACTION_RESULT_SUCCESS,
              ACTION_RESULT_FAILURE,
              ACTION_RESULT_CHECKPOINT ->
          EnumSet.of(EXECUTION_RESULT);
      case NOTE_LINK -> EnumSet.of(NOTE_LINK);
        // CUSTOM and every area the graphs do not describe themselves go to the plugin extension
        // points.
      default -> EnumSet.of(PLUGIN);
    };
  }
}
