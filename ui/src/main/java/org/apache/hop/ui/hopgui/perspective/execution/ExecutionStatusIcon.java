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

package org.apache.hop.ui.hopgui.perspective.execution;

import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.graphics.Image;

/**
 * Tree and tab icon for a pipeline or workflow execution. Failed wins over stalled; everything else
 * uses the default pipeline or workflow image.
 */
public enum ExecutionStatusIcon {
  DEFAULT,
  ERROR,
  STALLED;

  public static ExecutionStatusIcon from(ExecutionState state, long loggingInterval) {
    if (state == null) {
      return DEFAULT;
    }
    if (state.isFailed()) {
      return ERROR;
    }
    if (state.isStale(loggingInterval)) {
      return STALLED;
    }
    return DEFAULT;
  }

  public Image toImage(ExecutionType type) {
    boolean pipeline = type == ExecutionType.Pipeline;
    return switch (this) {
      case ERROR ->
          pipeline
              ? GuiResource.getInstance().getImagePipelineError()
              : GuiResource.getInstance().getImageWorkflowError();
      case STALLED ->
          pipeline
              ? GuiResource.getInstance().getImagePipelineStalled()
              : GuiResource.getInstance().getImageWorkflowStalled();
      case DEFAULT ->
          pipeline
              ? GuiResource.getInstance().getImagePipeline()
              : GuiResource.getInstance().getImageWorkflow();
    };
  }

  public static Image imageFor(ExecutionType type, ExecutionState state, long loggingInterval) {
    return from(state, loggingInterval).toImage(type);
  }
}
