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

package org.apache.hop.annotation;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;

/** Mouse extension points for a pipeline annotation. Each class is one extension point. */
final class PipelineAnnotationMouseExtensionPoints {
  private PipelineAnnotationMouseExtensionPoints() {}

  @ExtensionPoint(
      id = "PipelineAnnotationMouseDownExtensionPoint",
      extensionPointId = "PipelineGraphMouseDown",
      description = "Edit an annotation when its callout is clicked on a pipeline")
  public static class MouseDown implements IExtensionPoint<HopGuiPipelineGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension extension) {
      AnnotationClicks.pipelineMouseDown(extension);
    }
  }

  @ExtensionPoint(
      id = "PipelineAnnotationMouseUpExtensionPoint",
      extensionPointId = "PipelineGraphMouseUp",
      description = "Keep a pipeline annotation click from acting as a canvas click")
  public static class MouseUp implements IExtensionPoint<HopGuiPipelineGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension extension) {
      AnnotationClicks.pipelineMouseUp(extension);
    }
  }

  @ExtensionPoint(
      id = "PipelineAnnotationMouseDoubleClickExtensionPoint",
      extensionPointId = "PipelineGraphMouseDoubleClick",
      description = "Keep a pipeline annotation double-click from opening the pipeline properties")
  public static class MouseDoubleClick implements IExtensionPoint<HopGuiPipelineGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension extension) {
      AnnotationClicks.pipelineDoubleClick(extension);
    }
  }
}
