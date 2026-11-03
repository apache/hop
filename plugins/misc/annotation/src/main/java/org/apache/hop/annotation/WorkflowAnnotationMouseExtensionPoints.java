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
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;

/** Mouse extension points for a workflow annotation. Each class is one extension point. */
final class WorkflowAnnotationMouseExtensionPoints {
  private WorkflowAnnotationMouseExtensionPoints() {}

  @ExtensionPoint(
      id = "WorkflowAnnotationMouseDownExtensionPoint",
      extensionPointId = "WorkflowGraphMouseDown",
      description = "Edit an annotation when its callout is clicked on a workflow")
  public static class MouseDown implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension extension) {
      AnnotationClicks.workflowMouseDown(extension);
    }
  }

  @ExtensionPoint(
      id = "WorkflowAnnotationMouseUpExtensionPoint",
      extensionPointId = "WorkflowGraphMouseUp",
      description = "Keep a workflow annotation click from acting as a canvas click")
  public static class MouseUp implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension extension) {
      AnnotationClicks.workflowMouseUp(extension);
    }
  }

  @ExtensionPoint(
      id = "WorkflowAnnotationMouseDoubleClickExtensionPoint",
      extensionPointId = "WorkflowGraphMouseDoubleClick",
      description = "Keep a workflow annotation double-click from opening the workflow properties")
  public static class MouseDoubleClick implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension extension) {
      AnnotationClicks.workflowDoubleClick(extension);
    }
  }
}
