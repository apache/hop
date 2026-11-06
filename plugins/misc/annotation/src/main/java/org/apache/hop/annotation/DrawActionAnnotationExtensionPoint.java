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
import org.apache.hop.workflow.WorkflowPainterExtension;
import org.apache.hop.workflow.action.ActionMeta;

/** Draws the annotation anchored to a workflow action. */
@ExtensionPoint(
    id = "DrawActionAnnotationExtensionPoint",
    extensionPointId = "WorkflowPainterAction",
    description = "Draw an annotation callout next to a workflow action")
public class DrawActionAnnotationExtensionPoint
    implements IExtensionPoint<WorkflowPainterExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowPainterExtension extension) {
    if (extension == null || extension.actionMeta == null) {
      return;
    }
    ActionMeta action = extension.actionMeta;
    AnnotationPainter.paint(
        extension.gc,
        extension.areaOwners,
        extension.offset,
        action,
        extension.x1,
        extension.y1,
        extension.iconSize,
        action.isSelected(),
        AnnotationStore.read(action));
  }
}
