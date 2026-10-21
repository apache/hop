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

package org.apache.hop.ai.ui;

import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * A pipeline was saved: its AI Assistant sessions take the name of the file. A new pipeline is "New
 * pipeline" until it is first saved.
 */
@ExtensionPoint(
    id = "AiAdvisorPipelineSavedExtension",
    extensionPointId = "PipelineAfterSave",
    description = "Give the AI Assistant sessions of a saved pipeline the name of its file",
    classLoaderGroup = "hop-ai")
public class AiAdvisorPipelineSavedExtension implements IExtensionPoint<Object> {
  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object subject)
      throws HopException {
    saved(subject);
  }

  /** Rename the sessions of the saved pipeline or workflow after it. */
  static void saved(Object subject) {
    String name;
    if (subject instanceof PipelineMeta pipelineMeta) {
      name = pipelineMeta.getName();
    } else if (subject instanceof WorkflowMeta workflowMeta) {
      name = workflowMeta.getName();
    } else {
      return;
    }
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return;
    }
    AiAdvisorSessionStore.get(hopGui).artifactSaved(subject, name);
  }
}
