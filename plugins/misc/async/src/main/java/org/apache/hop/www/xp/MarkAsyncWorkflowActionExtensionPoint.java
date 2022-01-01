/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.www.AsyncWebService;
import org.apache.hop.www.Defaults;

import java.util.Map;

@ExtensionPoint(
    id = "MarkAsyncWorkflowActionExtensionPoint",
    extensionPointId = "WorkflowBeforeActionExecution",
    description = "Before the execution of an action, pass service to workflow data map")
public class MarkAsyncWorkflowActionExtensionPoint
    implements IExtensionPoint<WorkflowExecutionExtension> {
  @Override
  public void callExtensionPoint(
      ILogChannel iLogChannel, IVariables iVariables, WorkflowExecutionExtension ext)
      throws HopException {

    ActionMeta actionMeta = ext.actionMeta;

    // Make sure it's a pipeline action
    //
    if (!actionMeta.isPipeline()) {
      return;
    }

    // Do we have a set of status group attributes?
    //
    Map<String, String> attributesMap =
        actionMeta.getAttributesMap().get(Defaults.ASYNC_STATUS_GROUP);
    if (attributesMap == null) {
      return;
    }

    // Do we have a service name?
    //
    String serviceName = attributesMap.get(Defaults.ASYNC_ACTION_PIPELINE_SERVICE_NAME);
    if (serviceName == null) {
      return;
    }

    // Load the service...
    //
    IWorkflowEngine<WorkflowMeta> workflow = ext.workflow;

    IHopMetadataSerializer<AsyncWebService> serializer =
        workflow.getMetadataProvider().getSerializer(AsyncWebService.class);
    AsyncWebService service = serializer.load(serviceName);
    if (service == null) {
      throw new HopException("Unable to load asynchronous service " + serviceName);
    }

    // Store it in the workflow...
    //
    String key = Defaults.createWorkflowExtensionDataKey(actionMeta.getName());
    workflow.getExtensionDataMap().put(key, service);
  }
}
