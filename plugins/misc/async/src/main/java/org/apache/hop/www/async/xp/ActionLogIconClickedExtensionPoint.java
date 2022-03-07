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

package org.apache.hop.www.async.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.www.async.AsyncWebService;

@ExtensionPoint(
    id = "ActionLogIconClickedExtensionPoint",
    extensionPointId = "WorkflowGraphMouseUp",
    description = "Edit the async web service associated with the logging on click & mouse up")
public class ActionLogIconClickedExtensionPoint
    implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension extension)
      throws HopException {

    HopGuiWorkflowGraph workflowGraph = extension.getWorkflowGraph();

    AreaOwner areaOwner = extension.getAreaOwner();
    if (areaOwner == null) {
      return;
    }
    if (areaOwner.getAreaType() == AreaOwner.AreaType.CUSTOM) {
      if (areaOwner.getOwner() == null) {
        return;
      }
      if (areaOwner.getOwner() instanceof String) {
        String message = (String) areaOwner.getOwner();
        if (message.startsWith(DrawAsyncLoggingIconExtensionPoint.STRING_AREA_OWNER_PREFIX)) {
          String serviceName =
              message.substring(
                  DrawAsyncLoggingIconExtensionPoint.STRING_AREA_OWNER_PREFIX.length());
          if (StringUtils.isNotEmpty(serviceName)) {
            MultiMetadataProvider metadataProvider =
                workflowGraph.getHopGui().getMetadataProvider();
            IHopMetadataSerializer<AsyncWebService> serializer =
                metadataProvider.getSerializer(AsyncWebService.class);
            if (serializer.exists(serviceName)) {
              MetadataManager<AsyncWebService> manager =
                  new MetadataManager<>(
                      workflowGraph.getVariables(), metadataProvider, AsyncWebService.class);
              manager.editMetadata(serviceName);
              extension.setPreventingDefault(true);
            }
          }
        }
      }
    }
  }
}
