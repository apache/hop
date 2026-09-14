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

package org.apache.hop.workflow;

import java.util.List;
import lombok.Getter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionMeta;

/** Payload for {@code AfterCheckActions}. */
@Getter
public class CheckActionsExtension {
  private final List<ICheckResult> remarks;
  private final IVariables variables;
  private final WorkflowMeta workflowMeta;
  private final List<ActionMeta> actions;
  private final IHopMetadataProvider metadataProvider;

  public CheckActionsExtension(
      List<ICheckResult> remarks,
      IVariables variables,
      WorkflowMeta workflowMeta,
      List<ActionMeta> actions,
      IHopMetadataProvider metadataProvider) {
    this.remarks = remarks;
    this.variables = variables;
    this.workflowMeta = workflowMeta;
    this.actions = actions;
    this.metadataProvider = metadataProvider;
  }
}
