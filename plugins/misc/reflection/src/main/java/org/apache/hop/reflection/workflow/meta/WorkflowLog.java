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
 *
 */

package org.apache.hop.reflection.workflow.meta;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "workflow-log",
    name = "i18n::WorkflowLog.name",
    description = "i18n::WorkflowLog.description",
    image = "workflow-log.svg",
    category = HopMetadataCategory.LOGGING,
    documentationUrl = "/metadata-types/workflow-log.html",
    hopMetadataPropertyType = HopMetadataPropertyType.WORKFLOW_LOG)
@Getter
@Setter
public class WorkflowLog extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private boolean loggingParentsOnly;
  @HopMetadataProperty private boolean failParentOnLoggingFailure;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_FILE)
  private String pipelineFilename;

  @HopMetadataProperty private boolean executingAtStart;
  @HopMetadataProperty private boolean executingPeriodically;
  @HopMetadataProperty private String intervalInSeconds;
  @HopMetadataProperty private boolean executingAtEnd;

  @HopMetadataProperty(storeWithCode = true)
  private LogLevel logLevel;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.WORKFLOW_FILE)
  private List<String> workflowToLog;

  public WorkflowLog() {
    enabled = true;
    loggingParentsOnly = false;
    failParentOnLoggingFailure = false;
    executingAtStart = true;
    executingPeriodically = false;
    intervalInSeconds = "30";
    executingAtEnd = true;
    logLevel = LogLevel.ERROR;
    workflowToLog = new ArrayList<>();
  }

  public WorkflowLog(String name) {
    super(name);
    workflowToLog = new ArrayList<>();
    failParentOnLoggingFailure = false;
  }

  public WorkflowLog(
      String name,
      boolean enabled,
      boolean loggingParentsOnly,
      String pipelineFilename,
      boolean executingAtStart,
      boolean executingPeriodically,
      String intervalInSeconds,
      boolean executingAtEnd,
      List<String> workflowToLog,
      boolean failParentOnLoggingFailure) {
    super(name);
    this.enabled = enabled;
    this.loggingParentsOnly = loggingParentsOnly;
    this.pipelineFilename = pipelineFilename;
    this.executingAtStart = executingAtStart;
    this.executingPeriodically = executingPeriodically;
    this.intervalInSeconds = intervalInSeconds;
    this.executingAtEnd = executingAtEnd;
    this.workflowToLog = workflowToLog;
    this.failParentOnLoggingFailure = failParentOnLoggingFailure;
  }
}
