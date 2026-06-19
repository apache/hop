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

package org.apache.hop.reflection.pipeline.meta;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "pipeline-log",
    name = "i18n::PipelineLog.name",
    description = "i18n::PipelineLog.description",
    image = "pipeline-log.svg",
    documentationUrl = "/metadata-types/pipeline-log.html",
    hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_LOG)
@Getter
@Setter
public class PipelineLog extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private boolean loggingParentsOnly;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_FILE)
  private String pipelineFilename;

  @HopMetadataProperty private boolean executingAtStart;
  @HopMetadataProperty private boolean executingPeriodically;
  @HopMetadataProperty private String intervalInSeconds;
  @HopMetadataProperty private boolean executingAtEnd;

  @HopMetadataProperty(storeWithCode = true)
  private LogLevel logLevel;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_FILE)
  private List<PipelineToLogLocation> pipelinesToLog;

  public PipelineLog() {
    enabled = true;
    loggingParentsOnly = false;
    executingAtStart = true;
    executingPeriodically = false;
    intervalInSeconds = "30";
    executingAtEnd = true;
    logLevel = LogLevel.ERROR;
    pipelinesToLog = new ArrayList<>();
  }

  public PipelineLog(String name) {
    super(name);
    pipelinesToLog = new ArrayList<>();
  }

  public PipelineLog(
      String name,
      boolean enabled,
      boolean loggingParentsOnly,
      String pipelineFilename,
      boolean executingAtStart,
      boolean executingPeriodically,
      String intervalInSeconds,
      boolean executingAtEnd,
      List<PipelineToLogLocation> pipelinesToLog) {
    super(name);
    this.enabled = enabled;
    this.loggingParentsOnly = loggingParentsOnly;
    this.pipelineFilename = pipelineFilename;
    this.executingAtStart = executingAtStart;
    this.executingPeriodically = executingPeriodically;
    this.intervalInSeconds = intervalInSeconds;
    this.executingAtEnd = executingAtEnd;
    this.pipelinesToLog = pipelinesToLog;
  }
}
