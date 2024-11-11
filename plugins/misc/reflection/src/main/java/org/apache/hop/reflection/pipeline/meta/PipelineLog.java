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
public class PipelineLog extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private boolean loggingParentsOnly;
  @HopMetadataProperty private String pipelineFilename;
  @HopMetadataProperty private boolean executingAtStart;
  @HopMetadataProperty private boolean executingPeriodically;
  @HopMetadataProperty private String intervalInSeconds;
  @HopMetadataProperty private boolean executingAtEnd;
  @HopMetadataProperty private List<PipelineToLogLocation> pipelinesToLog;

  public PipelineLog() {
    enabled = true;
    loggingParentsOnly = true;
    executingAtStart = true;
    executingPeriodically = false;
    intervalInSeconds = "30";
    executingAtEnd = true;
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

  /**
   * Gets enabled
   *
   * @return value of enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled The enabled to set
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Gets loggingParentsOnly
   *
   * @return value of loggingParentsOnly
   */
  public boolean isLoggingParentsOnly() {
    return loggingParentsOnly;
  }

  /**
   * @param loggingParentsOnly The loggingParentsOnly to set
   */
  public void setLoggingParentsOnly(boolean loggingParentsOnly) {
    this.loggingParentsOnly = loggingParentsOnly;
  }

  /**
   * Gets pipelineFilename
   *
   * @return value of pipelineFilename
   */
  public String getPipelineFilename() {
    return pipelineFilename;
  }

  /**
   * @param pipelineFilename The pipelineFilename to set
   */
  public void setPipelineFilename(String pipelineFilename) {
    this.pipelineFilename = pipelineFilename;
  }

  /**
   * Gets executingAtStart
   *
   * @return value of executingAtStart
   */
  public boolean isExecutingAtStart() {
    return executingAtStart;
  }

  /**
   * @param executingAtStart The executingAtStart to set
   */
  public void setExecutingAtStart(boolean executingAtStart) {
    this.executingAtStart = executingAtStart;
  }

  /**
   * Gets executingPeriodically
   *
   * @return value of executingPeriodically
   */
  public boolean isExecutingPeriodically() {
    return executingPeriodically;
  }

  /**
   * @param executingPeriodically The executingPeriodically to set
   */
  public void setExecutingPeriodically(boolean executingPeriodically) {
    this.executingPeriodically = executingPeriodically;
  }

  /**
   * Gets intervalInSeconds
   *
   * @return value of intervalInSeconds
   */
  public String getIntervalInSeconds() {
    return intervalInSeconds;
  }

  /**
   * @param intervalInSeconds The intervalInSeconds to set
   */
  public void setIntervalInSeconds(String intervalInSeconds) {
    this.intervalInSeconds = intervalInSeconds;
  }

  /**
   * Gets executingAtEnd
   *
   * @return value of executingAtEnd
   */
  public boolean isExecutingAtEnd() {
    return executingAtEnd;
  }

  /**
   * @param executingAtEnd The executingAtEnd to set
   */
  public void setExecutingAtEnd(boolean executingAtEnd) {
    this.executingAtEnd = executingAtEnd;
  }

  public List<PipelineToLogLocation> getPipelinesToLog() {
    return pipelinesToLog;
  }

  public void setPipelinesToLog(List<PipelineToLogLocation> pipelinesToLog) {
    this.pipelinesToLog = pipelinesToLog;
  }
}
