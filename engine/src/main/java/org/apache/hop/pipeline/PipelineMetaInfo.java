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
 *
 */

package org.apache.hop.pipeline;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class PipelineMetaInfo extends AbstractMetaInfo {

  /** The version string for the pipeline. */
  @HopMetadataProperty(key = "pipeline_version")
  protected String pipelineVersion;

  /** Whether the pipeline is capturing transform performance snap shots. */
  @HopMetadataProperty(key = "capture_transform_performance")
  protected boolean capturingTransformPerformanceSnapShots;

  /** The transform performance capturing delay. */
  @HopMetadataProperty(key = "transform_performance_capturing_delay")
  protected long transformPerformanceCapturingDelay;

  /** The transform performance capturing size limit. */
  @HopMetadataProperty(key = "transform_performance_capturing_size_limit")
  protected String transformPerformanceCapturingSizeLimit;

  /** The pipeline type. */
  @HopMetadataProperty(key = "pipeline_type")
  protected PipelineMeta.PipelineType pipelineType;

  public PipelineMetaInfo() {
    super();
    this.capturingTransformPerformanceSnapShots = false;
    this.transformPerformanceCapturingDelay = 1000; // every 1 seconds
    this.transformPerformanceCapturingSizeLimit = "100"; // maximum 100 data points
    this.pipelineType = PipelineMeta.PipelineType.Normal;
  }

  /**
   * Gets pipelineVersion
   *
   * @return value of pipelineVersion
   */
  public String getPipelineVersion() {
    return pipelineVersion;
  }

  /**
   * Sets pipelineVersion
   *
   * @param pipelineVersion value of pipelineVersion
   */
  public void setPipelineVersion(String pipelineVersion) {
    this.pipelineVersion = pipelineVersion;
  }

  /**
   * Gets capturingTransformPerformanceSnapShots
   *
   * @return value of capturingTransformPerformanceSnapShots
   */
  public boolean isCapturingTransformPerformanceSnapShots() {
    return capturingTransformPerformanceSnapShots;
  }

  /**
   * Sets capturingTransformPerformanceSnapShots
   *
   * @param capturingTransformPerformanceSnapShots value of capturingTransformPerformanceSnapShots
   */
  public void setCapturingTransformPerformanceSnapShots(
      boolean capturingTransformPerformanceSnapShots) {
    this.capturingTransformPerformanceSnapShots = capturingTransformPerformanceSnapShots;
  }

  /**
   * Gets transformPerformanceCapturingDelay
   *
   * @return value of transformPerformanceCapturingDelay
   */
  public long getTransformPerformanceCapturingDelay() {
    return transformPerformanceCapturingDelay;
  }

  /**
   * Sets transformPerformanceCapturingDelay
   *
   * @param transformPerformanceCapturingDelay value of transformPerformanceCapturingDelay
   */
  public void setTransformPerformanceCapturingDelay(long transformPerformanceCapturingDelay) {
    this.transformPerformanceCapturingDelay = transformPerformanceCapturingDelay;
  }

  /**
   * Gets transformPerformanceCapturingSizeLimit
   *
   * @return value of transformPerformanceCapturingSizeLimit
   */
  public String getTransformPerformanceCapturingSizeLimit() {
    return transformPerformanceCapturingSizeLimit;
  }

  /**
   * Sets transformPerformanceCapturingSizeLimit
   *
   * @param transformPerformanceCapturingSizeLimit value of transformPerformanceCapturingSizeLimit
   */
  public void setTransformPerformanceCapturingSizeLimit(
      String transformPerformanceCapturingSizeLimit) {
    this.transformPerformanceCapturingSizeLimit = transformPerformanceCapturingSizeLimit;
  }

  /**
   * Gets pipelineType
   *
   * @return value of pipelineType
   */
  public PipelineMeta.PipelineType getPipelineType() {
    return pipelineType;
  }

  /**
   * Sets pipelineType
   *
   * @param pipelineType value of pipelineType
   */
  public void setPipelineType(PipelineMeta.PipelineType pipelineType) {
    this.pipelineType = pipelineType;
  }
}
