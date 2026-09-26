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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class PipelineMetaInfo extends AbstractMetaInfo {
  /**
   * The version string for the pipeline.
   *
   * @deprecated since 2.20, for removal. The label has to be set and updated by hand and says
   *     nothing that the version control system holding the file does not already say more
   *     reliably. It is still read from and written to the file, so no existing value is lost
   *     before it is removed.
   */
  @Deprecated(since = "2.20", forRemoval = true)
  @HopMetadataProperty(key = "pipeline_version")
  protected String pipelineVersion;

  /** Whether the pipeline is capturing transform performance snapshots. */
  @HopMetadataProperty(key = "capture_transform_performance")
  protected boolean capturingTransformPerformanceSnapShots;

  /** The transform performance capturing delay. */
  @HopMetadataProperty(key = "transform_performance_capturing_delay")
  protected long transformPerformanceCapturingDelay;

  /** The transform performance capturing size limit. */
  @HopMetadataProperty(key = "transform_performance_capturing_size_limit")
  protected String transformPerformanceCapturingSizeLimit;

  /**
   * The status of the pipeline: {@code 1} for draft, {@code 2} for production, {@code -1} when
   * unset.
   *
   * @deprecated since 2.20, for removal. Nothing in Hop acts on the value, it has to be maintained
   *     by hand, and it is left at {@code -1} in practice. It is still read from and written to the
   *     file, so no existing value is lost before it is removed.
   */
  @Deprecated(since = "2.20", forRemoval = true)
  @HopMetadataProperty(key = "pipeline_status")
  protected int pipelineStatus;

  @HopMetadataProperty(key = "parameters")
  protected NamedParameters namedParams = new NamedParameters();

  public PipelineMetaInfo() {
    super();
    this.capturingTransformPerformanceSnapShots = false;
    this.transformPerformanceCapturingDelay = 1000; // every 1 seconds
    this.transformPerformanceCapturingSizeLimit = "100"; // maximum 100 data points
  }
}
