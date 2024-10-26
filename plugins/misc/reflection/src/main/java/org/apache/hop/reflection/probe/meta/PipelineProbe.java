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

package org.apache.hop.reflection.probe.meta;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "pipeline-probe",
    name = "i18n::PipelineProbe.name",
    description = "i18n::PipelineProbe.desciption",
    image = "probe.svg",
    documentationUrl = "/metadata-types/pipeline-probe.html",
    hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_PROBE)
public class PipelineProbe extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private boolean enabled;
  @HopMetadataProperty private String pipelineFilename;
  @HopMetadataProperty private List<DataProbeLocation> dataProbeLocations;

  public PipelineProbe() {
    enabled = true;
    dataProbeLocations = new ArrayList<>();
  }

  public PipelineProbe(String name) {
    super(name);
    dataProbeLocations = new ArrayList<>();
  }

  public PipelineProbe(
      String name,
      boolean enabled,
      String pipelineFilename,
      List<DataProbeLocation> dataProbeLocations) {
    super(name);
    this.enabled = enabled;
    this.pipelineFilename = pipelineFilename;
    this.dataProbeLocations = dataProbeLocations;
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
   * Gets dataProbeLocations
   *
   * @return value of dataProbeLocations
   */
  public List<DataProbeLocation> getDataProbeLocations() {
    return dataProbeLocations;
  }

  /**
   * @param dataProbeLocations The dataProbeLocations to set
   */
  public void setDataProbeLocations(List<DataProbeLocation> dataProbeLocations) {
    this.dataProbeLocations = dataProbeLocations;
  }
}
