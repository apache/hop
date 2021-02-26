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

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class DataProbeLocation {

  @HopMetadataProperty
  private String sourcePipelineFilename;

  @HopMetadataProperty
  private String sourceTransformName;

  public DataProbeLocation() {
  }

  public DataProbeLocation( String sourcePipelineFilename, String sourceTransformName ) {
    this.sourcePipelineFilename = sourcePipelineFilename;
    this.sourceTransformName = sourceTransformName;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    DataProbeLocation that = (DataProbeLocation) o;
    return Objects.equals( sourcePipelineFilename, that.sourcePipelineFilename ) && Objects.equals( sourceTransformName, that.sourceTransformName );
  }

  @Override public int hashCode() {
    return Objects.hash( sourcePipelineFilename, sourceTransformName );
  }

  /**
   * Gets sourcePipelineFilename
   *
   * @return value of sourcePipelineFilename
   */
  public String getSourcePipelineFilename() {
    return sourcePipelineFilename;
  }

  /**
   * @param sourcePipelineFilename The sourcePipelineFilename to set
   */
  public void setSourcePipelineFilename( String sourcePipelineFilename ) {
    this.sourcePipelineFilename = sourcePipelineFilename;
  }

  /**
   * Gets sourceTransformName
   *
   * @return value of sourceTransformName
   */
  public String getSourceTransformName() {
    return sourceTransformName;
  }

  /**
   * @param sourceTransformName The sourceTransformName to set
   */
  public void setSourceTransformName( String sourceTransformName ) {
    this.sourceTransformName = sourceTransformName;
  }
}
