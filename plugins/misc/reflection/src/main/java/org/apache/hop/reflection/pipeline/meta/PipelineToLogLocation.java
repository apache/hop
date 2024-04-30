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

package org.apache.hop.reflection.pipeline.meta;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class PipelineToLogLocation {

  @HopMetadataProperty private String pipelineToLogFilename;

  public PipelineToLogLocation() {}

  public PipelineToLogLocation(String pipelineTologFilename) {
    this.pipelineToLogFilename = pipelineTologFilename;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipelineToLogLocation that = (PipelineToLogLocation) o;
    return Objects.equals(pipelineToLogFilename, that.pipelineToLogFilename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipelineToLogFilename);
  }

  public String getPipelineToLogFilename() {
    return pipelineToLogFilename;
  }

  public void setPipelineToLogFilename(String pipelineToLogFilename) {
    this.pipelineToLogFilename = pipelineToLogFilename;
  }
}
