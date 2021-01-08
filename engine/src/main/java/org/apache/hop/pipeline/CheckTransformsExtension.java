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

package org.apache.hop.pipeline;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

public class CheckTransformsExtension {
  private final List<ICheckResult> remarks;
  private final IVariables variables;
  private final PipelineMeta pipelineMeta;
  private final TransformMeta[] transformMetas;
  private final IHopMetadataProvider metadataProvider;

  public CheckTransformsExtension(
    List<ICheckResult> remarks,
    IVariables variables,
    PipelineMeta pipelineMeta,
    TransformMeta[] transformMetas,
    IHopMetadataProvider metadataProvider ) {
    this.remarks = remarks;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.transformMetas = transformMetas;
    this.metadataProvider = metadataProvider;
  }

  public List<ICheckResult> getRemarks() {
    return remarks;
  }

  public IVariables getVariableSpace() {
    return variables;
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public TransformMeta[] getTransformMetas() {
    return transformMetas;
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }
}
