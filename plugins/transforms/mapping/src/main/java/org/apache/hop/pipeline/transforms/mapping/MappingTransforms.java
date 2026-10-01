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

package org.apache.hop.pipeline.transforms.mapping;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.input.MappingInputMeta;
import org.apache.hop.pipeline.transforms.output.MappingOutput;
import org.apache.hop.pipeline.transforms.output.MappingOutputMeta;

/** Shared helpers for locating Mapping Input / Output transforms in a child pipeline. */
public final class MappingTransforms {

  private MappingTransforms() {
    // Utility class
  }

  public static boolean isInfoMapping(MappingIODefinition definition) {
    return definition != null
        && !definition.isMainDataPath()
        && StringUtils.isNotEmpty(definition.getInputTransformName());
  }

  public static boolean isTargetMapping(MappingIODefinition definition) {
    return definition != null
        && !definition.isMainDataPath()
        && StringUtils.isNotEmpty(definition.getOutputTransformName());
  }

  public static List<MappingInput> findMappingInputs(Pipeline mappingPipeline) {
    List<MappingInput> list = new ArrayList<>();
    if (mappingPipeline == null) {
      return list;
    }
    for (IEngineComponent component : mappingPipeline.getComponents()) {
      if (component instanceof MappingInput mappingInput) {
        list.add(mappingInput);
      }
    }
    return list;
  }

  public static List<MappingOutput> findMappingOutputs(Pipeline mappingPipeline) {
    List<MappingOutput> list = new ArrayList<>();
    if (mappingPipeline == null) {
      return list;
    }
    for (IEngineComponent component : mappingPipeline.getComponents()) {
      if (component instanceof MappingOutput mappingOutput) {
        list.add(mappingOutput);
      }
    }
    return list;
  }

  public static List<TransformMeta> findMappingInputMetas(PipelineMeta pipelineMeta) {
    List<TransformMeta> list = new ArrayList<>();
    if (pipelineMeta == null) {
      return list;
    }
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      if (transformMeta.getTransform() instanceof MappingInputMeta) {
        list.add(transformMeta);
      }
    }
    return list;
  }

  public static List<TransformMeta> findMappingOutputMetas(PipelineMeta pipelineMeta) {
    List<TransformMeta> list = new ArrayList<>();
    if (pipelineMeta == null) {
      return list;
    }
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      if (transformMeta.getTransform() instanceof MappingOutputMeta) {
        list.add(transformMeta);
      }
    }
    return list;
  }

  public static MappingInput findMappingInput(Pipeline mappingPipeline, String transformName) {
    for (MappingInput mappingInput : findMappingInputs(mappingPipeline)) {
      if (Utils.isEmpty(transformName) || transformName.equals(mappingInput.getTransformName())) {
        return mappingInput;
      }
    }
    return null;
  }

  public static MappingOutput findMappingOutput(Pipeline mappingPipeline, String transformName) {
    for (MappingOutput mappingOutput : findMappingOutputs(mappingPipeline)) {
      if (Utils.isEmpty(transformName) || transformName.equals(mappingOutput.getTransformName())) {
        return mappingOutput;
      }
    }
    return null;
  }
}
