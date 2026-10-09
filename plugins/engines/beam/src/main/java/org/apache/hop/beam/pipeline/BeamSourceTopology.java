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

package org.apache.hop.beam.pipeline;

import java.util.List;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Beam source handlers are called with a null predecessor list and a null input collection. A
 * non-null list is the caller's explicit topology. A null list is resolved from enabled row hops,
 * including error hops and excluding informational hops.
 */
public final class BeamSourceTopology {
  private BeamSourceTopology() {}

  public static void rejectIncomingRows(
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      List<TransformMeta> previousTransforms,
      PCollection<?> input,
      String message)
      throws HopException {
    if (input != null) {
      throw new HopException(message);
    }
    List<TransformMeta> predecessors = previousTransforms;
    if (predecessors == null && pipelineMeta != null && transformMeta != null) {
      predecessors = pipelineMeta.findPreviousTransforms(transformMeta, false);
    }
    if (predecessors != null && !predecessors.isEmpty()) {
      throw new HopException(message);
    }
  }
}
