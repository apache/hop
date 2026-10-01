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

package org.apache.hop.pipeline.analysis;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * A bounded-buffer deadlock risk found by {@link PipelineBufferDeadlockAnalyzer}: a multi-input
 * transform where at least two inbound predecessors share a common ancestor (split–rejoin).
 *
 * @param reconvergence the multi-input transform where streams rejoin
 * @param commonAncestor a transform that can reach more than one inbound predecessor
 * @param inboundPredecessors predecessors of {@code reconvergence} involved in the risk
 * @param spillHops hops {@code from → reconvergence} recommended for a spilling rowset (v1: all
 *     inbound risky hops)
 */
public record BufferDeadlockRisk(
    TransformMeta reconvergence,
    TransformMeta commonAncestor,
    List<TransformMeta> inboundPredecessors,
    Set<SpillHop> spillHops) {

  public BufferDeadlockRisk {
    Objects.requireNonNull(reconvergence, "reconvergence");
    Objects.requireNonNull(commonAncestor, "commonAncestor");
    inboundPredecessors = List.copyOf(inboundPredecessors);
    spillHops = Set.copyOf(spillHops);
  }

  /** One pipeline hop identified by transform names (copy-agnostic). */
  public record SpillHop(String fromTransformName, String toTransformName) {
    public SpillHop {
      Objects.requireNonNull(fromTransformName, "fromTransformName");
      Objects.requireNonNull(toTransformName, "toTransformName");
    }

    public boolean matches(String from, String to) {
      return fromTransformName.equalsIgnoreCase(from) && toTransformName.equalsIgnoreCase(to);
    }

    @Override
    public String toString() {
      return fromTransformName + " → " + toTransformName;
    }
  }

  public String formatMessage() {
    String preds =
        inboundPredecessors.stream().map(TransformMeta::getName).collect(Collectors.joining(", "));
    String hops = spillHops.stream().map(SpillHop::toString).collect(Collectors.joining(", "));
    return "Possible buffer deadlock at transform '"
        + reconvergence.getName()
        + "': inputs ["
        + preds
        + "] share common ancestor '"
        + commonAncestor.getName()
        + "'. Recommended spill hops: "
        + hops;
  }
}
