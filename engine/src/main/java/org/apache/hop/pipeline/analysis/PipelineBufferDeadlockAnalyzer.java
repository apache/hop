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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.analysis.BufferDeadlockRisk.SpillHop;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Static analyzer for classic multi-threaded pipeline buffer deadlocks.
 *
 * <p>Hop hop graphs are DAGs ({@link PipelineMeta#hasLoop}); the hangs documented for Stream Lookup
 * / Merge Join are <em>bounded-buffer wait-for cycles</em> on split–rejoin topologies. This class
 * finds multi-input transforms whose inbound predecessors share a common ancestor and recommends
 * the minimal set of inbound hops that may need a spilling {@link org.apache.hop.core.IRowSet}.
 *
 * <p>v1 spill recommendation: every hop {@code predecessor → reconvergence} involved in a risk (not
 * info-only — the main hop into Stream Lookup must be included).
 */
public final class PipelineBufferDeadlockAnalyzer {

  private PipelineBufferDeadlockAnalyzer() {}

  /**
   * @return risks in pipeline transform order; empty if none or {@code pipelineMeta} is null
   */
  public static List<BufferDeadlockRisk> analyze(PipelineMeta pipelineMeta) {
    if (pipelineMeta == null) {
      return Collections.emptyList();
    }

    List<BufferDeadlockRisk> risks = new ArrayList<>();
    Map<TransformMeta, Set<TransformMeta>> ancestorCache = new LinkedHashMap<>();

    for (TransformMeta reconvergence : pipelineMeta.getTransforms()) {
      if (reconvergence == null) {
        continue;
      }

      // Immediate predecessors including info streams
      List<TransformMeta> preds =
          new ArrayList<>(pipelineMeta.findPreviousTransforms(reconvergence, true));
      // Deduplicate while preserving order
      preds = new ArrayList<>(new LinkedHashSet<>(preds));
      if (preds.size() < 2) {
        continue;
      }

      // For each pair, find common ancestors; collect all preds that share ancestry with another
      Set<TransformMeta> riskyPreds = new LinkedHashSet<>();
      TransformMeta chosenAncestor = null;

      for (int i = 0; i < preds.size(); i++) {
        TransformMeta pi = preds.get(i);
        if (pi == null) {
          continue;
        }
        Set<TransformMeta> ancestorsI = ancestorsIncludingSelf(pipelineMeta, pi, ancestorCache);
        for (int j = i + 1; j < preds.size(); j++) {
          TransformMeta pj = preds.get(j);
          if (pj == null) {
            continue;
          }
          Set<TransformMeta> ancestorsJ = ancestorsIncludingSelf(pipelineMeta, pj, ancestorCache);
          TransformMeta common = firstCommonAncestor(ancestorsI, ancestorsJ);
          if (common != null) {
            riskyPreds.add(pi);
            riskyPreds.add(pj);
            if (chosenAncestor == null) {
              chosenAncestor = common;
            }
          }
        }
      }

      if (riskyPreds.size() < 2 || chosenAncestor == null) {
        continue;
      }

      List<TransformMeta> inbound = new ArrayList<>(riskyPreds);
      Set<SpillHop> spillHops = new LinkedHashSet<>();
      for (TransformMeta pred : inbound) {
        spillHops.add(new SpillHop(pred.getName(), reconvergence.getName()));
      }

      risks.add(new BufferDeadlockRisk(reconvergence, chosenAncestor, inbound, spillHops));
    }

    return risks;
  }

  /**
   * Union of all recommended spill hops across risks (copy-agnostic from/to names).
   *
   * @param risks analyzer output
   * @return immutable set of spill hops
   */
  public static Set<SpillHop> collectSpillHops(List<BufferDeadlockRisk> risks) {
    if (risks == null || risks.isEmpty()) {
      return Collections.emptySet();
    }
    Set<SpillHop> hops = new LinkedHashSet<>();
    for (BufferDeadlockRisk risk : risks) {
      hops.addAll(risk.spillHops());
    }
    return Collections.unmodifiableSet(hops);
  }

  /**
   * Whether the hop from {@code fromTransform} to {@code toTransform} is in the spill set.
   *
   * @param spillHops recommended hops
   * @param fromTransform source transform name
   * @param toTransform target transform name
   * @return true if this hop should use a spilling rowset
   */
  public static boolean shouldSpill(
      Set<SpillHop> spillHops, String fromTransform, String toTransform) {
    if (spillHops == null || spillHops.isEmpty() || fromTransform == null || toTransform == null) {
      return false;
    }
    for (SpillHop hop : spillHops) {
      if (hop.matches(fromTransform, toTransform)) {
        return true;
      }
    }
    return false;
  }

  private static Set<TransformMeta> ancestorsIncludingSelf(
      PipelineMeta meta, TransformMeta transform, Map<TransformMeta, Set<TransformMeta>> cache) {
    Set<TransformMeta> cached = cache.get(transform);
    if (cached != null) {
      return cached;
    }

    Set<TransformMeta> result = new LinkedHashSet<>();
    ArrayDeque<TransformMeta> stack = new ArrayDeque<>();
    result.add(transform);
    stack.push(transform);

    while (!stack.isEmpty()) {
      TransformMeta current = stack.pop();
      for (TransformMeta prev : meta.findPreviousTransforms(current, true)) {
        if (prev != null && result.add(prev)) {
          stack.push(prev);
        }
      }
    }

    cache.put(transform, result);
    return result;
  }

  /**
   * Picks a common ancestor for messaging. Walks {@code a} in self-first BFS order and returns the
   * first member also in {@code b} (nearest shared node on that walk).
   */
  private static TransformMeta firstCommonAncestor(Set<TransformMeta> a, Set<TransformMeta> b) {
    for (TransformMeta candidate : a) {
      if (b.contains(candidate)) {
        return candidate;
      }
    }
    return null;
  }
}
