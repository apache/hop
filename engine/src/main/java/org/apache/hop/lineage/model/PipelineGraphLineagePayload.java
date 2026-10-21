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

package org.apache.hop.lineage.model;

import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * The pipeline's hop graph: which transform feeds which, as observed when the run started.
 *
 * <p>A sink needs this to attribute lineage to the transform that actually touched a dataset.
 * Dataset lineage is derived through jobs — a job's inputs are paired with its outputs — so a sink
 * that files a whole pipeline's I/O under one job can only express the full input × output product,
 * not the true pairs. With the graph a sink can give each transform's job the datasets it consumes
 * (its own reads plus what its upstream transforms produced) and its own outputs.
 *
 * <p>Only enabled hops with two known ends are listed, in pipeline order. Informational hops are
 * included: data flows through them, even though they do no I/O of their own.
 *
 * <p>The graph is emitted with the pipeline's {@code STARTED} event, which fires after the
 * transform threads have started. A transform's own events, including early I/O, can therefore
 * arrive before the graph, even in an earlier batch. Collect a run's events by the pipeline's log
 * channel id (transform events carry it in the {@code pipelineLogChannelId} attribute) and resolve
 * them when the run finishes, rather than assuming the graph comes first.
 */
@Getter
public final class PipelineGraphLineagePayload implements LineagePayload {

  /** One enabled hop: the transform named {@code from} feeds the transform named {@code to}. */
  @Getter
  public static final class Transition {
    private final String from;
    private final String to;

    public Transition(String from, String to) {
      this.from = Objects.requireNonNull(from, "from");
      this.to = Objects.requireNonNull(to, "to");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Transition that = (Transition) o;
      return from.equals(that.from) && to.equals(that.to);
    }

    @Override
    public int hashCode() {
      return Objects.hash(from, to);
    }

    @Override
    public String toString() {
      return from + ">" + to;
    }
  }

  private final List<Transition> transitions;

  public PipelineGraphLineagePayload(List<Transition> transitions) {
    this.transitions = List.copyOf(Objects.requireNonNull(transitions, "transitions"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineGraphLineagePayload that = (PipelineGraphLineagePayload) o;
    return transitions.equals(that.transitions);
  }

  @Override
  public int hashCode() {
    return transitions.hashCode();
  }
}
