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

package org.apache.hop.spark.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.metric.SQLMetricInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.Option;
import scala.Tuple4;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Driver-side {@link SparkListener} that turns Spark's own per-task SQL metrics into Hop {@link
 * SparkTransformMetricSlice}s for native Dataset stages.
 *
 * <p>{@link SparkNativeMetrics#track} inserts a named {@code CollectMetrics} node ({@code
 * Dataset.observe}) as an anchor for each transform. This listener resolves that anchor in the
 * physical plan to the accumulator that counts its rows, then reads the per-task value of that
 * accumulator from task events: the partition index, host and launch/finish time come from the
 * {@link TaskInfo}. Nothing is inserted into the Dataset lineage, so whole-stage codegen and column
 * pruning stay intact.
 *
 * <p>Row counts for nodes directly below the anchor come from the node's {@code number of output
 * rows} metric. Nodes that consume a shuffle (Sort, Coalesce, …) carry no such metric, so their
 * anchor is bound to the consuming stage instead (identified by the node's other metric ids) and
 * the task's shuffle-read record count is used. Slices are absolute snapshots merged with max
 * semantics in the accumulator, so a stage that runs twice (range-partition sampling before a sort)
 * does not double count.
 */
public class SparkNativeMetricsListener extends SparkListener {

  static final String TOKEN_PREFIX = "hop_metrics_";
  private static final String NUM_OUTPUT_ROWS = "number of output rows";
  private static final String SHUFFLE_READ_RECORDS = "internal.metrics.shuffle.read.recordsRead";
  private static final Set<String> SHUFFLE_NODES =
      Set.of(
          "Exchange",
          "ShuffleQueryStage",
          "AQEShuffleRead",
          "CustomShuffleReader",
          "BroadcastExchange",
          "BroadcastQueryStage");
  private static final Set<String> PASS_THROUGH_NODES =
      Set.of("WholeStageCodegen", "InputAdapter", "CollectMetrics", "AdaptiveSparkPlan");

  private final SparkTransformMetricsAccumulator sink;
  private final String prefix;
  private final AtomicInteger sequence = new AtomicInteger();

  /** observe() token → registration. */
  private final Map<String, Registration> registrations = new ConcurrentHashMap<>();

  /** accumulator id of a "number of output rows" metric → registrations counted by it. */
  private final Map<Long, List<Registration>> directIds = new ConcurrentHashMap<>();

  /** accumulator id of any metric of a shuffle-consuming node → registrations counted by it. */
  private final Map<Long, List<Registration>> shuffleMarkerIds = new ConcurrentHashMap<>();

  /** taskId → task info (for live updates that carry no TaskInfo). */
  private final Map<Long, TaskInfo> tasks = new ConcurrentHashMap<>();

  public SparkNativeMetricsListener(SparkTransformMetricsAccumulator sink) {
    this.sink = sink;
    this.prefix = TOKEN_PREFIX + UUID.randomUUID().toString().substring(0, 8) + "_";
  }

  /** Register a transform and return the unique observation name to anchor it with. */
  public String register(String transformName, SparkNativeMetrics.Role role) {
    String token = prefix + sequence.incrementAndGet();
    registrations.put(token, new Registration(transformName, role));
    return token;
  }

  public void addTo(SparkContext sparkContext) {
    sparkContext.addSparkListener(this);
  }

  public void removeFrom(SparkContext sparkContext) {
    try {
      sparkContext.removeSparkListener(this);
    } catch (Exception ignored) {
      // context may already be stopped
    }
  }

  /** Block until queued listener events are delivered so final counts are visible. */
  public void flush(SparkContext sparkContext, long timeoutMs) {
    try {
      sparkContext.listenerBus().waitUntilEmpty(timeoutMs);
    } catch (Exception ignored) {
      // best effort; a late event only delays the last snapshot
    }
  }

  // ---- plan resolution --------------------------------------------------------------------

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (event instanceof SparkListenerSQLExecutionStart start) {
      resolvePlan(start.sparkPlanInfo());
    } else if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate update) {
      resolvePlan(update.sparkPlanInfo());
    }
  }

  void resolvePlan(SparkPlanInfo root) {
    if (root == null) {
      return;
    }
    List<SparkPlanInfo> stack = new ArrayList<>();
    stack.add(root);
    while (!stack.isEmpty()) {
      SparkPlanInfo node = stack.remove(stack.size() - 1);
      Registration registration = anchorOf(node);
      if (registration != null) {
        bind(registration, node);
      }
      stack.addAll(children(node));
    }
  }

  private Registration anchorOf(SparkPlanInfo node) {
    if (!"CollectMetrics".equals(node.nodeName())) {
      return null;
    }
    // simpleString: "CollectMetrics <name>, [<aggregates>]"
    String s = node.simpleString();
    int from = s.indexOf(prefix);
    if (from < 0) {
      return null;
    }
    int to = s.indexOf(',', from);
    String token = to < 0 ? s.substring(from) : s.substring(from, to);
    return registrations.get(token.trim());
  }

  /**
   * Walk down the single-child chain below an anchor: bind to the first "number of output rows"
   * metric, or, when a shuffle boundary is reached first, to the metrics of the consuming node.
   */
  private void bind(Registration registration, SparkPlanInfo anchor) {
    List<Long> markers = new ArrayList<>();
    SparkPlanInfo node = firstChild(anchor);
    while (node != null) {
      Long numOutputRows = metricId(node, NUM_OUTPUT_ROWS);
      if (numOutputRows != null) {
        directIds.computeIfAbsent(numOutputRows, k -> new ArrayList<>()).add(registration);
        return;
      }
      if (SHUFFLE_NODES.contains(node.nodeName())) {
        if (markers.isEmpty()) {
          // No consumer metrics between anchor and shuffle: attribute to the map side instead
          node = firstChild(node);
          continue;
        }
        for (Long id : markers) {
          shuffleMarkerIds.computeIfAbsent(id, k -> new ArrayList<>()).add(registration);
        }
        return;
      }
      if (!isPassThrough(node)) {
        for (SQLMetricInfo metric : metrics(node)) {
          markers.add(metric.accumulatorId());
        }
      }
      node = firstChild(node);
    }
  }

  /** Wrapper nodes whose metrics (e.g. codegen "duration") say nothing about row flow. */
  private static boolean isPassThrough(SparkPlanInfo node) {
    // nodeName carries a suffix for codegen stages: "WholeStageCodegen (2)"
    for (String name : PASS_THROUGH_NODES) {
      if (node.nodeName().startsWith(name)) {
        return true;
      }
    }
    return false;
  }

  private static SparkPlanInfo firstChild(SparkPlanInfo node) {
    List<SparkPlanInfo> children = children(node);
    return children.size() == 1 ? children.get(0) : null;
  }

  private static List<SparkPlanInfo> children(SparkPlanInfo node) {
    return CollectionConverters.asJava(node.children());
  }

  private static List<SQLMetricInfo> metrics(SparkPlanInfo node) {
    return CollectionConverters.asJava(node.metrics());
  }

  private static Long metricId(SparkPlanInfo node, String name) {
    for (SQLMetricInfo metric : metrics(node)) {
      if (name.equals(metric.name())) {
        return metric.accumulatorId();
      }
    }
    return null;
  }

  // ---- task events ------------------------------------------------------------------------

  @Override
  public void onTaskStart(SparkListenerTaskStart taskStart) {
    TaskInfo info = taskStart.taskInfo();
    if (info != null) {
      tasks.put(info.taskId(), info);
    }
  }

  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate update) {
    for (Tuple4<Object, Object, Object, scala.collection.immutable.Seq<AccumulableInfo>> entry :
        CollectionConverters.asJava(update.accumUpdates())) {
      long taskId = ((Number) entry._1()).longValue();
      TaskInfo info = tasks.get(taskId);
      if (info != null) {
        report(info, CollectionConverters.asJava(entry._4()), false);
      }
    }
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    TaskInfo info = taskEnd.taskInfo();
    if (info == null) {
      return;
    }
    tasks.remove(info.taskId());
    if (info.successful()) {
      report(info, CollectionConverters.asJava(info.accumulables()), true);
    }
  }

  private void report(TaskInfo info, List<AccumulableInfo> accumulables, boolean finished) {
    if (accumulables == null || accumulables.isEmpty()) {
      return;
    }
    Map<Long, Long> byId = new HashMap<>();
    long shuffleReadRecords = -1;
    for (AccumulableInfo acc : accumulables) {
      Long value = longValue(acc.update());
      if (value == null) {
        continue;
      }
      byId.put(acc.id(), value);
      if (acc.name().isDefined() && SHUFFLE_READ_RECORDS.equals(acc.name().get())) {
        shuffleReadRecords = value;
      }
    }
    Map<Registration, Long> counts = new HashMap<>();
    for (Map.Entry<Long, Long> e : byId.entrySet()) {
      List<Registration> direct = directIds.get(e.getKey());
      if (direct != null) {
        for (Registration r : direct) {
          counts.merge(r, e.getValue(), Math::max);
        }
      }
      List<Registration> viaShuffle = shuffleMarkerIds.get(e.getKey());
      if (viaShuffle != null && shuffleReadRecords >= 0) {
        for (Registration r : viaShuffle) {
          counts.merge(r, shuffleReadRecords, Math::max);
        }
      }
    }
    for (Map.Entry<Registration, Long> e : counts.entrySet()) {
      sink.add(slice(e.getKey(), info, e.getValue(), finished));
    }
  }

  private static Long longValue(Option<Object> update) {
    if (update == null || update.isEmpty()) {
      return null;
    }
    Object v = update.get();
    return v instanceof Number n ? n.longValue() : null;
  }

  private static SparkTransformMetricSlice slice(
      Registration registration, TaskInfo info, long count, boolean finished) {
    long read = 0;
    long written = 0;
    long input = 0;
    long output = 0;
    switch (registration.role()) {
      case INPUT -> {
        input = count;
        written = count;
      }
      case OUTPUT -> {
        read = count;
        output = count;
      }
      default -> {
        read = count;
        written = count;
      }
    }
    return new SparkTransformMetricSlice(
        registration.transformName(),
        info.index(),
        info.host(),
        read,
        written,
        input,
        output,
        0,
        !finished,
        finished,
        info.launchTime(),
        finished ? info.finishTime() : 0L);
  }

  /** A transform anchored by one observe() token. */
  record Registration(String transformName, SparkNativeMetrics.Role role) {}
}
