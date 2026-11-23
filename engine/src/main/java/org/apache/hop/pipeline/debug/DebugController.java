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

package org.apache.hop.pipeline.debug;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Condition;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;

/**
 * Steps a local pipeline one {@code processRow} at a time.
 *
 * <p>Threads block in {@link #beforeProcessRow(ITransform)} until a permit is granted or the
 * transform is set free-running. A transform with input needs an unread row, or every input closed,
 * before a permit is granted. Otherwise the call reports {@link Activity#WAITING} and the thread
 * stays parked. Try-steps rewind input cursors afterwards and leave output in the trial buffer.
 */
public class DebugController {
  private static final String CONST_DEBUG_CONTROLLER_DETAIL_PIPELINE_STOPPED =
      "DebugController.Detail.PipelineStopped";

  private static final Class<?> PKG = DebugController.class;

  /** Canvas state-map key for the transform the debugger will step. The value is the name. */
  public static final String STATE_CURRENT_TRANSFORM = "debug.currentTransform";

  /** How a step call finished. */
  public enum Activity {
    WAITING,
    STEPPED,
    FINISHED,
    TIMED_OUT,
    STOPPED,
    PAUSED
  }

  /** Result of one step request. {@code completed} is how many processRow calls finished. */
  public record StepResult(
      String transformName,
      int copy,
      Activity activity,
      int requested,
      int completed,
      String detail) {}

  /** One finished processRow while the session was stepping or running. */
  public record StepEvent(
      String transformName, int copy, boolean tried, long linesRead, long linesWritten) {}

  private static final int MAX_EVENTS = 1000;
  private static final long DEFAULT_TIMEOUT_MS = 10_000L;

  private final Pipeline pipeline;
  private final Object lock = new Object();
  private final Map<String, Integer> permits = new HashMap<>();
  private final Map<String, Integer> completed = new HashMap<>();
  private final Set<String> free = new HashSet<>();
  private final Set<String> tryStep = new HashSet<>();
  private final Set<String> inTry = new HashSet<>();
  private final Set<String> insideRow = new HashSet<>();
  private final Set<String> ended = new HashSet<>();
  private final Set<String> pausedByCondition = new HashSet<>();
  private final Deque<StepEvent> events = new ArrayDeque<>();
  private final Map<String, HopPause> hopPauses = new HashMap<>();

  private boolean timelineDirty;
  private String pauseMessage;

  /** A condition evaluated when a row enters one hop, and what that match pauses. */
  public record HopPause(Condition condition, DebugPauseScope scope) {}

  public DebugController(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public boolean isTimelineDirty() {
    return timelineDirty;
  }

  /**
   * Called by the transform thread before every {@code processRow}. Returns false when the pipeline
   * or the transform has been stopped.
   */
  public boolean beforeProcessRow(ITransform transform) {
    String key = key(transform);
    synchronized (lock) {
      while (true) {
        if (isStopped(transform)) {
          return false;
        }
        if (free.contains(key)) {
          insideRow.add(key);
          return true;
        }
        int left = permits.getOrDefault(key, 0);
        if (left > 0 && readyForInput(transform)) {
          if (left == 1) {
            permits.remove(key);
          } else {
            permits.put(key, left - 1);
          }
          if (tryStep.contains(key)) {
            inTry.add(key);
          }
          insideRow.add(key);
          lock.notifyAll();
          return true;
        }
        try {
          lock.wait(200L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  /**
   * Called by the transform thread after {@code processRow}. {@code keepGoing} is false when the
   * call returned false or threw, and that call is not counted as a processed row.
   */
  public void afterProcessRow(ITransform transform, boolean keepGoing) {
    String key = key(transform);
    synchronized (lock) {
      insideRow.remove(key);
      boolean tried = inTry.remove(key);
      if (keepGoing) {
        completed.merge(key, 1, Integer::sum);
        appendEvent(
            new StepEvent(
                transform.getTransformName(),
                transform.getCopy(),
                tried,
                transform.getLinesRead(),
                transform.getLinesWritten()));
      }
      lock.notifyAll();
    }
  }

  /** The transform returned false from {@code processRow} and will not take another row. */
  public void transformEnded(ITransform transform) {
    synchronized (lock) {
      ended.add(key(transform));
      lock.notifyAll();
    }
  }

  /** Lets the selected copy process {@code count} input rows (one source output row each). */
  public StepResult process(ITransform transform, int count) {
    return step(transform, count, false, DEFAULT_TIMEOUT_MS);
  }

  /**
   * Same as {@link #process(ITransform, int)}, then rewinds every input cursor once for the whole
   * batch. Output rows land in the trial buffer of each output hop.
   *
   * <p>Only the hop cursors move back. The transform's own fields, counters, files, and database
   * writes stay as the try left them. Sort rows, Group by, Unique rows, Add sequence, Table output,
   * and anything else that keeps state will see the same row again if it is processed for real
   * afterwards.
   */
  public StepResult tryRows(ITransform transform, int count) {
    return step(transform, count, true, DEFAULT_TIMEOUT_MS);
  }

  public StepResult step(ITransform transform, int count, boolean asTry, long timeoutMs) {
    if (count < 1) {
      throw new IllegalArgumentException(text("DebugController.Error.Count"));
    }
    String key = key(transform);
    synchronized (lock) {
      pauseMessage = null;
      pausedByCondition.remove(key);
      if (isStopped(transform)) {
        return stopped(transform, count, 0);
      }
      if (!waitUntilStarted(transform, timeoutMs)) {
        if (hasFinished(transform)) {
          return finished(transform, count, 0);
        }
        return result(
            transform, Activity.TIMED_OUT, count, 0, text("DebugController.Detail.DidNotStart"));
      }
      if (hasFinished(transform)) {
        return finished(transform, count, 0);
      }
      if (!readyForInput(transform)) {
        return result(
            transform, Activity.WAITING, count, 0, text("DebugController.Detail.Waiting"));
      }
      free.remove(key);
      if (asTry) {
        tryStep.add(key);
        beginTry(transform);
      } else {
        tryStep.remove(key);
      }
      int seen = completed.getOrDefault(key, 0);
      permits.merge(key, count, Integer::sum);
      lock.notifyAll();
      // The timeout is how long a step may go without finishing another row, not a budget for the
      // whole batch. A CSV source can need far longer than 10s to produce 50,000 rows.
      Activity activity = Activity.TIMED_OUT;
      String detail = null;
      int progressed = 0;
      long lastProgress = System.nanoTime();
      long stallNs = Math.max(1L, timeoutMs) * 1_000_000L;
      try {
        while (true) {
          int doneNow = Math.max(0, completed.getOrDefault(key, 0) - seen);
          if (pausedByCondition.contains(key) && !insideRow.contains(key)) {
            activity = Activity.PAUSED;
            detail = pauseMessage == null ? text("DebugController.Detail.Paused") : pauseMessage;
            break;
          }
          if (doneNow >= count) {
            activity = Activity.STEPPED;
            break;
          }
          if (isStopped(transform)) {
            activity = Activity.STOPPED;
            detail = text(CONST_DEBUG_CONTROLLER_DETAIL_PIPELINE_STOPPED);
            break;
          }
          if (hasFinished(transform)) {
            activity = Activity.FINISHED;
            detail = text("DebugController.Detail.TransformFinished");
            break;
          }
          long now = System.nanoTime();
          if (doneNow > progressed) {
            progressed = doneNow;
            lastProgress = now;
          } else if (pausedByCondition.contains(key)) {
            lastProgress = now;
          } else if (now - lastProgress >= stallNs) {
            activity = Activity.TIMED_OUT;
            detail =
                text(
                    "DebugController.Detail.Stalled",
                    Integer.toString(doneNow),
                    Integer.toString(count));
            break;
          }
          try {
            lock.wait(20L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            activity = Activity.STOPPED;
            detail = text(CONST_DEBUG_CONTROLLER_DETAIL_PIPELINE_STOPPED);
            break;
          }
        }
      } finally {
        tryStep.remove(key);
        permits.remove(key);
        if (asTry) {
          endTry(transform);
        }
      }
      int done = Math.max(0, completed.getOrDefault(key, 0) - seen);
      if (activity == Activity.STEPPED) {
        detail =
            asTry
                ? text(
                    done == 1
                        ? "DebugController.Detail.TriedOne"
                        : "DebugController.Detail.TriedMany",
                    Integer.toString(done))
                : text("DebugController.Detail.Stepped", Integer.toString(done));
      }
      return result(transform, activity, count, done, detail);
    }
  }

  /** Resumes only these copies. Every other copy stays parked between rows. */
  public void runSelected(List<ITransform> transforms) {
    synchronized (lock) {
      pauseMessage = null;
      free.clear();
      tryStep.clear();
      if (transforms != null) {
        for (ITransform transform : transforms) {
          free.add(key(transform));
        }
      }
      lock.notifyAll();
    }
  }

  /** Resumes every copy. */
  public void continueAll() {
    synchronized (lock) {
      pauseMessage = null;
      tryStep.clear();
      free.clear();
      if (pipeline.getTransforms() != null) {
        for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
          free.add(key(combi.transform));
        }
      }
      lock.notifyAll();
    }
  }

  /** True when this copy is free-running instead of waiting for a permit. */
  public boolean isFreeRunning(ITransform transform) {
    synchronized (lock) {
      return free.contains(key(transform));
    }
  }

  /** True when at least one copy of this transform is free-running. */
  public boolean isAnyCopyFreeRunning(String transformName) {
    synchronized (lock) {
      if (pipeline.getTransforms() == null) {
        return false;
      }
      for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
        if (transformName.equals(combi.transform.getTransformName())
            && free.contains(key(combi.transform))) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * True when a copy of this transform is still in the session and is not free-running. Finished
   * copies are ignored. Several transforms can be free-running while others stay paused.
   */
  public boolean isAnyCopyPaused(String transformName) {
    synchronized (lock) {
      if (pipeline.getTransforms() == null) {
        return false;
      }
      for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
        ITransform transform = combi.transform;
        if (!transformName.equals(transform.getTransformName())) {
          continue;
        }
        if (transform.getExecutionStartDate() != null && !transform.isRunning()) {
          continue;
        }
        if (!free.contains(key(transform))) {
          return true;
        }
      }
      return false;
    }
  }

  /** Parks every copy before its next row. A row already inside processRow finishes first. */
  public void pause() {
    synchronized (lock) {
      free.clear();
      lock.notifyAll();
    }
  }

  /**
   * Runs upstream copies until {@code target} has a new unread input row, or the upstream copies
   * finish. {@code target} itself stays parked.
   */
  public StepResult runToHere(ITransform target, long timeoutMs) {
    List<ITransform> upstream = upstreamOf(target);
    int before = unreadInputs(target);
    runSelected(upstream);
    long deadline = System.nanoTime() + Math.max(1L, timeoutMs) * 1_000_000L;
    boolean arrived = false;
    boolean upstreamDone = upstream.isEmpty();
    while (System.nanoTime() < deadline && !isStopped(target)) {
      if (unreadInputs(target) > before) {
        arrived = true;
        break;
      }
      if (allFinished(upstream)) {
        upstreamDone = true;
        break;
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    pause();
    if (isStopped(target)) {
      return result(
          target, Activity.STOPPED, 0, 0, text(CONST_DEBUG_CONTROLLER_DETAIL_PIPELINE_STOPPED));
    }
    if (arrived) {
      return result(target, Activity.STEPPED, 0, 0, text("DebugController.Detail.InputArrived"));
    }
    if (upstreamDone) {
      return result(
          target, Activity.FINISHED, 0, 0, text("DebugController.Detail.UpstreamFinished"));
    }
    return result(target, Activity.TIMED_OUT, 0, 0, text("DebugController.Detail.NoInput"));
  }

  /**
   * Moves one hop's cursor. Returns false when the reading transform has already dropped this hop,
   * because rewinding it would not be seen. Downstream buffers stay until {@link #clearDownstream}.
   */
  public boolean rewind(CursorRowSet rowSet, int index) {
    if (!consumerStillReads(rowSet)) {
      return false;
    }
    rowSet.setCursor(index);
    timelineDirty = true;
    return true;
  }

  /** Drops rows on every hop downstream of {@code rowSet}. The hop itself is kept. */
  public void clearDownstream(CursorRowSet rowSet) {
    Set<String> seen = new HashSet<>();
    Deque<String> pending = new ArrayDeque<>();
    pending.add(rowSet.getDestinationTransformName());
    while (!pending.isEmpty()) {
      String origin = pending.removeFirst();
      if (!seen.add(origin)) {
        continue;
      }
      for (CursorRowSet hop : getCursorRowSets()) {
        if (origin.equals(hop.getOriginTransformName())) {
          hop.discardRows();
          pending.add(hop.getDestinationTransformName());
        }
      }
    }
    timelineDirty = false;
  }

  /** Wakes threads parked in {@link #beforeProcessRow(ITransform)} so a stop can land. */
  public void cancelWaiters() {
    synchronized (lock) {
      permits.clear();
      free.clear();
      tryStep.clear();
      lock.notifyAll();
    }
  }

  public List<CursorRowSet> getCursorRowSets() {
    List<CursorRowSet> sets = new ArrayList<>();
    if (pipeline.getRowsets() == null) {
      return sets;
    }
    for (IRowSet rowSet : pipeline.getRowsets()) {
      if (rowSet instanceof CursorRowSet cursorRowSet) {
        sets.add(cursorRowSet);
      }
    }
    return sets;
  }

  public List<StepEvent> getEvents() {
    synchronized (lock) {
      return new ArrayList<>(events);
    }
  }

  private boolean waitUntilStarted(ITransform transform, long timeoutMs) {
    long deadline = System.nanoTime() + Math.max(1L, timeoutMs) * 1_000_000L;
    while (transform.getExecutionStartDate() == null
        && !isStopped(transform)
        && System.nanoTime() < deadline) {
      try {
        lock.wait(20L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return transform.getExecutionStartDate() != null;
  }

  private boolean readyForInput(ITransform transform) {
    List<IRowSet> inputs = snapshotInputs(transform);
    if (inputs.isEmpty()) {
      return true;
    }
    boolean unread = false;
    boolean open = false;
    for (IRowSet rowSet : inputs) {
      if (rowSet.size() > 0) {
        unread = true;
      }
      if (!rowSet.isDone()) {
        open = true;
      }
    }
    return unread || !open;
  }

  private int unreadInputs(ITransform transform) {
    int total = 0;
    for (IRowSet rowSet : snapshotInputs(transform)) {
      total += rowSet.size();
    }
    return total;
  }

  private List<IRowSet> snapshotInputs(ITransform transform) {
    List<IRowSet> inputs = transform.getInputRowSets();
    if (inputs == null || inputs.isEmpty()) {
      return List.of();
    }
    return new ArrayList<>(inputs);
  }

  private void beginTry(ITransform transform) {
    for (IRowSet rowSet : snapshotInputs(transform)) {
      if (rowSet instanceof CursorRowSet cursorRowSet) {
        cursorRowSet.mark();
      }
    }
    for (IRowSet rowSet : snapshotOutputs(transform)) {
      if (rowSet instanceof CursorRowSet cursorRowSet) {
        cursorRowSet.setTrialMode(true);
      }
    }
  }

  private void endTry(ITransform transform) {
    for (IRowSet rowSet : snapshotInputs(transform)) {
      if (rowSet instanceof CursorRowSet cursorRowSet) {
        cursorRowSet.resetToMark();
      }
    }
    for (IRowSet rowSet : snapshotOutputs(transform)) {
      if (rowSet instanceof CursorRowSet cursorRowSet) {
        cursorRowSet.setTrialMode(false);
      }
    }
  }

  private List<IRowSet> snapshotOutputs(ITransform transform) {
    List<IRowSet> outputs = transform.getOutputRowSets();
    if (outputs == null || outputs.isEmpty()) {
      return List.of();
    }
    return new ArrayList<>(outputs);
  }

  private List<ITransform> upstreamOf(ITransform target) {
    List<ITransform> found = new ArrayList<>();
    PipelineMeta meta = pipeline.getPipelineMeta();
    if (meta == null || target.getTransformMeta() == null) {
      return found;
    }
    Set<String> names = new HashSet<>();
    Deque<TransformMeta> pending = new ArrayDeque<>();
    pending.add(target.getTransformMeta());
    Set<String> visited = new HashSet<>();
    while (!pending.isEmpty()) {
      TransformMeta current = pending.removeFirst();
      if (!visited.add(current.getName())) {
        continue;
      }
      for (TransformMeta previous : meta.findPreviousTransforms(current, false)) {
        names.add(previous.getName());
        pending.add(previous);
      }
    }
    names.remove(target.getTransformName());
    for (String name : names) {
      List<ITransform> copies = pipeline.getTransforms(name);
      if (copies != null) {
        found.addAll(copies);
      }
    }
    return found;
  }

  private boolean allFinished(List<ITransform> transforms) {
    if (transforms.isEmpty()) {
      return true;
    }
    for (ITransform transform : transforms) {
      if (!hasFinished(transform)) {
        return false;
      }
    }
    return true;
  }

  private boolean hasFinished(ITransform transform) {
    return ended.contains(key(transform))
        || (transform.getExecutionStartDate() != null && !transform.isRunning());
  }

  /** True while the destination copy still has this hop in its input list. */
  private boolean consumerStillReads(CursorRowSet rowSet) {
    if (rowSet == null || pipeline.getTransforms() == null) {
      return false;
    }
    List<ITransform> copies = pipeline.getTransforms(rowSet.getDestinationTransformName());
    if (copies == null) {
      return false;
    }
    for (ITransform transform : copies) {
      if (transform.getCopy() != rowSet.getDestinationTransformCopy()) {
        continue;
      }
      if (transform.getExecutionStartDate() == null) {
        return true;
      }
      List<IRowSet> inputs = transform.getInputRowSets();
      return inputs != null && inputs.contains(rowSet);
    }
    return false;
  }

  private boolean isStopped(ITransform transform) {
    return transform.isStopped() || pipeline.isStopped();
  }

  private void appendEvent(StepEvent event) {
    events.addLast(event);
    while (events.size() > MAX_EVENTS) {
      events.removeFirst();
    }
  }

  private StepResult result(
      ITransform transform, Activity activity, int requested, int completedRows, String detail) {
    return new StepResult(
        transform.getTransformName(),
        transform.getCopy(),
        activity,
        requested,
        completedRows,
        detail);
  }

  /**
   * Called when a row has been stored on a debug hop. A matching condition parks the chosen
   * transforms before their next row.
   */
  public void rowEntered(CursorRowSet rowSet, IRowMeta rowMeta, Object[] row) {
    HopPause pause;
    synchronized (lock) {
      pause = hopPauses.get(hopKey(rowSet));
    }
    boolean hit = false;
    if (pause != null && pause.condition() != null && rowMeta != null && row != null) {
      try {
        hit = pause.condition().evaluate(rowMeta, row);
      } catch (RuntimeException e) {
        hit = false;
      }
    }
    synchronized (lock) {
      if (hit && pause != null) {
        switch (pause.scope()) {
          case TRANSFORM -> park(rowSet.getOriginTransformName(), rowSet.getOriginTransformCopy());
          case PIPELINE -> {
            free.clear();
            permits.clear();
            if (pipeline.getTransforms() != null) {
              for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
                pausedByCondition.add(key(combi.transform));
              }
            }
          }
          case DOWNSTREAM -> parkDownstream(rowSet);
          default -> park(rowSet.getOriginTransformName(), rowSet.getOriginTransformCopy());
        }
        pauseMessage =
            text(
                "DebugController.Pause.Matched",
                pause.scope().getDescription(),
                rowSet.getOriginTransformName(),
                rowSet.getDestinationTransformName());
      }
      // Wake a step that is parked until the next input row arrives.
      lock.notifyAll();
    }
  }

  public void setHopPause(CursorRowSet rowSet, Condition condition, DebugPauseScope scope) {
    synchronized (lock) {
      if (rowSet == null || condition == null || scope == null) {
        return;
      }
      hopPauses.put(hopKey(rowSet), new HopPause(new Condition(condition), scope));
    }
  }

  public void clearHopPause(CursorRowSet rowSet) {
    synchronized (lock) {
      if (rowSet != null) {
        hopPauses.remove(hopKey(rowSet));
      }
    }
  }

  public HopPause getHopPause(CursorRowSet rowSet) {
    synchronized (lock) {
      return rowSet == null ? null : hopPauses.get(hopKey(rowSet));
    }
  }

  /** True when any copy of this hop has a pause condition. */
  public boolean hasHopPause(String origin, String destination) {
    synchronized (lock) {
      for (String key : hopPauses.keySet()) {
        String[] parts = key.split("\t", -1);
        if (parts.length >= 4 && origin.equals(parts[0]) && destination.equals(parts[2])) {
          return true;
        }
      }
      return false;
    }
  }

  public String getPauseMessage() {
    synchronized (lock) {
      return pauseMessage;
    }
  }

  private void park(String name, int copy) {
    String transformKey = name + "\t" + copy;
    free.remove(transformKey);
    permits.remove(transformKey);
    pausedByCondition.add(transformKey);
  }

  private StepResult stopped(ITransform transform, int requested, int completedRows) {
    return result(
        transform,
        Activity.STOPPED,
        requested,
        completedRows,
        text(CONST_DEBUG_CONTROLLER_DETAIL_PIPELINE_STOPPED));
  }

  private StepResult finished(ITransform transform, int requested, int completedRows) {
    return result(
        transform,
        Activity.FINISHED,
        requested,
        completedRows,
        text("DebugController.Detail.TransformFinished"));
  }

  private static String text(String key, String... parameters) {
    return BaseMessages.getString(PKG, key, parameters);
  }

  private void parkDownstream(CursorRowSet rowSet) {
    park(rowSet.getDestinationTransformName(), rowSet.getDestinationTransformCopy());
    if (pipeline.getPipelineMeta() == null || pipeline.getTransforms() == null) {
      return;
    }
    TransformMeta destination =
        pipeline.getPipelineMeta().findTransform(rowSet.getDestinationTransformName());
    if (destination == null) {
      return;
    }
    Set<String> seen = new HashSet<>();
    Deque<TransformMeta> walk = new ArrayDeque<>();
    walk.addAll(pipeline.getPipelineMeta().findNextTransforms(destination));
    while (!walk.isEmpty()) {
      TransformMeta next = walk.removeFirst();
      if (next == null || !seen.add(next.getName())) {
        continue;
      }
      for (TransformMetaDataCombi combi : pipeline.getTransforms()) {
        if (next.getName().equals(combi.transform.getTransformName())) {
          park(combi.transform.getTransformName(), combi.transform.getCopy());
        }
      }
      walk.addAll(pipeline.getPipelineMeta().findNextTransforms(next));
    }
  }

  static String hopKey(CursorRowSet rowSet) {
    return rowSet.getOriginTransformName()
        + "\t"
        + rowSet.getOriginTransformCopy()
        + "\t"
        + rowSet.getDestinationTransformName()
        + "\t"
        + rowSet.getDestinationTransformCopy();
  }

  static String key(ITransform transform) {
    return transform.getTransformName() + "\t" + transform.getCopy();
  }
}
