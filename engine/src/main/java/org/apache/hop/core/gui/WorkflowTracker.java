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

package org.apache.hop.core.gui;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Responsible for tracking the execution of a workflow as a hierarchy. */
@Getter
@Setter
public class WorkflowTracker<T extends WorkflowMeta> {
  /**
   * The trackers for each individual action. Since we invoke LinkedList.removeFirst() there is no
   * sense in lurking the field behind the interface
   */
  private LinkedList<WorkflowTracker> workflowTrackers;

  /**
   * Set to track unique identifiers of workflow trackers to prevent duplicates when actions run in
   * parallel
   */
  private Set<String> trackerIdentifiers;

  /** If the workflowTrackers list is empty, then this is the result */
  private ActionResult result;

  /** The parent workflow tracker, null if this is the root */
  private WorkflowTracker parentWorkflowTracker;

  private String workflowName;

  private String workflowFilename;

  private int maxChildren;

  private final ReentrantReadWriteLock lock;

  /**
   * @param workflowMeta the workflow metadata to keep track of (with maximum 5000 children)
   */
  public WorkflowTracker(T workflowMeta) {
    this(
        workflowMeta,
        Const.toInt(EnvUtil.getSystemProperty(Const.HOP_MAX_WORKFLOW_TRACKER_SIZE), 5000));
  }

  /**
   * @param workflowMeta The workflow metadata to track
   * @param maxChildren The maximum number of children to keep track of (1000 is the default)
   */
  public WorkflowTracker(T workflowMeta, int maxChildren) {
    if (workflowMeta != null) {
      this.workflowName = workflowMeta.getName();
      this.workflowFilename = workflowMeta.getFilename();
    }

    this.workflowTrackers = new LinkedList<>();
    this.trackerIdentifiers = new HashSet<>();
    this.maxChildren = maxChildren;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a workflow tracker with a single result (maxChildren children are kept)
   *
   * @param workflowMeta the workflow metadata to keep track of
   * @param result the action result to track.
   */
  public WorkflowTracker(T workflowMeta, ActionResult result) {
    this(workflowMeta);
    this.result = result;
  }

  /**
   * Creates a workflow tracker with a single result
   *
   * @param workflowMeta the workflow metadata to keep track of
   * @param maxChildren The maximum number of children to keep track of
   * @param result the action result to track.
   */
  public WorkflowTracker(T workflowMeta, int maxChildren, ActionResult result) {
    this(workflowMeta, maxChildren);
    this.result = result;
  }

  public void addWorkflowTracker(WorkflowTracker workflowTracker) {
    lock.writeLock().lock();
    try {
      String identifier = workflowTracker.getUniqueIdentifier();

      // Only add if this tracker is unique (not already present)
      if (identifier != null && !trackerIdentifiers.contains(identifier)) {
        workflowTrackers.add(workflowTracker);
        trackerIdentifiers.add(identifier);

        // Remove oldest entries if we exceed maxChildren
        while (workflowTrackers.size() > maxChildren) {
          WorkflowTracker removed = workflowTrackers.removeFirst();
          String removedId = removed.getUniqueIdentifier();
          if (removedId != null) {
            trackerIdentifiers.remove(removedId);
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public WorkflowTracker getWorkflowTracker(int i) {
    lock.readLock().lock();
    try {
      return workflowTrackers.get(i);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int nrWorkflowTrackers() {
    lock.readLock().lock();
    try {
      return workflowTrackers.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns a list that contains all workflow trackers. The list is created as a defensive copy of
   * internal trackers' storage.
   *
   * @return list of workflow trackers
   */
  public List<WorkflowTracker> getWorkflowTrackers() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(workflowTrackers);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @param workflowTrackers The workflowTrackers to set.
   */
  public void setWorkflowTrackers(List<WorkflowTracker> workflowTrackers) {
    lock.writeLock().lock();
    try {
      this.workflowTrackers.clear();
      this.trackerIdentifiers.clear();
      this.workflowTrackers.addAll(workflowTrackers);

      // Rebuild the identifier set
      for (WorkflowTracker tracker : workflowTrackers) {
        String identifier = tracker.getUniqueIdentifier();
        if (identifier != null) {
          this.trackerIdentifiers.add(identifier);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return Returns the result.
   */
  public ActionResult getActionResult() {
    return result;
  }

  /**
   * @param result The result to set.
   */
  public void setActionResult(ActionResult result) {
    this.result = result;
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      workflowTrackers.clear();
      trackerIdentifiers.clear();
      result = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Finds the WorkflowTracker for the action specified. Use this to
   *
   * @param actionMeta The action to search the workflow tracker for
   * @return The WorkflowTracker of null if none could be found...
   */
  public WorkflowTracker findWorkflowTracker(ActionMeta actionMeta) {
    if (actionMeta.getName() == null) {
      return null;
    }

    lock.readLock().lock();
    try {
      ListIterator<WorkflowTracker> it = workflowTrackers.listIterator(workflowTrackers.size());
      while (it.hasPrevious()) {
        WorkflowTracker tracker = it.previous();
        ActionResult result = tracker.getActionResult();
        if (result != null && actionMeta.getName().equals(result.getActionName())) {
          return tracker;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return null;
  }

  public int getTotalNumberOfItems() {
    lock.readLock().lock();
    try {
      int total = 1; // 1 = this one

      for (WorkflowTracker workflowTracker : workflowTrackers) {
        total += workflowTracker.getTotalNumberOfItems();
      }

      return total;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Gets a unique identifier for this workflow tracker. Uses the log channel ID to uniquely
   * identify each execution event. When actions run in parallel, the SAME logChannelId means the
   * SAME execution event, which should be deduplicated. Different logChannelIds means different
   * events.
   *
   * <p>Falls back to a combination of workflow name, action name, comment, and timestamp if
   * logChannelId is not available.
   *
   * @return A unique identifier string, or null if insufficient information
   */
  public String getUniqueIdentifier() {
    if (result != null) {
      // Fallback: construct identifier from available fields
      StringBuilder identifier = new StringBuilder();
      if (workflowName != null) {
        identifier.append(workflowName).append("::");
      }
      if (workflowFilename != null) {
        identifier.append(workflowFilename);
      }

      return identifier.length() > 0 ? identifier.toString() : null;
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkflowTracker<?> that = (WorkflowTracker<?>) o;

    String thisId = this.getUniqueIdentifier();
    String thatId = that.getUniqueIdentifier();

    // If both have identifiers, compare them
    if (thisId != null && thatId != null) {
      return thisId.equals(thatId);
    }

    // Fallback to comparing workflow name and filename
    return Objects.equals(workflowName, that.workflowName)
        && Objects.equals(workflowFilename, that.workflowFilename)
        && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    String identifier = getUniqueIdentifier();
    if (identifier != null) {
      return identifier.hashCode();
    }
    return Objects.hash(workflowName, workflowFilename, result);
  }
}
