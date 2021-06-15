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

import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for tracking the execution of a workflow as a hierarchy.
 *
 * @author Matt
 * @since 30-mar-2006
 */
public class WorkflowTracker<T extends WorkflowMeta> {
  /**
   * The trackers for each individual action.
   * Since we invoke LinkedList.removeFirst() there is no sense in lurking the field behind the interface
   */
  private LinkedList<WorkflowTracker> workflowTrackers;

  /**
   * If the workflowTrackers list is empty, then this is the result
   */
  private ActionResult result;

  /**
   * The parent workflow tracker, null if this is the root
   */
  private WorkflowTracker parentWorkflowTracker;

  private String workflowName;

  private String workflowFilename;

  private int maxChildren;

  private final ReentrantReadWriteLock lock;

  /**
   * @param workflowMeta the workflow metadata to keep track of (with maximum 5000 children)
   */
  public WorkflowTracker( T workflowMeta ) {
    this( workflowMeta, Const.toInt( EnvUtil.getSystemProperty( Const.HOP_MAX_WORKFLOW_TRACKER_SIZE ), 5000 ) );
  }

  /**
   * @param workflowMeta     The workflow metadata to track
   * @param maxChildren The maximum number of children to keep track of (1000 is the default)
   */
  public WorkflowTracker( T workflowMeta, int maxChildren ) {
    if ( workflowMeta != null ) {
      this.workflowName = workflowMeta.getName();
      this.workflowFilename = workflowMeta.getFilename();
    }

    this.workflowTrackers = new LinkedList<>();
    this.maxChildren = maxChildren;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Creates a workflow tracker with a single result (maxChildren children are kept)
   *
   * @param workflowMeta the workflow metadata to keep track of
   * @param result  the action result to track.
   */
  public WorkflowTracker( T workflowMeta, ActionResult result ) {
    this( workflowMeta );
    this.result = result;
  }

  /**
   * Creates a workflow tracker with a single result
   *
   * @param workflowMeta     the workflow metadata to keep track of
   * @param maxChildren The maximum number of children to keep track of
   * @param result      the action result to track.
   */
  public WorkflowTracker( T workflowMeta, int maxChildren, ActionResult result ) {
    this( workflowMeta, maxChildren );
    this.result = result;
  }

  public void addWorkflowTracker( WorkflowTracker workflowTracker ) {
    lock.writeLock().lock();
    try {
      workflowTrackers.add( workflowTracker );
      while ( workflowTrackers.size() > maxChildren ) {
        // Use remove instead of subList
        workflowTrackers.removeFirst();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public WorkflowTracker getWorkflowTracker( int i ) {
    lock.readLock().lock();
    try {
      return workflowTrackers.get( i );
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
   * Returns a list that contains all workflow trackers. The list is created as a defensive copy of internal trackers'
   * storage.
   *
   * @return list of workflow trackers
   */
  public List<WorkflowTracker> getWorkflowTrackers() {
    lock.readLock().lock();
    try {
      return new ArrayList<>( workflowTrackers );
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @param workflowTrackers The workflowTrackers to set.
   */
  public void setWorkflowTrackers( List<WorkflowTracker> workflowTrackers ) {
    lock.writeLock().lock();
    try {
      this.workflowTrackers.clear();
      this.workflowTrackers.addAll( workflowTrackers );
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
  public void setActionResult( ActionResult result ) {
    this.result = result;
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      workflowTrackers.clear();
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
  public WorkflowTracker findWorkflowTracker( ActionMeta actionMeta ) {
    if ( actionMeta.getName() == null ) {
      return null;
    }

    lock.readLock().lock();
    try {
      ListIterator<WorkflowTracker> it = workflowTrackers.listIterator( workflowTrackers.size() );
      while ( it.hasPrevious() ) {
        WorkflowTracker tracker = it.previous();
        ActionResult result = tracker.getActionResult();
        if ( result != null ) {
          if ( actionMeta.getName().equals( result.getActionName() ) ) {
            return tracker;
          }
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return null;
  }

  /**
   * @return Returns the parentWorkflowTracker.
   */
  public WorkflowTracker getParentWorkflowTracker() {
    return parentWorkflowTracker;
  }

  /**
   * @param parentWorkflowTracker The parentWorkflowTracker to set.
   */
  public void setParentWorkflowTracker( WorkflowTracker parentWorkflowTracker ) {
    this.parentWorkflowTracker = parentWorkflowTracker;
  }

  public int getTotalNumberOfItems() {
    lock.readLock().lock();
    try {
      int total = 1; // 1 = this one

      for ( WorkflowTracker workflowTracker : workflowTrackers ) {
        total += workflowTracker.getTotalNumberOfItems();
      }

      return total;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return the workflowFilename
   */
  public String getWorfkflowFilename() {
    return workflowFilename;
  }

  /**
   * @param workflowFilename the workflowFilename to set
   */
  public void setWorkflowFilename( String workflowFilename ) {
    this.workflowFilename = workflowFilename;
  }

  /**
   * @return the workflowName
   */
  public String getWorkflowName() {
    return workflowName;
  }

  /**
   * @param workflowName the workflowName to set
   */
  public void setWorkflowName( String workflowName ) {
    this.workflowName = workflowName;
  }

  /**
   * @return the maxChildren
   */
  public int getMaxChildren() {
    return maxChildren;
  }

  /**
   * @param maxChildren the maxChildren to set
   */
  public void setMaxChildren( int maxChildren ) {
    this.maxChildren = maxChildren;
  }
}
