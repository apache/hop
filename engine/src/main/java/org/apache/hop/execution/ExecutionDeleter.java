/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution;

import java.util.Date;
import java.util.List;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;

/** Shared delete loop for execution information locations. */
public final class ExecutionDeleter {

  /** How many parent ids to take from a newest-first list before deleting that page. */
  public static final int PAGE_SIZE = 200;

  private ExecutionDeleter() {}

  /**
   * Delete top-level executions and the children {@link IExecutionInfoLocation#deleteExecution}
   * removes with them.
   *
   * @param olderThan exclusive upper bound on the execution start date. Null deletes every
   *     execution. A missing start date is deleted.
   * @param monitor optional progress and cancel. Cancel stops before the next execution.
   * @return how many top-level executions were deleted
   */
  public static int delete(
      IExecutionInfoLocation location, Date olderThan, IProgressMonitor monitor)
      throws HopException {
    if (olderThan == null) {
      return deleteAll(location, monitor);
    }
    return deleteOlderThan(location, olderThan, monitor);
  }

  /**
   * Page through parent ids. Deleting a page is what advances a list that only returns its newest
   * rows, so a backend capped at 10 or 50 still drains.
   */
  private static int deleteAll(IExecutionInfoLocation location, IProgressMonitor monitor)
      throws HopException {
    int deleted = 0;
    int failed = 0;
    HopException firstFailure = null;
    while (!canceled(monitor)) {
      List<String> ids = location.getExecutionIds(false, PAGE_SIZE);
      if (ids == null || ids.isEmpty()) {
        break;
      }
      int removed = 0;
      for (String id : ids) {
        if (canceled(monitor)) {
          break;
        }
        subTask(monitor, deleted + removed + 1, id);
        try {
          if (location.deleteExecution(id)) {
            removed++;
          } else {
            failed++;
            if (firstFailure == null) {
              firstFailure = new HopException("Execution " + id + " was not deleted");
            }
          }
        } catch (Exception e) {
          failed++;
          if (firstFailure == null) {
            firstFailure = new HopException("Error deleting execution " + id, e);
          }
        }
      }
      deleted += removed;
      // Nothing in this page went away. Another pass would ask for the same ids.
      if (removed == 0) {
        break;
      }
    }
    throwIfFailed(deleted, failed, firstFailure);
    return deleted;
  }

  /**
   * One pass over every parent id. Locations whose unbounded list is capped override {@link
   * IExecutionInfoLocation#deleteExecutions(Date, IProgressMonitor)} and filter in the query.
   */
  private static int deleteOlderThan(
      IExecutionInfoLocation location, Date olderThan, IProgressMonitor monitor)
      throws HopException {
    List<String> ids = location.getExecutionIds(false, 0);
    int deleted = 0;
    int failed = 0;
    HopException firstFailure = null;
    if (ids != null) {
      for (String id : ids) {
        if (canceled(monitor)) {
          break;
        }
        Execution execution = location.getExecution(id);
        Date start = execution == null ? null : execution.getExecutionStartDate();
        if (start != null && !start.before(olderThan)) {
          continue;
        }
        subTask(monitor, deleted + 1, id);
        try {
          if (location.deleteExecution(id)) {
            deleted++;
          } else {
            failed++;
            if (firstFailure == null) {
              firstFailure = new HopException("Execution " + id + " was not deleted");
            }
          }
        } catch (Exception e) {
          failed++;
          if (firstFailure == null) {
            firstFailure = new HopException("Error deleting execution " + id, e);
          }
        }
      }
    }
    throwIfFailed(deleted, failed, firstFailure);
    return deleted;
  }

  /**
   * The first failure is kept as the cause. Callers that need the deleted count read it from the
   * message only when this throws; a clean run returns the count instead.
   */
  public static void throwIfFailed(int deleted, int failed, HopException firstFailure)
      throws HopException {
    if (failed > 0) {
      HopException failure =
          firstFailure == null
              ? new HopException("One or more executions were not deleted")
              : firstFailure;
      throw new HopException(
          "Deleted " + deleted + " execution(s). " + failed + " could not be deleted.", failure);
    }
  }

  private static void subTask(IProgressMonitor monitor, int index, String id) {
    if (monitor != null) {
      monitor.subTask("Deleting execution " + index + ": " + id);
    }
  }

  private static boolean canceled(IProgressMonitor monitor) {
    return monitor != null && monitor.isCanceled();
  }
}
