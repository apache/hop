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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;

public record DefaultExecutionSelector(
    boolean isSelectingParents,
    boolean isSelectingFailed,
    boolean isSelectingRunning,
    boolean isSelectingFinished,
    boolean isSelectingWorkflows,
    boolean isSelectingPipelines,
    String filterText,
    LastPeriod startDateFilter)
    implements IExecutionSelector {
  public static final SimpleDateFormat START_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm");

  public boolean isSelected(Execution execution) {
    if (isSelectingWorkflows && !execution.getExecutionType().equals(ExecutionType.Workflow)) {
      return false;
    }
    if (isSelectingPipelines && !execution.getExecutionType().equals(ExecutionType.Pipeline)) {
      return false;
    }
    if (StringUtils.isNotEmpty(filterText)) {
      boolean match = execution.getName().toLowerCase().contains(filterText.toLowerCase());
      match = match || execution.getId().contains(filterText);
      if (execution.getExecutionStartDate() != null) {
        String startDateString = START_DATE_FORMAT.format(execution.getExecutionStartDate());
        match = match || startDateString.contains(filterText);
      }
      if (!match) {
        return false;
      }
    }
    if (startDateFilter != null && execution.getExecutionStartDate() != null) {
      return startDateFilter.matches(execution.getExecutionStartDate());
    }
    return false;
  }

  public boolean isSelected(ExecutionState executionState) {
    if (isSelectingParents && StringUtils.isNotEmpty(executionState.getParentId())) {
      return false;
    }
    if (isSelectingFailed && !executionState.isFailed()) {
      return false;
    }
    if (isSelectingFinished
        && !executionState.getStatusDescription().toLowerCase().startsWith("finished")) {
      return false;
    }
    return !isSelectingRunning
        || executionState.getStatusDescription().toLowerCase().startsWith("running");
  }

  /**
   * This is a default implementation to make all implementations work.
   *
   * @param location The location to load from
   * @param selector The selector to use
   * @return The list of selected execution IDs.
   * @throws HopException In case something went wrong.
   */
  public static List<String> findExecutionIDs(
      IExecutionInfoLocation location, IExecutionSelector selector) throws HopException {

    List<String> selection = new ArrayList<>();
    List<String> ids = location.getExecutionIds(false, 1000);
    for (String id : ids) {
      Execution execution = location.getExecution(id);
      ExecutionState state = location.getExecutionState(id);
      if (selector.isSelected(execution) && selector.isSelected(state)) {
        selection.add(id);
      }
    }
    return selection;
  }
}
