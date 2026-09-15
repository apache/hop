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

package org.apache.hop.www;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Server-side capacity check used by register/export servlets. When a client sends {@link
 * #PARAMETER_MAX_CONCURRENT}, registration is refused if the number of unfinished pipelines plus
 * workflows is already at that cap.
 */
public final class HopServerAdmission {

  public static final String PARAMETER_MAX_CONCURRENT = "max_concurrent";
  public static final String RESULT_AT_CAPACITY = "SERVER_AT_CAPACITY";

  private static final Object LOCK = new Object();

  private HopServerAdmission() {}

  public static int parseMaxConcurrent(HttpServletRequest request) {
    if (request == null) {
      return 0;
    }
    return parseMaxConcurrent(request.getParameter(PARAMETER_MAX_CONCURRENT));
  }

  public static int parseMaxConcurrent(String raw) {
    if (StringUtils.isEmpty(raw)) {
      return 0;
    }
    return (int) Const.toLong(raw, 0L);
  }

  public static boolean isAtCapacityResult(WebResult webResult) {
    return webResult != null && RESULT_AT_CAPACITY.equalsIgnoreCase(webResult.getResult());
  }

  public static boolean isRetryableRegistrationFailure(Throwable error, String containerId) {
    if (StringUtils.isNotEmpty(containerId)) {
      return false;
    }
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof HopServerAtCapacityException) {
        return true;
      }
      String message = current.getMessage();
      if (message != null
          && (message.contains(RESULT_AT_CAPACITY)
              || message.toLowerCase(Locale.ROOT).contains("at capacity"))) {
        return true;
      }
      if (current.getCause() == current) {
        break;
      }
    }
    return false;
  }

  public static int countOccupyingSlots(PipelineMap pipelineMap, WorkflowMap workflowMap) {
    int count = 0;
    if (pipelineMap != null) {
      for (HopServerObjectEntry entry : pipelineMap.getPipelineObjects()) {
        IPipelineEngine<PipelineMeta> pipeline = pipelineMap.getPipeline(entry);
        if (occupiesSlot(pipeline)) {
          count++;
        }
      }
    }
    if (workflowMap != null) {
      for (HopServerObjectEntry entry : workflowMap.getWorkflowObjects()) {
        IWorkflowEngine<WorkflowMeta> workflow = workflowMap.getWorkflow(entry);
        if (occupiesSlot(workflow)) {
          count++;
        }
      }
    }
    return count;
  }

  public static boolean occupiesSlot(IPipelineEngine<PipelineMeta> pipeline) {
    return pipeline != null && !pipeline.isFinished();
  }

  public static boolean occupiesSlot(IWorkflowEngine<WorkflowMeta> workflow) {
    return workflow != null && !workflow.isFinished();
  }

  /**
   * If {@code maxConcurrent} is greater than zero and the server is already at that many occupying
   * slots, throw {@link HopServerAtCapacityException}. Otherwise run {@code add} while holding the
   * admission lock so the count-and-add is atomic.
   */
  public static void admitAndAdd(
      PipelineMap pipelineMap, WorkflowMap workflowMap, int maxConcurrent, Runnable add)
      throws HopServerAtCapacityException {
    synchronized (LOCK) {
      if (maxConcurrent > 0) {
        int occupying = countOccupyingSlots(pipelineMap, workflowMap);
        if (occupying >= maxConcurrent) {
          throw new HopServerAtCapacityException(occupying, maxConcurrent);
        }
      }
      add.run();
    }
  }

  public static String querySuffix(int maxConcurrent) {
    if (maxConcurrent <= 0) {
      return "";
    }
    return "&" + PARAMETER_MAX_CONCURRENT + "=" + maxConcurrent;
  }
}
