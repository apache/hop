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

package org.apache.hop.debug.action;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Custom logging on an action offers to log the result, the result rows, the result files and the
 * changed variables of that action. That output has to survive a workflow that itself logs nothing,
 * which is the whole point of setting a custom level on a single action.
 */
class ModifyActionLogLevelExtensionPointTest {

  private static final String ACTION = "dummy";

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void theActionResultIsLoggedWhenTheWorkflowItselfLogsNothing() throws Exception {
    ActionDebugLevel debugLevel = new ActionDebugLevel(LogLevel.DETAILED);
    debugLevel.setLoggingResult(true);

    List<String> logged = runAndCollectLog(debugLevel, LogLevel.NOTHING);

    assertTrue(
        logged.stream().anyMatch(line -> line.contains("Action results:")),
        "the action result was not logged, log was: " + logged);
  }

  /**
   * The custom level can also be lower than the workflow's own level. Asking for the result of an
   * action still has to log it - the checkbox is more specific than the level.
   */
  @Test
  void theActionResultIsLoggedWhenTheCustomLevelIsLowerThanTheWorkflowLevel() throws Exception {
    ActionDebugLevel debugLevel = new ActionDebugLevel(LogLevel.ERROR);
    debugLevel.setLoggingResult(true);

    List<String> logged = runAndCollectLog(debugLevel, LogLevel.BASIC);

    assertTrue(
        logged.stream().anyMatch(line -> line.contains("Action results:")),
        "the action result was not logged, log was: " + logged);
  }

  /** Run a "start -> dummy" workflow with a custom logging configuration on the dummy action. */
  private List<String> runAndCollectLog(ActionDebugLevel debugLevel, LogLevel workflowLogLevel)
      throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("custom action logging");

    ActionMeta start = new ActionMeta(new ActionStart("start"));
    ActionMeta dummy = new ActionMeta(new ActionDummy());
    dummy.setName(ACTION);
    workflowMeta.addAction(start);
    workflowMeta.addAction(dummy);
    workflowMeta.addWorkflowHop(new org.apache.hop.workflow.WorkflowHopMeta(start, dummy));

    Map<String, String> debugGroup = new HashMap<>();
    DebugLevelUtil.storeActionDebugLevel(debugGroup, ACTION, debugLevel);
    workflowMeta.getAttributesMap().put(Defaults.DEBUG_GROUP, debugGroup);

    // The workflow creates its log channel when it starts, and without a parent logging object
    // that channel takes its level from the default log level - the same knob the GUI and hop-run
    // turn when you pick a log level for a run.
    //
    LogLevel previousDefault = DefaultLogLevel.getLogLevel();
    DefaultLogLevel.setLogLevel(workflowLogLevel);

    List<String> logged = new ArrayList<>();
    IHopLoggingEventListener listener =
        (HopLoggingEvent event) -> {
          synchronized (logged) {
            logged.add(event.getMessage().toString());
          }
        };
    HopLogStore.getAppender().addLoggingEventListener(listener);
    try {
      LocalWorkflowEngine workflow = new LocalWorkflowEngine(workflowMeta);
      workflow.initializeFrom(new Variables());
      workflow.setLogLevel(workflowLogLevel);
      workflow.startExecution();
    } finally {
      HopLogStore.getAppender().removeLoggingEventListener(listener);
      DefaultLogLevel.setLogLevel(previousDefault);
    }

    synchronized (logged) {
      return new ArrayList<>(logged);
    }
  }
}
