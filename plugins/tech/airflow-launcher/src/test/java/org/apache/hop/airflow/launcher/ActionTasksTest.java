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

package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.airflow.launcher.ActionTasks.ActionTask;
import org.apache.hop.airflow.launcher.ActionTasks.HopKind;
import org.apache.hop.airflow.launcher.ActionTasks.Role;
import org.apache.hop.airflow.launcher.ActionTasks.Upstream;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

class ActionTasksTest {

  /** Builds the actions and hops of a workflow. */
  private static final class Workflow {
    private final StringBuilder actions = new StringBuilder();
    private final StringBuilder hops = new StringBuilder();

    Workflow action(String name, String type) {
      return action(name, type, false);
    }

    Workflow action(String name, String type, boolean parallel) {
      actions
          .append("<action><name>")
          .append(name)
          .append("</name><type>")
          .append(type)
          .append("</type><parallel>")
          .append(parallel ? "Y" : "N")
          .append("</parallel></action>");
      return this;
    }

    Workflow success(String from, String to) {
      return hop(from, to, "Y", "N", "Y");
    }

    Workflow failure(String from, String to) {
      return hop(from, to, "N", "N", "Y");
    }

    Workflow always(String from, String to) {
      return hop(from, to, "Y", "Y", "Y");
    }

    Workflow disabled(String from, String to) {
      return hop(from, to, "Y", "N", "N");
    }

    private Workflow hop(
        String from, String to, String evaluation, String unconditional, String enabled) {
      hops.append("<hop><from>")
          .append(from)
          .append("</from><to>")
          .append(to)
          .append("</to><enabled>")
          .append(enabled)
          .append("</enabled><evaluation>")
          .append(evaluation)
          .append("</evaluation><unconditional>")
          .append(unconditional)
          .append("</unconditional></hop>");
      return this;
    }

    List<ActionTask> tasks(List<String> reasons) throws Exception {
      String xml =
          "<workflow><actions>" + actions + "</actions><hops>" + hops + "</hops></workflow>";
      Element root =
          DocumentBuilderFactory.newInstance()
              .newDocumentBuilder()
              .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
              .getDocumentElement();
      return ActionTasks.of(
          (Element) root.getElementsByTagName("actions").item(0),
          (Element) root.getElementsByTagName("hops").item(0),
          reasons);
    }

    Map<String, ActionTask> tasks() throws Exception {
      List<String> reasons = new ArrayList<>();
      Map<String, ActionTask> byId = new LinkedHashMap<>();
      for (ActionTask task : tasks(reasons)) {
        byId.put(task.taskId(), task);
      }
      assertEquals(List.of(), reasons);
      return byId;
    }

    List<String> reasons() throws Exception {
      List<String> reasons = new ArrayList<>();
      assertEquals(List.of(), tasks(reasons));
      return reasons;
    }
  }

  private static Workflow start() {
    return new Workflow().action("Start", "SPECIAL");
  }

  private static Upstream up(String taskId, HopKind kind) {
    return new Upstream(taskId, kind, false);
  }

  @Test
  void runsActionsAfterEachOtherOnSuccessHops() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Load", "PIPELINE")
            .action("Check", "SIMPLE_EVAL")
            .always("Start", "Load")
            .success("Load", "Check")
            .tasks();

    assertEquals(List.of("Load", "Check"), List.copyOf(tasks.keySet()));
    assertEquals(
        new ActionTask(
            "Load", "Load", "PIPELINE", Role.ACTION, List.of(), "all_success", null, null),
        tasks.get("Load"));
    assertEquals(List.of(up("Load", HopKind.SUCCESS)), tasks.get("Check").upstreams());
    assertEquals("all_success", tasks.get("Check").triggerRule());
  }

  @Test
  void runsTheTaskAfterAFailureHopWhenTheTaskBeforeFailed() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Check", "EVAL_TABLE_CONTENT")
            .action("Wrong totals", "ABORT")
            .always("Start", "Check")
            .failure("Check", "Wrong totals")
            .tasks();

    assertEquals(Role.ACTION, tasks.get("Check").role());
    assertEquals("one_failed", tasks.get("Wrong_totals").triggerRule());
    assertEquals(List.of(up("Check", HopKind.FAILURE)), tasks.get("Wrong_totals").upstreams());
  }

  @Test
  void runsTheTaskAfterAnUnconditionalHopEitherWay() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Load", "PIPELINE")
            .action("Clean up", "DELETE_FILES")
            .always("Start", "Load")
            .always("Load", "Clean up")
            .tasks();

    assertEquals("none_skipped", tasks.get("Clean_up").triggerRule());
    assertTrue(tasks.get("Clean_up").needsTheTaskBeforeToHaveRun());
  }

  @Test
  void makesAnActionWithBothKindsOfHopsACondition() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Check", "SIMPLE_EVAL")
            .action("Not expected", "ABORT")
            .action("Handle", "WRITE_TO_LOG")
            .always("Start", "Check")
            .success("Check", "Not expected")
            .failure("Check", "Handle")
            .tasks();

    ActionTask check = tasks.get("Check");
    assertEquals(Role.CONDITION, check.role());
    assertEquals("Not_expected", check.thenTaskId());
    assertEquals("Handle", check.elseTaskId());
    // The condition succeeds either way: its sides wait for it to succeed
    assertEquals("all_success", tasks.get("Handle").triggerRule());
    assertEquals(List.of(up("Check", HopKind.FAILURE)), tasks.get("Handle").upstreams());
    assertEquals(3, tasks.size());
  }

  @Test
  void leadsTheSidesOfConditionsToASharedActionThroughGates() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("A", "SIMPLE_EVAL")
            .action("B", "SIMPLE_EVAL")
            .action("C", "SUCCESS")
            .action("Abort", "ABORT")
            .always("Start", "A")
            .success("A", "B")
            .failure("A", "Abort")
            .success("B", "C")
            .failure("B", "Abort")
            .tasks();

    assertEquals("A__else", tasks.get("A").elseTaskId());
    assertEquals("B", tasks.get("A").thenTaskId());
    assertEquals(Role.GATE, tasks.get("A__else").role());
    assertEquals(List.of(up("A", HopKind.FAILURE)), tasks.get("A__else").upstreams());
    ActionTask abort = tasks.get("Abort");
    assertEquals("one_success", abort.triggerRule());
    assertEquals(
        List.of(up("A__else", HopKind.SUCCESS), up("B__else", HopKind.SUCCESS)), abort.upstreams());
  }

  @Test
  void leadsAFailureHopIntoAnActionWithMoreHopsThroughAGate() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Fan out", "DUMMY", true)
            .action("A", "SIMPLE_EVAL")
            .action("B", "SIMPLE_EVAL")
            .action("Mail", "MAIL")
            .always("Start", "Fan out")
            .always("Fan out", "A")
            .always("Fan out", "B")
            .failure("A", "Mail")
            .success("B", "Mail")
            .tasks();

    ActionTask gate = tasks.get("A__on_failure");
    assertEquals(Role.GATE, gate.role());
    assertEquals("one_failed", gate.triggerRule());
    assertEquals("one_success", tasks.get("Mail").triggerRule());
    assertEquals(
        List.of(up("A__on_failure", HopKind.SUCCESS), up("B", HopKind.SUCCESS)),
        tasks.get("Mail").upstreams());
  }

  @Test
  void letsAJoinWaitForEveryBranch() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Fan out", "DUMMY", true)
            .action("A", "PIPELINE")
            .action("B", "PIPELINE")
            .action("Both done", "JOIN")
            .always("Start", "Fan out")
            .always("Fan out", "A")
            .always("Fan out", "B")
            .success("A", "Both done")
            .success("B", "Both done")
            .tasks();

    ActionTask join = tasks.get("Both_done");
    assertEquals(Role.JOIN, join.role());
    assertEquals("none_failed_min_one_success", join.triggerRule());
    assertEquals(List.of(up("A", HopKind.SUCCESS), up("B", HopKind.SUCCESS)), join.upstreams());
    // A parallel branch gets the result without the rows, as in Hop
    assertEquals(
        List.of(new Upstream("Fan_out", HopKind.ALWAYS, true)), tasks.get("A").upstreams());
  }

  @Test
  void neverFollowsAConditionalHopFromAnActionThatDoesNotEvaluate() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("Dummy", "DUMMY")
            .action("Never", "WRITE_TO_LOG")
            .always("Start", "Dummy")
            .success("Dummy", "Never")
            .tasks();

    assertEquals(List.of("Dummy"), List.copyOf(tasks.keySet()));
  }

  @Test
  void ignoresDisabledHops() throws Exception {
    Map<String, ActionTask> tasks =
        start()
            .action("A", "WRITE_TO_LOG")
            .action("B", "WRITE_TO_LOG")
            .always("Start", "A")
            .disabled("A", "B")
            .tasks();

    assertEquals(List.of("A"), List.copyOf(tasks.keySet()));
  }

  @Test
  void keepsOneTaskForALoop() throws Exception {
    List<String> reasons =
        start()
            .action("A", "SIMPLE_EVAL")
            .action("B", "WAIT_FOR_SQL")
            .always("Start", "A")
            .failure("A", "B")
            .always("B", "A")
            .reasons();

    assertEquals(
        List.of("action 'A' follows the Start action and another action as well"), reasons);

    reasons =
        start()
            .action("First", "WRITE_TO_LOG")
            .action("A", "SIMPLE_EVAL")
            .action("B", "WAIT_FOR_SQL")
            .always("Start", "First")
            .success("First", "A")
            .failure("A", "B")
            .always("B", "A")
            .reasons();
    assertEquals(List.of("a hop leads back to an earlier action"), reasons);
  }

  @Test
  void keepsOneTaskWithoutASingleStartAction() throws Exception {
    assertEquals(List.of("no Start action"), new Workflow().action("A", "WRITE_TO_LOG").reasons());
    assertEquals(
        List.of("the Start action leads to no action"),
        start().action("A", "WRITE_TO_LOG").reasons());
  }

  @Test
  void makesTaskIdsAirflowAcceptsAndNoTwoAlike() {
    Set<String> used = new HashSet<>();
    assertEquals("Load_orders", ActionTasks.uniqueId("Load orders!", used));
    assertEquals("Load_orders_2", ActionTasks.uniqueId("Load orders?", used));
    assertEquals("etl.step-1", ActionTasks.uniqueId("etl.step-1", used));
    assertEquals("action", ActionTasks.uniqueId("???", used));
    assertTrue(used.contains("action"));
  }
}
