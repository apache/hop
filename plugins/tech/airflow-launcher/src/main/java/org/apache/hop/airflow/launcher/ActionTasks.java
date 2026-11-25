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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * The actions of a workflow as Airflow tasks: one task per action, the hops as dependencies.
 *
 * <ul>
 *   <li>An action with only success hops, or only failure hops, leaving it is a task that fails
 *       when its result is false. The task after a success hop runs when it succeeded (Airflow's
 *       default), after a failure hop when it failed ({@code one_failed}), after an unconditional
 *       hop in both cases ({@code none_skipped}). After those two, a task fails without running its
 *       action when the task before it did not run, as the workflow stops there in Hop.
 *   <li>An action with both is a condition: it succeeds in Airflow either way, and Airflow skips
 *       the side its result does not take. A failure that the workflow handles then leaves the DAG
 *       run successful, as the workflow is in Hop.
 *   <li>Where several hops lead to one action, it runs once, as soon as the first hop is followed
 *       ({@code one_success}). In Hop it runs each time one is. A failure or unconditional hop into
 *       such an action, and a side of a condition with more than one action or with an action other
 *       hops lead to as well, go through a gate: a small task that passes the state on. A gate that
 *       is not taken is skipped.
 *   <li>A Join action waits for every action before it, as in Hop ({@code
 *       none_failed_min_one_success}). Joins and gates do not start Hop.
 * </ul>
 *
 * <p>A hop back to an earlier action, and an action that the Start action and another action both
 * lead to, don't fit: those workflows keep one task that runs the whole workflow.
 */
public final class ActionTasks {

  /** Airflow's trigger rules. */
  static final String ALL_SUCCESS = "all_success";

  static final String ONE_FAILED = "one_failed";
  static final String NONE_SKIPPED = "none_skipped";
  static final String ONE_SUCCESS = "one_success";
  static final String NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success";

  /** The type of Hop's Join action, which waits for the actions before it. */
  public static final String JOIN = "JOIN";

  /**
   * The actions of Hop that don't evaluate their result: Hop follows only the unconditional hops
   * that leave them. Only for a stub that does not list them ({@link
   * HopAirflowBundle#ATTRIBUTE_NOT_EVALUATING_ACTIONS}); the plugin writes the list from the plugin
   * registry, so actions of other plugins are in it too.
   */
  static final Set<String> NOT_EVALUATING = Set.of("SPECIAL", "DUMMY", "SnowflakeWarehouseManager");

  /** When a hop is followed. */
  public enum HopKind {
    SUCCESS,
    FAILURE,
    ALWAYS
  }

  /** What a task does. */
  public enum Role {
    /** Runs its action; fails when the result is false. */
    ACTION,
    /** Runs its action; its result decides which side runs next. */
    CONDITION,
    /** Waits for the tasks before it and passes their state on. */
    JOIN,
    /** Passes the state of the task before it on, when the hop between them is followed. */
    GATE
  }

  /**
   * A task before a task.
   *
   * @param taskId its task id
   * @param kind the outcome of the task before that is passed on: its result when the hop is a
   *     success or failure hop
   * @param withoutRows its action launches the next actions in parallel: as in Hop, they get its
   *     result without the rows
   */
  public record Upstream(String taskId, HopKind kind, boolean withoutRows) {}

  /**
   * One task.
   *
   * @param taskId the Airflow task id: the action name with only the characters Airflow allows
   * @param actionName the name of the action, in the task's documentation in Airflow; for a gate,
   *     the action before it
   * @param actionType the plugin id of the action; null for a gate
   * @param role what the task does
   * @param upstreams the tasks before it; none for a task the Start action leads to
   * @param triggerRule when it runs, after the tasks before it
   * @param thenTaskId for a condition: the task that runs when the result is true
   * @param elseTaskId for a condition: the task that runs when the result is false
   */
  public record ActionTask(
      String taskId,
      String actionName,
      String actionType,
      Role role,
      List<Upstream> upstreams,
      String triggerRule,
      String thenTaskId,
      String elseTaskId) {

    /** Joins and gates do not start Hop. */
    public boolean runsHop() {
      return role == Role.ACTION || role == Role.CONDITION;
    }

    /**
     * Airflow also runs a task after a failure or unconditional hop when the task before it did not
     * run because a task before that failed. In Hop the workflow stops there: the task fails then,
     * without running its action.
     */
    public boolean needsTheTaskBeforeToHaveRun() {
      return ONE_FAILED.equals(triggerRule) || NONE_SKIPPED.equals(triggerRule);
    }
  }

  /** A dependency, and the trigger rule it needs when it is the only one. */
  private record Edge(Upstream upstream, String rule) {}

  private ActionTasks() {}

  /**
   * @param actions the {@code actions} element of the workflow
   * @param hops the {@code hops} element of the workflow
   * @return the tasks, each after the tasks before it, or an empty list when the workflow can't be
   *     split into tasks, with the reason in {@code reasons}
   */
  public static List<ActionTask> of(Element actions, Element hops, List<String> reasons) {
    return of(actions, hops, reasons, NOT_EVALUATING);
  }

  /**
   * @param actions the {@code actions} element of the workflow
   * @param hops the {@code hops} element of the workflow
   * @param notEvaluating the plugin ids of the actions that don't evaluate their result
   * @return the tasks, each after the tasks before it, or an empty list when the workflow can't be
   *     split into tasks, with the reason in {@code reasons}
   */
  public static List<ActionTask> of(
      Element actions, Element hops, List<String> reasons, Set<String> notEvaluating) {
    Map<String, String> types = new LinkedHashMap<>();
    List<String> starts = new ArrayList<>();
    Set<String> parallel = new HashSet<>();
    for (Element action : children(actions, "action")) {
      String name = text(action, "name");
      types.put(name, text(action, "type"));
      if ("Y".equals(text(action, "parallel"))) {
        parallel.add(name);
      }
      if ("SPECIAL".equals(text(action, "type"))) {
        starts.add(name);
      }
    }
    if (starts.size() != 1) {
      reasons.add(starts.isEmpty() ? "no Start action" : "more than one Start action");
      return List.of();
    }
    String start = starts.get(0);

    Map<String, List<Element>> outgoing = new HashMap<>();
    Map<String, List<Element>> incoming = new HashMap<>();
    for (Element hop : children(hops, "hop")) {
      if ("N".equals(text(hop, "enabled"))) {
        continue;
      }
      String from = text(hop, "from");
      String to = text(hop, "to");
      if (!types.containsKey(from) || !types.containsKey(to)) {
        continue;
      }
      if (notEvaluating.contains(types.get(from)) && kind(hop) != HopKind.ALWAYS) {
        // Never followed
        continue;
      }
      outgoing.computeIfAbsent(from, k -> new ArrayList<>()).add(hop);
      incoming.computeIfAbsent(to, k -> new ArrayList<>()).add(hop);
    }

    // The actions the Start action leads to
    Set<String> reached = new LinkedHashSet<>();
    Deque<String> queue = new ArrayDeque<>(List.of(start));
    while (!queue.isEmpty()) {
      String name = queue.poll();
      for (Element hop : outgoing.getOrDefault(name, List.of())) {
        String to = text(hop, "to");
        if (reached.add(to)) {
          queue.add(to);
        }
      }
    }
    if (reached.isEmpty()) {
      reasons.add("the Start action leads to no action");
      return List.of();
    }
    if (reached.contains(start)) {
      reasons.add("a hop leads back to the Start action");
      return List.of();
    }

    // The hops into each action, from the Start action or actions it leads to
    Map<String, List<Element>> in = new LinkedHashMap<>();
    for (String name : reached) {
      List<Element> hopsIn = new ArrayList<>();
      for (Element hop : incoming.getOrDefault(name, List.of())) {
        String from = text(hop, "from");
        if (start.equals(from) || reached.contains(from)) {
          hopsIn.add(hop);
        }
      }
      if (hopsIn.size() > 1 && hopsIn.stream().anyMatch(hop -> start.equals(text(hop, "from")))) {
        reasons.add("action '" + name + "' follows the Start action and another action as well");
        return List.of();
      }
      in.put(name, hopsIn);
    }

    List<String> order = topologicalOrder(reached, in, start);
    if (order == null) {
      reasons.add("a hop leads back to an earlier action");
      return List.of();
    }

    Set<String> used = new HashSet<>();
    Map<String, String> taskIds = new LinkedHashMap<>();
    for (String name : order) {
      taskIds.put(name, uniqueId(name, used));
    }
    Map<String, List<Edge>> edges = new LinkedHashMap<>();
    order.forEach(name -> edges.put(name, new ArrayList<>()));
    Map<String, List<ActionTask>> gatesAfter = new HashMap<>();
    Map<String, String[]> sides = new HashMap<>();

    for (String name : order) {
      List<Element> out =
          outgoing.getOrDefault(name, List.of()).stream()
              .filter(hop -> reached.contains(text(hop, "to")))
              .toList();
      boolean condition =
          !JOIN.equals(types.get(name))
              && out.stream().anyMatch(hop -> kind(hop) == HopKind.SUCCESS)
              && out.stream().anyMatch(hop -> kind(hop) == HopKind.FAILURE);
      String taskId = taskIds.get(name);
      boolean withoutRows = parallel.contains(name);
      Map<HopKind, String> gates = new EnumMap<>(HopKind.class);
      String[] side = new String[2];
      for (Element hop : out) {
        String to = text(hop, "to");
        HopKind kind = kind(hop);
        boolean merge = in.get(to).size() > 1 || JOIN.equals(types.get(to));
        Upstream upstream = new Upstream(taskId, kind, withoutRows);
        boolean gated;
        if (condition && kind != HopKind.ALWAYS) {
          long onThisSide = out.stream().filter(h -> kind(h) == kind).count();
          gated = merge || onThisSide > 1;
        } else {
          gated = merge && kind != HopKind.SUCCESS;
        }
        if (!gated) {
          String rule = condition && kind != HopKind.ALWAYS ? ALL_SUCCESS : rule(kind);
          edges.get(to).add(new Edge(upstream, rule));
          if (condition && kind != HopKind.ALWAYS) {
            side[kind == HopKind.SUCCESS ? 0 : 1] = taskIds.get(to);
          }
          continue;
        }
        String gate = gates.get(kind);
        if (gate == null) {
          gate = uniqueId(taskId + "__" + gateSuffix(condition, kind), used);
          gates.put(kind, gate);
          String gateRule = condition && kind != HopKind.ALWAYS ? ALL_SUCCESS : rule(kind);
          gatesAfter
              .computeIfAbsent(name, k -> new ArrayList<>())
              .add(
                  new ActionTask(
                      gate, name, null, Role.GATE, List.of(upstream), gateRule, null, null));
          if (condition && kind != HopKind.ALWAYS) {
            side[kind == HopKind.SUCCESS ? 0 : 1] = gate;
          }
        }
        edges.get(to).add(new Edge(new Upstream(gate, HopKind.SUCCESS, false), ALL_SUCCESS));
      }
      if (condition) {
        sides.put(name, side);
      }
    }

    List<ActionTask> tasks = new ArrayList<>();
    for (String name : order) {
      String type = types.get(name);
      List<Edge> into = edges.get(name);
      String triggerRule;
      if (JOIN.equals(type)) {
        triggerRule = NONE_FAILED_MIN_ONE_SUCCESS;
      } else if (into.size() > 1) {
        triggerRule = ONE_SUCCESS;
      } else if (into.size() == 1) {
        triggerRule = into.get(0).rule();
      } else {
        triggerRule = ALL_SUCCESS;
      }
      String[] side = sides.get(name);
      Role role = JOIN.equals(type) ? Role.JOIN : side != null ? Role.CONDITION : Role.ACTION;
      tasks.add(
          new ActionTask(
              taskIds.get(name),
              name,
              type,
              role,
              into.stream().map(Edge::upstream).toList(),
              triggerRule,
              side == null ? null : side[0],
              side == null ? null : side[1]));
      tasks.addAll(gatesAfter.getOrDefault(name, List.of()));
    }
    return tasks;
  }

  private static String rule(HopKind kind) {
    return switch (kind) {
      case SUCCESS -> ALL_SUCCESS;
      case FAILURE -> ONE_FAILED;
      case ALWAYS -> NONE_SKIPPED;
    };
  }

  private static String gateSuffix(boolean condition, HopKind kind) {
    return switch (kind) {
      case SUCCESS -> "then";
      case FAILURE -> condition ? "else" : "on_failure";
      case ALWAYS -> "always";
    };
  }

  /**
   * The actions, each after the actions that lead to it, in the order of the workflow where that
   * leaves a choice; null when the hops make a loop.
   */
  static List<String> topologicalOrder(
      Set<String> actions, Map<String, List<Element>> in, String start) {
    Map<String, Integer> waiting = new LinkedHashMap<>();
    Map<String, List<String>> next = new HashMap<>();
    for (String name : actions) {
      int count = 0;
      for (Element hop : in.get(name)) {
        String from = text(hop, "from");
        if (!start.equals(from)) {
          count++;
          next.computeIfAbsent(from, k -> new ArrayList<>()).add(name);
        }
      }
      waiting.put(name, count);
    }
    List<String> order = new ArrayList<>();
    Deque<String> ready = new ArrayDeque<>();
    waiting.forEach(
        (name, count) -> {
          if (count == 0) {
            ready.add(name);
          }
        });
    while (!ready.isEmpty()) {
      String name = ready.poll();
      order.add(name);
      for (String to : next.getOrDefault(name, List.of())) {
        if (waiting.merge(to, -1, Integer::sum) == 0) {
          ready.add(to);
        }
      }
    }
    return order.size() == actions.size() ? order : null;
  }

  static HopKind kind(Element hop) {
    if ("Y".equals(text(hop, "unconditional"))) {
      return HopKind.ALWAYS;
    }
    return "N".equals(text(hop, "evaluation")) ? HopKind.FAILURE : HopKind.SUCCESS;
  }

  /** A task id: letters, digits, '_', '.' and '-' only, used by no other task. */
  static String uniqueId(String name, Set<String> used) {
    String base = name.trim().replaceAll("[^A-Za-z0-9_.-]+", "_").replaceAll("^_+|_+$", "");
    if (base.isEmpty()) {
      base = "action";
    }
    if (base.length() > 200) {
      base = base.substring(0, 200);
    }
    String id = base;
    for (int i = 2; !used.add(id); i++) {
      id = base + "_" + i;
    }
    return id;
  }

  private static List<Element> children(Element parent, String tag) {
    List<Element> elements = new ArrayList<>();
    if (parent == null) {
      return elements;
    }
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && tag.equals(element.getTagName())) {
        elements.add(element);
      }
    }
    return elements;
  }

  private static String text(Element parent, String tag) {
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && tag.equals(element.getTagName())) {
        return element.getTextContent().trim();
      }
    }
    return "";
  }
}
