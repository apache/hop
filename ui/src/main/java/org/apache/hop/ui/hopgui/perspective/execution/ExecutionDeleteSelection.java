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

package org.apache.hop.ui.hopgui.perspective.execution;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;

/**
 * Turns a multi-selection in the execution tree into the deletes to run. A selected location
 * already covers the executions under it, so those executions are not deleted a second time.
 */
final class ExecutionDeleteSelection {

  private ExecutionDeleteSelection() {}

  /** One selected tree row. {@code execution} is null when the row is the location itself. */
  record Item(ExecutionInfoLocation location, Execution execution) {}

  /** One delete. {@code execution} is null when the whole location is wiped. */
  record Target(ExecutionInfoLocation location, Execution execution) {
    boolean locationWipe() {
      return execution == null;
    }
  }

  static List<Target> targets(List<Item> items) {
    Set<String> wipedLocations = new LinkedHashSet<>();
    List<Target> targets = new ArrayList<>();
    if (items == null) {
      return targets;
    }
    for (Item item : items) {
      if (item == null || item.location() == null || item.execution() != null) {
        continue;
      }
      String name = item.location().getName();
      if (name != null && wipedLocations.add(name)) {
        targets.add(new Target(item.location(), null));
      }
    }
    for (Item item : items) {
      if (item == null || item.location() == null || item.execution() == null) {
        continue;
      }
      if (wipedLocations.contains(item.location().getName())) {
        continue;
      }
      targets.add(new Target(item.location(), item.execution()));
    }
    return targets;
  }
}
