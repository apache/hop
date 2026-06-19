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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultExecutionSelectorTest {

  /** Mimics the Execution Information perspective defaults: only parents, last hour. */
  private static DefaultExecutionSelector selectorWithFilter(String filterText) {
    return new DefaultExecutionSelector(
        true, false, false, false, false, false, filterText, LastPeriod.ONE_HOUR);
  }

  private static Execution execution(String id, String parentId, Date startDate) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setParentId(parentId);
    execution.setName("test-execution");
    execution.setExecutionType(ExecutionType.Pipeline);
    execution.setExecutionStartDate(startDate);
    return execution;
  }

  private static ExecutionState state(String id, String parentId) {
    ExecutionState state = new ExecutionState();
    state.setId(id);
    state.setParentId(parentId);
    state.setExecutionType(ExecutionType.Pipeline);
    state.setStatusDescription("Running");
    return state;
  }

  private static Date daysAgo(int days) {
    return new Date(System.currentTimeMillis() - Duration.ofDays(days).toMillis());
  }

  @Test
  void isSelectingByUuidRecognizesUuid() {
    assertTrue(selectorWithFilter(UUID.randomUUID().toString()).isSelectingByUuid());
  }

  @Test
  void isSelectingByUuidRejectsNonUuidAndBlank() {
    assertFalse(selectorWithFilter("my-pipeline").isSelectingByUuid());
    assertFalse(selectorWithFilter("").isSelectingByUuid());
    assertFalse(selectorWithFilter(null).isSelectingByUuid());
  }

  @Test
  void filterByUuidMatchesExecutionOutsideTimeWindow() {
    // An exact execution-ID match must ignore the time-window filter so an old
    // execution can still be found by its ID.
    String id = UUID.randomUUID().toString();
    assertTrue(selectorWithFilter(id).isSelected(execution(id, null, daysAgo(1))));
  }

  @Test
  void filterByUuidMatchIsCaseInsensitive() {
    String id = UUID.randomUUID().toString();
    DefaultExecutionSelector selector = selectorWithFilter(id.toUpperCase());
    assertTrue(selector.isSelected(execution(id, null, new Date())));
  }

  @Test
  void filterByUuidDoesNotMatchDifferentId() {
    DefaultExecutionSelector selector = selectorWithFilter(UUID.randomUUID().toString());
    assertFalse(selector.isSelected(execution(UUID.randomUUID().toString(), null, new Date())));
  }

  @Test
  void filterByUuidMatchesChildExecutionState() {
    // A child execution has a non-empty parentId; the default "only parents"
    // toggle must not veto an exact UUID match.
    String id = UUID.randomUUID().toString();
    DefaultExecutionSelector selector = selectorWithFilter(id);
    assertTrue(selector.isSelected(state(id, UUID.randomUUID().toString())));
  }

  @Test
  void filterByUuidSelectsNullExecutionState() {
    // The selection is decided by isSelected(Execution); a not-yet-written state
    // must not veto it (and must not throw).
    DefaultExecutionSelector selector = selectorWithFilter(UUID.randomUUID().toString());
    assertTrue(selector.isSelected((ExecutionState) null));
  }

  @Test
  void nullExecutionStateIsSkippedWithoutUuidFilter() {
    // Without a UUID filter a null state must be skipped instead of throwing.
    assertFalse(selectorWithFilter("my-pipeline").isSelected((ExecutionState) null));
  }

  @Test
  void parentToggleStillFiltersChildrenWithoutUuidFilter() {
    DefaultExecutionSelector selector = selectorWithFilter("");
    assertFalse(selector.isSelected(state("x", "some-parent-id")));
    assertTrue(selector.isSelected(state("x", null)));
  }

  @Test
  void timeWindowStillAppliesWithoutUuidFilter() {
    DefaultExecutionSelector selector = selectorWithFilter("");
    assertTrue(selector.isSelected(execution(UUID.randomUUID().toString(), null, new Date())));
    assertFalse(selector.isSelected(execution(UUID.randomUUID().toString(), null, daysAgo(1))));
  }
}
